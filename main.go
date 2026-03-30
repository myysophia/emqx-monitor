package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"strconv"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"gopkg.in/natefinch/lumberjack.v2"
	"gopkg.in/yaml.v3"
)

// UserStat 用户连接统计
type UserStat struct {
	Username string `json:"username"`
	Count    int    `json:"count"`
}

// StatsResponse API 响应结构
type StatsResponse struct {
	Timestamp string     `json:"timestamp"`
	Data      []UserStat `json:"data"`
	Total     int        `json:"total"`
}

// StatsCache 线程安全的缓存
type StatsCache struct {
	mu      sync.RWMutex
	data    []UserStat
	total   int
	updated time.Time
}

// HistoryData 历史数据点
type HistoryData struct {
	Timestamp time.Time      `json:"timestamp"`
	UserStats map[string]int `json:"user_stats"`
	Total     int            `json:"total"`
}

// AlertChange 变化数据
type AlertChange struct {
	Username      string  `json:"username"`
	Current       int     `json:"current"`
	Previous      int     `json:"previous"`
	Change        int     `json:"change"`
	ChangePercent float64 `json:"change_percent"`
}

// WeChatMessage 企业微信消息
type WeChatMessage struct {
	MsgType string `json:"msgtype"`
	Markdown *struct {
		Content string `json:"content"`
		MentionedList []string `json:"mentioned_list,omitempty"`
	} `json:"markdown,omitempty"`
}

// YAMLConfig 配置文件结构
type YAMLConfig struct {
	EMqx           struct {
		Namespace string `yaml:"namespace"`
		Pod       string `yaml:"pod"`
	} `yaml:"emqx"`
	CollectInterval string `yaml:"collect_interval"`
	CheckInterval   string `yaml:"check_interval"`
	ServerPort      int    `yaml:"server_port"`
	WebhookURL      string `yaml:"webhook_url"`
	Report          struct {
		Enabled bool `yaml:"enabled"`
		Day     int  `yaml:"day"`
		Hour    int  `yaml:"hour"`
		Minute  int  `yaml:"minute"`
	} `yaml:"report"`
	Log             struct {
		Level     string `yaml:"level"`
		File      string `yaml:"file"`
		MaxSize   int    `yaml:"max_size"`
		MaxBackups int   `yaml:"max_backups"`
		MaxAge    int    `yaml:"max_age"`
		Compress  bool   `yaml:"compress"`
	} `yaml:"log"`
	Alert           struct {
		MinChange   int      `yaml:"min_change"`     // 最小下降数量阈值
		MinPercent  float64  `yaml:"min_percent"`    // 最小下降百分比阈值
		IgnoreUsers []string `yaml:"ignore_users"`   // 忽略的用户列表
	} `yaml:"alert"`
	MqttTest        struct {
		Enabled          bool   `yaml:"enabled"`
		Broker           string `yaml:"broker"`
		Topic            string `yaml:"topic"`
		Username         string `yaml:"username"`
		Password         string `yaml:"password"`
		ClassName        string `yaml:"class_name"`
		Timeout          int    `yaml:"timeout"`
		TestLoops        int    `yaml:"test_loops"`
		TestInterval     int    `yaml:"test_interval"`
		FailureThreshold int    `yaml:"failure_threshold"`
		Schedule         string `yaml:"schedule"`
	} `yaml:"mqtt_test"`
}

// ReportConfig 报告配置
type ReportConfig struct {
	Enabled bool
	Day     time.Weekday
	Hour    int
	Minute  int
}

// Config 配置
type Config struct {
	Namespace       string
	Pod             string
	CollectInterval time.Duration
	CheckInterval   time.Duration
	ServerPort      int
	WebhookURL      string
	LogFile         string
	Report          ReportConfig
	MqttTest        MqttTestConfig
	Alert           AlertConfig
}

// AlertConfig 告警配置
type AlertConfig struct {
	MinChange  int      // 最小下降数量阈值
	MinPercent float64  // 最小下降百分比阈值
	IgnoreUsers []string // 忽略的用户列表
}

// MqttTestConfig MQTT 测试配置
type MqttTestConfig struct {
	Enabled          bool
	Broker           string
	Topic            string
	Username         string
	Password         string
	ClassName        string
	Timeout          time.Duration
	TestLoops        int
	TestInterval     time.Duration
	FailureThreshold int
}

var (
	cache   = &StatsCache{}
	history = &HistoryStore{}
	config  Config
	regex   = regexp.MustCompile(`username=([^,]+)`)
	logger  = InitLogger()
	// MQTT 测试失败计数器
	mqttFailureCount = 0
	mqttFailureMu    sync.Mutex
)

// InitLogger 初始化日志系统
func InitLogger() *lumberjack.Logger {
	logDir := "/opt/emqx/emqx-monitor/logs"
	os.MkdirAll(logDir, 0755)

	logger := &lumberjack.Logger{
		Filename:   filepath.Join(logDir, "emqx-monitor.log"),
		MaxSize:    100,    // MB
		MaxBackups: 10,     // 保留旧文件最大个数
		MaxAge:     30,     // 保留旧文件最大天数
		Compress:   true,   // 是否压缩
	}

	return logger
}

// HistoryStore 历史数据存储
type HistoryStore struct {
	mu        sync.RWMutex
	data      []HistoryData
	fileStore *FileStore
}

const retentionDays = 30

// dashboardHTML 内嵌的前端趋势图页面
const dashboardHTML = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>EMQX Monitor</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { background: #1a1a2e; color: #e0e0e0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; padding: 20px; }
.header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; flex-wrap: wrap; gap: 12px; }
h1 { font-size: 1.5em; color: #00d4ff; }
.controls { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
.controls label { font-size: 0.9em; color: #888; }
.btn { background: #16213e; color: #e0e0e0; border: 1px solid #0f3460; padding: 6px 14px; border-radius: 4px; cursor: pointer; font-size: 0.85em; }
.btn:hover { background: #0f3460; }
.btn.active { background: #0f3460; border-color: #00d4ff; color: #00d4ff; }
.status { font-size: 0.8em; color: #666; }
.stats-row { display: flex; gap: 16px; margin-bottom: 20px; flex-wrap: wrap; }
.stat-card { background: #16213e; border: 1px solid #0f3460; border-radius: 8px; padding: 14px 20px; min-width: 140px; }
.stat-card .label { font-size: 0.8em; color: #888; }
.stat-card .value { font-size: 1.6em; color: #00d4ff; font-weight: bold; margin-top: 4px; }
.chart-container { background: #16213e; border: 1px solid #0f3460; border-radius: 8px; padding: 16px; margin-bottom: 20px; position: relative; }
.chart-container canvas { max-height: 380px; }
.toggle { display: flex; gap: 12px; margin-bottom: 12px; }
.toggle label { font-size: 0.85em; display: flex; align-items: center; gap: 4px; cursor: pointer; }
.toggle input { cursor: pointer; }
</style>
</head>
<body>
<div class="header">
  <h1>EMQX Connection Monitor</h1>
  <div class="controls">
    <label>Range:</label>
    <button class="btn" data-days="1">1D</button>
    <button class="btn active" data-days="7">7D</button>
    <button class="btn" data-days="14">14D</button>
    <button class="btn" data-days="30">30D</button>
    <span class="status" id="status"></span>
  </div>
</div>

<div class="stats-row" id="statsRow"></div>

<div class="chart-container">
  <div class="toggle">
    <label><input type="checkbox" id="showUsers" checked> Show by user</label>
  </div>
  <canvas id="trendChart"></canvas>
</div>

<script>
const COLORS = ['#00d4ff','#ff6b6b','#ffd93d','#6bcb77','#4d96ff','#ff922b','#cc5de8','#20c997','#ff8787','#74c0fc'];
let currentDays = 7;
let chart = null;
let refreshTimer = null;

async function loadData(days) {
  const resp = await fetch('/api/v1/clients/history?days=' + days);
  return resp.json();
}

async function loadCurrentStats() {
  try {
    const resp = await fetch('/api/v1/clients/stats');
    const data = await resp.json();
    renderStats(data);
  } catch(e) {}
}

function renderStats(data) {
  const row = document.getElementById('statsRow');
  let html = '<div class="stat-card"><div class="label">Total Connections</div><div class="value">' + data.total + '</div></div>';
  html += '<div class="stat-card"><div class="label">Users</div><div class="value">' + data.data.length + '</div></div>';
  html += '<div class="stat-card"><div class="label">Updated</div><div class="value" style="font-size:1em;color:#aaa">' + (data.timestamp || '-') + '</div></div>';
  row.innerHTML = html;
}

function buildChart(data, showUsers) {
  const ctx = document.getElementById('trendChart').getContext('2d');
  if (chart) chart.destroy();

  const labels = data.map(d => {
    const t = new Date(d.timestamp);
    return t.toLocaleString('zh-CN', { month:'2-digit', day:'2-digit', hour:'2-digit', minute:'2-digit' });
  });

  // Downsample for large datasets
  const maxPoints = 800;
  let sampled = data;
  if (data.length > maxPoints) {
    const step = Math.ceil(data.length / maxPoints);
    sampled = data.filter((_, i) => i % step === 0);
  }

  const sLabels = sampled.map(d => {
    const t = new Date(d.timestamp);
    return t.toLocaleString('zh-CN', { month:'2-digit', day:'2-digit', hour:'2-digit', minute:'2-digit' });
  });

  const datasets = [];

  if (showUsers) {
    const users = new Set();
    sampled.forEach(d => Object.keys(d.user_stats).forEach(u => users.add(u)));
    let ci = 0;
    users.forEach(u => {
      datasets.push({
        label: u,
        data: sampled.map(d => d.user_stats[u] || 0),
        borderColor: COLORS[ci % COLORS.length],
        backgroundColor: COLORS[ci % COLORS.length] + '20',
        borderWidth: 1.5,
        pointRadius: 0,
        tension: 0.3,
        fill: false,
      });
      ci++;
    });
  }

  datasets.unshift({
    label: 'Total',
    data: sampled.map(d => d.total),
    borderColor: '#00d4ff',
    backgroundColor: 'rgba(0,212,255,0.08)',
    borderWidth: 2,
    pointRadius: 0,
    tension: 0.3,
    fill: true,
  });

  chart = new Chart(ctx, {
    type: 'line',
    data: { labels: sLabels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { labels: { color: '#aaa', boxWidth: 12, font: { size: 11 } } },
        tooltip: { backgroundColor: '#16213e', borderColor: '#0f3460', borderWidth: 1 }
      },
      scales: {
        x: { ticks: { color: '#666', maxTicksLimit: 12, font: { size: 10 } }, grid: { color: '#1a1a3e' } },
        y: { ticks: { color: '#666', font: { size: 10 } }, grid: { color: '#1a1a3e' }, beginAtZero: true }
      }
    }
  });
}

async function refresh() {
  const status = document.getElementById('status');
  status.textContent = 'Loading...';
  try {
    const data = await loadData(currentDays);
    const showUsers = document.getElementById('showUsers').checked;
    buildChart(data, showUsers);
    status.textContent = data.length + ' points';
  } catch(e) {
    status.textContent = 'Error: ' + e.message;
  }
}

// Range buttons
document.querySelectorAll('.btn[data-days]').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.btn[data-days]').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    currentDays = parseInt(btn.dataset.days);
    refresh();
  });
});

// Toggle by-user
document.getElementById('showUsers').addEventListener('change', () => refresh());

// Auto refresh
function scheduleRefresh() {
  if (refreshTimer) clearInterval(refreshTimer);
  refreshTimer = setInterval(() => { refresh(); loadCurrentStats(); }, 30000);
}

// Init
refresh();
loadCurrentStats();
scheduleRefresh();
</script>
</body>
</html>`

// FileStore 历史数据文件持久化（JSON Lines 格式）
type FileStore struct {
	filePath string
	mu       sync.Mutex
}

func NewFileStore(filePath string) *FileStore {
	return &FileStore{filePath: filePath}
}

// Append 将一条数据追加写入文件末尾
func (fs *FileStore) Append(entry HistoryData) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	f, err := os.OpenFile(fs.filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open file for append: %w", err)
	}
	defer f.Close()

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal entry: %w", err)
	}

	data = append(data, '\n')
	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("write entry: %w", err)
	}

	return nil
}

// Load 从文件加载历史数据，过滤过期条目
func (fs *FileStore) Load(retentionDays int) ([]HistoryData, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	f, err := os.Open(fs.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("open file for read: %w", err)
	}
	defer f.Close()

	cutoff := time.Now().AddDate(0, 0, -retentionDays)
	var entries []HistoryData

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024)

	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var entry HistoryData
		if err := json.Unmarshal(line, &entry); err != nil {
			logWarn("FileStore: skipping corrupt line %d: %v", lineNum, err)
			continue
		}

		if entry.Timestamp.After(cutoff) {
			entries = append(entries, entry)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan file: %w", err)
	}

	logInfo("FileStore: loaded %d entries from %s", len(entries), fs.filePath)
	return entries, nil
}

// Compact 重写文件，移除过期条目（原子替换）
func (fs *FileStore) Compact(retentionDays int) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	cutoff := time.Now().AddDate(0, 0, -retentionDays)

	f, err := os.Open(fs.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("open file for compaction: %w", err)
	}
	defer f.Close()

	tmpPath := fs.filePath + ".tmp"
	tmpF, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024)

	retained := 0
	removed := 0
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var entry HistoryData
		if err := json.Unmarshal(line, &entry); err != nil {
			tmpF.Write(line)
			tmpF.Write([]byte{'\n'})
			removed++
			continue
		}

		if entry.Timestamp.After(cutoff) {
			tmpF.Write(line)
			tmpF.Write([]byte{'\n'})
			retained++
		} else {
			removed++
		}
	}

	if err := scanner.Err(); err != nil {
		tmpF.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("scan during compaction: %w", err)
	}

	if err := tmpF.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("close temp file: %w", err)
	}

	if err := os.Rename(tmpPath, fs.filePath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename temp file: %w", err)
	}

	logInfo("FileStore: compacted: retained %d, removed %d entries", retained, removed)
	return nil
}

// Init 加载持久化数据并初始化存储
func (h *HistoryStore) Init(filePath string) error {
	h.fileStore = NewFileStore(filePath)

	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create data directory: %w", err)
	}

	entries, err := h.fileStore.Load(retentionDays)
	if err != nil {
		logWarn("Failed to load history from disk: %v, starting fresh", err)
		return nil
	}

	if len(entries) > 0 {
		h.data = entries
		logInfo("HistoryStore: restored %d entries from disk", len(entries))
	}

	return nil
}

func (h *HistoryStore) Add(stats []UserStat, total int) {
	h.mu.Lock()
	defer h.mu.Unlock()

	userStats := make(map[string]int)
	for _, s := range stats {
		userStats[s.Username] = s.Count
	}

	entry := HistoryData{
		Timestamp: time.Now(),
		UserStats: userStats,
		Total:     total,
	}
	h.data = append(h.data, entry)

	// 持久化到磁盘（错误只记日志，不中断服务）
	if h.fileStore != nil {
		if err := h.fileStore.Append(entry); err != nil {
			logError("Failed to persist history entry: %v", err)
		}
	}

	// 保留最近 30 天的数据
	cutoff := time.Now().AddDate(0, 0, -retentionDays)
	for i, d := range h.data {
		if d.Timestamp.After(cutoff) {
			h.data = h.data[i:]
			break
		}
	}
}

// CompactDisk 触发文件压缩
func (h *HistoryStore) CompactDisk() {
	if h.fileStore != nil {
		if err := h.fileStore.Compact(retentionDays); err != nil {
			logError("Failed to compact history file: %v", err)
		}
	}
}

// GetHistory 返回指定天数内的原始历史数据
func (h *HistoryStore) GetHistory(days int) []HistoryData {
	h.mu.RLock()
	defer h.mu.RUnlock()

	cutoff := time.Now().AddDate(0, 0, -days)
	var result []HistoryData
	for _, d := range h.data {
		if d.Timestamp.After(cutoff) {
			result = append(result, d)
		}
	}
	return result
}

func (h *HistoryStore) GetHourBefore(hour int) *HistoryData {
	h.mu.RLock()
	defer h.mu.RUnlock()

	target := time.Now().Add(-time.Duration(hour) * time.Hour)

	// 查找最近的目标时间点的数据
	for i := len(h.data) - 1; i >= 0; i-- {
		if h.data[i].Timestamp.Before(target) {
			return &h.data[i]
		}
	}
	return nil
}

func (h *HistoryStore) GetSameHourLastWeek() *HistoryData {
	h.mu.RLock()
	defer h.mu.RUnlock()

	// 一周前的同一小时
	target := time.Now().Add(-7 * 24 * time.Hour)
	target = time.Date(target.Year(), target.Month(), target.Day(), target.Hour(), 0, 0, 0, target.Location())

	for i := len(h.data) - 1; i >= 0; i-- {
		if h.data[i].Timestamp.Before(target) {
			return &h.data[i]
		}
	}
	return nil
}

// DailyStats 每日统计
type DailyStats struct {
	Date         string            `json:"date"`
	UserStats    map[string]int    `json:"user_stats"`    // 用户平均连接数
	UserMaxStats map[string]int    `json:"user_max_stats"`// 用户峰值连接数
	AvgTotal     int               `json:"avg_total"`     // 平均总连接数
	MaxTotal     int               `json:"max_total"`     // 峰值总连接数
	SampleCount  int               `json:"sample_count"`  // 采样次数
}

// GetDailyStats 获取最近 N 天的每日统计（计算平均值和峰值）
func (h *HistoryStore) GetDailyStats(days int) []DailyStats {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.data) == 0 {
		return nil
	}

	// 按天聚合数据 - 使用新的结构来跟踪总和、计数和最大值
	type dailyAccumulator struct {
		UserSums   map[string]int  // 用户连接数总和
		UserMaxes  map[string]int  // 用户连接数峰值
		TotalSum   int             // 总连接数总和
		TotalMax   int             // 总连接数峰值
		Count      int             // 采样次数
	}

	dailyMap := make(map[string]*dailyAccumulator)
	now := time.Now()

	for _, d := range h.data {
		// 只统计最近 N 天
		if d.Timestamp.Before(now.AddDate(0, 0, -days)) {
			continue
		}

		dateKey := d.Timestamp.Format("2006-01-02")
		if dailyMap[dateKey] == nil {
			dailyMap[dateKey] = &dailyAccumulator{
				UserSums:  make(map[string]int),
				UserMaxes: make(map[string]int),
			}
		}

		acc := dailyMap[dateKey]
		acc.Count++
		acc.TotalSum += d.Total
		if d.Total > acc.TotalMax {
			acc.TotalMax = d.Total
		}

		for user, count := range d.UserStats {
			acc.UserSums[user] += count
			if acc.UserMaxes[user] < count {
				acc.UserMaxes[user] = count
			}
		}
	}

	// 转换为有序切片，计算平均值
	var result []DailyStats
	for i := days - 1; i >= 0; i-- {
		date := now.AddDate(0, 0, -i).Format("2006-01-02")
		if acc, ok := dailyMap[date]; ok {
			stats := DailyStats{
				Date:         date,
				UserStats:    make(map[string]int),
				UserMaxStats: make(map[string]int),
				MaxTotal:     acc.TotalMax,
				SampleCount:  acc.Count,
			}

			// 计算用户平均连接数
			if acc.Count > 0 {
				stats.AvgTotal = acc.TotalSum / acc.Count
				for user, sum := range acc.UserSums {
					stats.UserStats[user] = sum / acc.Count
					stats.UserMaxStats[user] = acc.UserMaxes[user]
				}
			}

			result = append(result, stats)
		}
	}

	return result
}

func main() {
	// 设置日志输出到文件和控制台
	logWriter := io.MultiWriter(logger, os.Stdout)
	log.SetOutput(logWriter)

	loadConfig()

	logInfo("=== EMQX Monitor Starting ===")
	logInfo("Config: Namespace=%s, Pod=%s, CollectInterval=%v, CheckInterval=%v, Port=%d",
		config.Namespace, config.Pod, config.CollectInterval, config.CheckInterval, config.ServerPort)

	// 初始化历史数据持久化
	if err := history.Init("/opt/emqx/emqx-monitor/data/history.jsonl"); err != nil {
		logError("Failed to initialize history store: %v", err)
	}

	// 初始采集
	collectData()

	// 启动定时采集（30秒）
	go startCollector()

	// 启动历史文件压缩（每天凌晨 3 点）
	go startCompaction()

	// 启动告警检查（1小时）
	go startAlertChecker()

	// 启动周报定时任务
	go startWeeklyReport()

	// 启动 HTTP 服务
	startServer()
}

func loadConfig() {
	configPath := "/opt/emqx/emqx-monitor/config/config.yaml"

	// 尝试读取配置文件
	data, err := os.ReadFile(configPath)
	if err != nil {
		logWarn("Failed to read config file: %v, using defaults", err)
		loadDefaultConfig()
		return
	}

	var yamlConfig YAMLConfig
	if err := yaml.Unmarshal(data, &yamlConfig); err != nil {
		logWarn("Failed to parse config file: %v, using defaults", err)
		loadDefaultConfig()
		return
	}

	// 解析配置
	config.Namespace = yamlConfig.EMqx.Namespace
	config.Pod = yamlConfig.EMqx.Pod
	config.ServerPort = yamlConfig.ServerPort
	config.WebhookURL = yamlConfig.WebhookURL

	// 解析时间间隔
	if d, err := time.ParseDuration(yamlConfig.CollectInterval); err == nil {
		config.CollectInterval = d
	} else {
		config.CollectInterval = 30 * time.Second
	}

	if d, err := time.ParseDuration(yamlConfig.CheckInterval); err == nil {
		config.CheckInterval = d
	} else {
		config.CheckInterval = 1 * time.Hour
	}

	// 更新日志配置
	if yamlConfig.Log.File != "" {
		logger.Filename = yamlConfig.Log.File
	}
	if yamlConfig.Log.MaxSize > 0 {
		logger.MaxSize = yamlConfig.Log.MaxSize
	}
	if yamlConfig.Log.MaxBackups > 0 {
		logger.MaxBackups = yamlConfig.Log.MaxBackups
	}
	if yamlConfig.Log.MaxAge > 0 {
		logger.MaxAge = yamlConfig.Log.MaxAge
	}
	logger.Compress = yamlConfig.Log.Compress

	config.LogFile = logger.Filename

	// 解析报告配置
	config.Report.Enabled = yamlConfig.Report.Enabled
	config.Report.Day = time.Weekday(yamlConfig.Report.Day)
	config.Report.Hour = yamlConfig.Report.Hour
	config.Report.Minute = yamlConfig.Report.Minute

	// 解析 MQTT 测试配置
	config.MqttTest.Enabled = yamlConfig.MqttTest.Enabled
	config.MqttTest.Broker = yamlConfig.MqttTest.Broker
	config.MqttTest.Topic = yamlConfig.MqttTest.Topic
	config.MqttTest.Username = yamlConfig.MqttTest.Username
	config.MqttTest.Password = yamlConfig.MqttTest.Password
	config.MqttTest.ClassName = yamlConfig.MqttTest.ClassName
	config.MqttTest.Timeout = time.Duration(yamlConfig.MqttTest.Timeout) * time.Second
	config.MqttTest.TestLoops = yamlConfig.MqttTest.TestLoops
	config.MqttTest.TestInterval = time.Duration(yamlConfig.MqttTest.TestInterval) * time.Second
	config.MqttTest.FailureThreshold = yamlConfig.MqttTest.FailureThreshold

	// 解析告警配置
	config.Alert.MinChange = yamlConfig.Alert.MinChange
	config.Alert.MinPercent = yamlConfig.Alert.MinPercent
	config.Alert.IgnoreUsers = yamlConfig.Alert.IgnoreUsers
}

func loadDefaultConfig() {
	config.Namespace = "ems-au"
	config.Pod = "emqx-eu-0"
	config.CollectInterval = 30 * time.Second
	config.CheckInterval = 1 * time.Hour
	config.ServerPort = 8080
	config.WebhookURL = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=2785e74d-e41c-475f-b8ac-a8643aa36000"
	config.Report.Enabled = true
	config.Report.Day = time.Monday
	config.Report.Hour = 9
	config.Report.Minute = 0
	config.Alert.MinChange = 5       // 默认至少下降 5 个才告警
	config.Alert.MinPercent = 30.0   // 默认至少下降 30% 才告警
	config.Alert.IgnoreUsers = []string{} // 默认不忽略任何用户
}

func startCollector() {
	ticker := time.NewTicker(config.CollectInterval)
	for range ticker.C {
		collectData()
	}
}

func startAlertChecker() {
	// 等待一小时后开始第一次检查
	time.Sleep(config.CheckInterval)

	ticker := time.NewTicker(config.CheckInterval)
	for range ticker.C {
		runAlertCheck()
	}
}

// startCompaction 每天凌晨 3 点执行历史文件压缩
func startCompaction() {
	for {
		now := time.Now()
		next := time.Date(now.Year(), now.Month(), now.Day(), 3, 0, 0, 0, now.Location())
		if next.Before(now) {
			next = next.Add(24 * time.Hour)
		}
		time.Sleep(next.Sub(now))

		logInfo("Starting history file compaction...")
		history.CompactDisk()
	}
}

func startWeeklyReport() {
	if !config.Report.Enabled {
		return
	}

	logInfo("Weekly report scheduled for %s %02d:%02d", config.Report.Day, config.Report.Hour, config.Report.Minute)

	for {
		// 计算下次发送时间
		now := time.Now()
		next := time.Date(now.Year(), now.Month(), now.Day(), config.Report.Hour, config.Report.Minute, 0, 0, now.Location())

		// 如果本周时间已过，计算下周
		if next.Weekday() != config.Report.Day || next.Before(now) {
			daysUntil := int((config.Report.Day - now.Weekday() + 7) % 7)
			if daysUntil == 0 && next.Before(now) {
				daysUntil = 7
			}
			next = now.AddDate(0, 0, daysUntil)
			next = time.Date(next.Year(), next.Month(), next.Day(), config.Report.Hour, config.Report.Minute, 0, 0, next.Location())
		}

		duration := next.Sub(now)
		logInfo("Next weekly report at %s (in %v)", next.Format(time.RFC3339), duration)

		time.Sleep(duration)
		sendWeeklyReport()
	}
}

func collectData() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cmd := []string{
		"kubectl", "-n", config.Namespace, "exec", config.Pod,
		"--", "emqx_ctl", "clients", "list",
	}

	output, err := execCommand(ctx, cmd...)
	if err != nil {
		logError("Failed to collect data: %v", err)
		return
	}

	stats := parseOutput(output)

	cache.mu.Lock()
	cache.data = stats
	cache.total = 0
	for _, s := range stats {
		cache.total += s.Count
	}
	cache.updated = time.Now()
	cache.mu.Unlock()

	// 添加到历史数据
	history.Add(stats, cache.total)

	logInfo("Collected: %d users, %d total connections", len(stats), cache.total)
}

func execCommand(ctx context.Context, cmd ...string) (string, error) {
	c := exec.CommandContext(ctx, cmd[0], cmd[1:]...)
	output, err := c.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("command failed: %w, output: %s", err, string(output))
	}
	return string(output), nil
}

func parseOutput(output string) []UserStat {
	lines := strings.Split(output, "\n")
	userCount := make(map[string]int)

	for _, line := range lines {
		matches := regex.FindStringSubmatch(line)
		if len(matches) > 1 {
			username := matches[1]
			userCount[username]++
		}
	}

	var stats []UserStat
	for username, count := range userCount {
		stats = append(stats, UserStat{
			Username: username,
			Count:    count,
		})
	}

	return stats
}

func runAlertCheck() {
	logInfo("Running alert check...")

	cache.mu.RLock()
	currentStats := cache.data
	currentTotal := cache.total
	cache.mu.RUnlock()

	// 环比（1小时前）
	prevHour := history.GetHourBefore(1)
	var momChanges []AlertChange
	if prevHour != nil {
		for _, stat := range currentStats {
			// 跳过被忽略的用户
			if isUserIgnored(stat.Username) {
				continue
			}

			prevCount := prevHour.UserStats[stat.Username]
			if stat.Count < prevCount {
				change := prevCount - stat.Count
				percent := float64(change) / float64(prevCount) * 100

				// 检查是否满足告警阈值
				if shouldAlert(change, percent, prevCount) {
					momChanges = append(momChanges, AlertChange{
						Username:      stat.Username,
						Current:       stat.Count,
						Previous:      prevCount,
						Change:        change,
						ChangePercent: percent,
					})
				}
			}
		}
	}

	// 同比（上周同一时间）
	lastWeek := history.GetSameHourLastWeek()
	var yoyChanges []AlertChange
	if lastWeek != nil {
		for _, stat := range currentStats {
			// 跳过被忽略的用户
			if isUserIgnored(stat.Username) {
				continue
			}

			prevCount := lastWeek.UserStats[stat.Username]
			if stat.Count < prevCount {
				change := prevCount - stat.Count
				percent := float64(change) / float64(prevCount) * 100

				// 检查是否满足告警阈值
				if shouldAlert(change, percent, prevCount) {
					yoyChanges = append(yoyChanges, AlertChange{
						Username:      stat.Username,
						Current:       stat.Count,
						Previous:      prevCount,
						Change:        change,
						ChangePercent: percent,
					})
				}
			}
		}
	}

	// 总数变化
	var totalMom, totalYoy string
	if prevHour != nil && currentTotal < prevHour.Total {
		change := prevHour.Total - currentTotal
		percent := float64(change) / float64(prevHour.Total) * 100
		// 检查是否满足告警阈值
		if shouldAlert(change, percent, prevHour.Total) {
			totalMom = fmt.Sprintf("⚠️ 总连接数减少 %d (%.1f%%)", change, percent)
		}
	}
	if lastWeek != nil && currentTotal < lastWeek.Total {
		change := lastWeek.Total - currentTotal
		percent := float64(change) / float64(lastWeek.Total) * 100
		// 检查是否满足告警阈值
		if shouldAlert(change, percent, lastWeek.Total) {
			totalYoy = fmt.Sprintf("⚠️ 总连接数减少 %d (%.1f%%)", change, percent)
		}
	}

	// 如果有告警，发送企业微信通知
	if len(momChanges) > 0 || len(yoyChanges) > 0 || totalMom != "" || totalYoy != "" {
		sendAlert(momChanges, yoyChanges, totalMom, totalYoy, currentTotal)
	}
}

// isUserIgnored 检查用户是否被忽略
func isUserIgnored(username string) bool {
	for _, ignored := range config.Alert.IgnoreUsers {
		if ignored == username {
			return true
		}
	}
	return false
}

// shouldAlert 判断是否应该告警
func shouldAlert(change int, percent float64, prevCount int) bool {
	// 如果两个阈值都为 0，则保持原有行为（任何下降都告警）
	if config.Alert.MinChange == 0 && config.Alert.MinPercent == 0 {
		return true
	}

	// 满足任一条件即告警
	if config.Alert.MinChange > 0 && change >= config.Alert.MinChange {
		return true
	}
	if config.Alert.MinPercent > 0 && percent >= config.Alert.MinPercent {
		return true
	}

	return false
}

func sendAlert(momChanges, yoyChanges []AlertChange, totalMom, totalYoy string, currentTotal int) {
	now := time.Now().Format("2006-01-02 15:04:05")

	content := fmt.Sprintf("# EMQX 连接监控告警\n\n")
	content += fmt.Sprintf("**时间**: %s\n\n", now)
	content += fmt.Sprintf("**当前总连接数**: %d\n\n", currentTotal)

	if totalMom != "" {
		content += fmt.Sprintf("## 环比分析（1小时前）\n%s\n\n", totalMom)
	}

	if totalYoy != "" {
		content += fmt.Sprintf("## 同比分析（上周同期）\n%s\n\n", totalYoy)
	}

	if len(momChanges) > 0 {
		content += fmt.Sprintf("### 用户连接数环比下降\n\n")
		content += "| 用户 | 当前 | 1小时前 | 减少 | 降幅 |\n"
		content += "|------|------|---------|------|------|\n"
		for _, c := range momChanges {
			content += fmt.Sprintf("| %s | %d | %d | %d | %.1f%% |\n",
				c.Username, c.Current, c.Previous, c.Change, c.ChangePercent)
		}
		content += "\n"
	}

	if len(yoyChanges) > 0 {
		content += fmt.Sprintf("### 用户连接数同比下降\n\n")
		content += "| 用户 | 当前 | 上周同期 | 减少 | 降幅 |\n"
		content += "|------|------|----------|------|------|\n"
		for _, c := range yoyChanges {
			content += fmt.Sprintf("| %s | %d | %d | %d | %.1f%% |\n",
				c.Username, c.Current, c.Previous, c.Change, c.ChangePercent)
		}
		content += "\n"
	}

	content += fmt.Sprintf("---\n*请检查 EMQX 服务状态*")

	msg := WeChatMessage{
		MsgType: "markdown",
		Markdown: &struct {
			Content string `json:"content"`
			MentionedList []string `json:"mentioned_list,omitempty"`
		}{
			Content: content,
			MentionedList: nil,
		},
	}

	if err := sendWeChatMessage(msg); err != nil {
		logError("Failed to send alert: %v", err)
	} else {
		logInfo("Alert sent successfully")
	}
}

func sendWeChatMessage(msg WeChatMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	// 打印发送的 JSON 用于调试
	logInfo("Sending WeChat message: %s", string(data))

	resp, err := http.Post(config.WebhookURL, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("webhook returned status %d", resp.StatusCode)
	}

	return nil
}

func startServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", handleDashboard)
	mux.HandleFunc("/api/v1/clients/stats", handleStats)
	mux.HandleFunc("/api/v1/clients/check", handleManualCheck)
	mux.HandleFunc("/api/v1/clients/test-alert", handleTestAlert)
	mux.HandleFunc("/api/v1/clients/weekly-report", handleWeeklyReport)
	mux.HandleFunc("/api/v1/mqtt/test", handleMqttTest)
	mux.HandleFunc("/api/v1/reload", handleReload)
	mux.HandleFunc("/api/v1/clients/history", handleHistory)

	addr := fmt.Sprintf(":%d", config.ServerPort)
	logInfo("Server started on %s", addr)

	if err := http.ListenAndServe(addr, mux); err != nil {
		logError("Server failed: %v", err)
		os.Exit(1)
	}
}

func handleHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	days := 7
	if d := r.URL.Query().Get("days"); d != "" {
		if n, err := strconv.Atoi(d); err == nil && n >= 1 && n <= retentionDays {
			days = n
		}
	}

	data := history.GetHistory(days)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func handleDashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, dashboardHTML)
}

func handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	cache.mu.RLock()
	defer cache.mu.RUnlock()

	response := StatsResponse{
		Timestamp: cache.updated.Format(time.RFC3339),
		Data:      cache.data,
		Total:     cache.total,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		logError("Failed to encode response: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

// handleManualCheck 手动触发告警检查
func handleManualCheck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	go runAlertCheck()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "ok",
		"message": "Alert check triggered",
	})
}

// handleTestAlert 发送测试告警
func handleTestAlert(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	go sendTestAlert()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "ok",
		"message": "Test alert sent",
	})
}

// handleWeeklyReport 手动触发周报
func handleWeeklyReport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	go sendWeeklyReport()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "ok",
		"message": "Weekly report triggered",
	})
}

// sendTestAlert 发送测试告警
func sendTestAlert() {
	now := time.Now().Format("2006-01-02 15:04:05")

	content := fmt.Sprintf("# EMQX 监控测试消息\n\n")
	content += fmt.Sprintf("**时间**: %s\n\n", now)
	content += fmt.Sprintf("**状态**: EMQX 监控服务运行正常\n\n")
	content += fmt.Sprintf("**配置**:\n")
	content += fmt.Sprintf("- Namespace: %s\n", config.Namespace)
	content += fmt.Sprintf("- Pod: %s\n", config.Pod)
	content += fmt.Sprintf("- 采集间隔: %v\n", config.CollectInterval)
	content += fmt.Sprintf("- 检查间隔: %v\n", config.CheckInterval)
	content += fmt.Sprintf("- 服务端口: %d\n", config.ServerPort)

	msg := WeChatMessage{
		MsgType: "markdown",
		Markdown: &struct {
			Content string `json:"content"`
			MentionedList []string `json:"mentioned_list,omitempty"`
		}{
			Content: content,
			MentionedList: nil,
		},
	}

	if err := sendWeChatMessage(msg); err != nil {
		logError("Failed to send test alert: %v", err)
	} else {
		logInfo("Test alert sent successfully")
	}
}

// handleReload 处理配置重载请求
func handleReload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 重新加载配置
	oldConfig := config
	loadConfig()

	// 记录变化
	logInfo("Config reloaded:")
	if oldConfig.WebhookURL != config.WebhookURL {
		logInfo("  WebhookURL changed: %s -> %s", oldConfig.WebhookURL, config.WebhookURL)
	}
	if oldConfig.MqttTest.Broker != config.MqttTest.Broker {
		logInfo("  MQTT Broker changed: %s -> %s", oldConfig.MqttTest.Broker, config.MqttTest.Broker)
	}
	if oldConfig.MqttTest.FailureThreshold != config.MqttTest.FailureThreshold {
		logInfo("  MQTT FailureThreshold changed: %d -> %d", oldConfig.MqttTest.FailureThreshold, config.MqttTest.FailureThreshold)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "ok",
		"message": "Configuration reloaded",
		"config": map[string]interface{}{
			"mqtt_broker":        config.MqttTest.Broker,
			"mqtt_enabled":       config.MqttTest.Enabled,
			"failure_threshold":  config.MqttTest.FailureThreshold,
			"webhook_url":        config.WebhookURL,
			"alert_min_change":   config.Alert.MinChange,
			"alert_min_percent":  config.Alert.MinPercent,
			"alert_ignore_users": config.Alert.IgnoreUsers,
		},
	})
}

// generateSummaryWithClaude 调用 Claude 生成智能总结
func generateSummaryWithClaude(data string) string {
	prompt := fmt.Sprintf(`请分析以下 EMQX 用户连接数据，生成趋势总结：

%s

要求：
1. 识别各用户连接数趋势（上升/下降/平稳）
2. 标注异常变化（突增/突降）
3. 总结要简洁易懂，使用中文
4. 使用表情符号增强可读性
5. 每个用户用1-2句话总结`, data)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "claude", "-p", prompt)
	output, err := cmd.CombinedOutput()
	if err != nil {
		logError("Claude command failed: %v, output: %s", err, string(output))
		return ""
	}

	return string(output)
}

// sendWeeklyReport 发送周报
func sendWeeklyReport() {
	logInfo("Generating weekly report...")

	// 获取最近7天的数据
	thisWeek := history.GetDailyStats(7)
	if len(thisWeek) == 0 {
		logWarn("No data available for weekly report")
		return
	}

	// 获取上周数据（用于对比）
	lastWeek := history.GetDailyStats(14)
	if len(lastWeek) < 7 {
		logWarn("Not enough data for comparison")
	}

	// 获取当前状态
	cache.mu.RLock()
	currentStats := cache.data
	currentTotal := cache.total
	cache.mu.RUnlock()

	// 准备给 Claude 的数据
	var claudeData strings.Builder
	claudeData.WriteString(fmt.Sprintf("报告时间: %s\n\n", time.Now().Format("2006-01-02 15:04:05")))
	claudeData.WriteString("【本周数据】\n")
	claudeData.WriteString("日期\t平均连接数\t峰值连接数\n")
	for _, day := range thisWeek {
		claudeData.WriteString(fmt.Sprintf("%s\t%d\t%d\n", day.Date, day.AvgTotal, day.MaxTotal))
	}

	// 按用户统计
	claudeData.WriteString("\n【各用户本周连接数】\n")
	userMap := make(map[string][]int)
	for _, day := range thisWeek {
		for user, count := range day.UserStats {
			userMap[user] = append(userMap[user], count)
		}
	}
	for user, counts := range userMap {
		avg := 0
		for _, c := range counts {
			avg += c
		}
		if len(counts) > 0 {
			avg = avg / len(counts)
		}
		claudeData.WriteString(fmt.Sprintf("%s: 平均 %d, 数据: %v\n", user, avg, counts))
	}

	// 调用 Claude 生成总结
	summary := generateSummaryWithClaude(claudeData.String())

	// 构建周报消息
	content := fmt.Sprintf("# EMQX 连接监控周报\n\n")
	content += fmt.Sprintf("**报告时间**: %s\n\n", time.Now().Format("2006-01-02 15:04:05"))
	content += fmt.Sprintf("**当前总连接数**: %d\n\n", currentTotal)

	// Claude 生成的智能总结
	if summary != "" {
		content += "## 📊 智能分析\n\n"
		content += summary + "\n\n"
	}

	// 本周数据表格
	content += "## 📈 本周数据\n\n"
	content += "| 日期 | 平均连接数 | 峰值连接数 | 采样次数 |\n"
	content += "|------|------------|------------|----------|\n"
	for _, day := range thisWeek {
		content += fmt.Sprintf("| %s | %d | %d | %d |\n", day.Date, day.AvgTotal, day.MaxTotal, day.SampleCount)
	}

	// 各用户当前连接数
	content += "\n## 👥 各用户连接数\n\n"
	content += "| 用户 | 当前连接数 |\n"
	content += "|------|------------|\n"
	for _, stat := range currentStats {
		content += fmt.Sprintf("| %s | %d |\n", stat.Username, stat.Count)
	}

	content += "\n---\n*EMQX 监控服务自动生成*"

	msg := WeChatMessage{
		MsgType: "markdown",
		Markdown: &struct {
			Content string `json:"content"`
			MentionedList []string `json:"mentioned_list,omitempty"`
		}{
			Content: content,
			MentionedList: nil,
		},
	}

	if err := sendWeChatMessage(msg); err != nil {
		logError("Failed to send weekly report: %v", err)
	} else {
		logInfo("Weekly report sent successfully")
	}
}

// 日志辅助函数
func logInfo(format string, v ...interface{}) {
	log.Printf("[INFO] "+format, v...)
}

func logWarn(format string, v ...interface{}) {
	log.Printf("[WARN] "+format, v...)
}

func logError(format string, v ...interface{}) {
	log.Printf("[ERROR] "+format, v...)
}

// ============ MQTT 测试相关函数 ============

// MqttTestResult MQTT 测试结果
type MqttTestResult struct {
	Success bool
	Type    string // "pub" 或 "sub"
	Error   error
}

// runMqttTest 执行 MQTT 测试
func runMqttTest() error {
	if !config.MqttTest.Enabled {
		return fmt.Errorf("MQTT test is disabled")
	}

	logInfo("Starting MQTT test: broker=%s, topic=%s, loops=%d",
		config.MqttTest.Broker, config.MqttTest.Topic, config.MqttTest.TestLoops)

	var lastError error
	successCount := 0

	for i := 0; i < config.MqttTest.TestLoops; i++ {
		logInfo("MQTT test loop %d/%d", i+1, config.MqttTest.TestLoops)

		// 执行 Publish 测试
		pubSuccess := mqttPublishTest()
		if pubSuccess {
			successCount++
			logInfo("MQTT Publish test %d succeeded", i+1)
		} else {
			logError("MQTT Publish test %d failed", i+1)
			lastError = fmt.Errorf("publish test failed")
		}

		// 如果不是最后一次循环，等待间隔时间
		if i < config.MqttTest.TestLoops-1 {
			time.Sleep(config.MqttTest.TestInterval)
		}
	}

	totalTests := config.MqttTest.TestLoops // 只测试 publish
	if successCount < totalTests {
		mqttFailureMu.Lock()
		mqttFailureCount++
		currentCount := mqttFailureCount
		mqttFailureMu.Unlock()

		logWarn("MQTT test completed with failures: %d/%d succeeded, failure count: %d",
			successCount, totalTests, currentCount)

		// 达到告警阈值，发送告警
		if currentCount >= config.MqttTest.FailureThreshold {
			sendMqttTestAlert(successCount, totalTests, currentCount)
			// 重置计数器
			mqttFailureMu.Lock()
			mqttFailureCount = 0
			mqttFailureMu.Unlock()
		}

		return lastError
	}

	// 测试全部成功，重置失败计数
	mqttFailureMu.Lock()
	mqttFailureCount = 0
	mqttFailureMu.Unlock()

	logInfo("MQTT test completed successfully: %d/%d succeeded", successCount, totalTests)
	return nil
}

// mqttPublishTest MQTT 发布测试
func mqttPublishTest() bool {
	broker := fmt.Sprintf("tcp://%s", config.MqttTest.Broker)

	opts := mqtt.NewClientOptions()
	opts.AddBroker(broker)
	opts.SetUsername(config.MqttTest.Username)
	opts.SetPassword(config.MqttTest.Password)
	opts.SetClientID("emqx-monitor-pub-" + randomString(8))
	opts.SetCleanSession(true)
	opts.SetAutoReconnect(false)

	client := mqtt.NewClient(opts)
	defer client.Disconnect(250)

	if token := client.Connect(); token.WaitTimeout(config.MqttTest.Timeout) && token.Error() != nil {
		logError("MQTT publish connect failed: %v", token.Error())
		return false
	}

	token := client.Publish(config.MqttTest.Topic, 0, false, "hello nova")
	if token.WaitTimeout(config.MqttTest.Timeout) && token.Error() != nil {
		logError("MQTT publish failed: %v", token.Error())
		return false
	}

	return true
}

// mqttSubscribeTest MQTT 订阅测试
func mqttSubscribeTest() bool {
	broker := fmt.Sprintf("tcp://%s", config.MqttTest.Broker)

	opts := mqtt.NewClientOptions()
	opts.AddBroker(broker)
	opts.SetUsername(config.MqttTest.Username)
	opts.SetPassword(config.MqttTest.Password)
	opts.SetClientID("emqx-monitor-sub-" + randomString(8))
	opts.SetCleanSession(true)
	opts.SetAutoReconnect(false)

	client := mqtt.NewClient(opts)

	if token := client.Connect(); token.WaitTimeout(config.MqttTest.Timeout) && token.Error() != nil {
		logError("MQTT subscribe connect failed: %v", token.Error())
		return false
	}
	defer client.Disconnect(250)

	// 创建一个通道来接收订阅结果
	done := make(chan bool, 1)

	// 订阅主题
	token := client.Subscribe(config.MqttTest.Topic, 0, func(client mqtt.Client, msg mqtt.Message) {
		done <- true
	})
	if token.WaitTimeout(config.MqttTest.Timeout) && token.Error() != nil {
		logError("MQTT subscribe failed: %v", token.Error())
		return false
	}

	// 等待消息或超时
	select {
	case <-done:
		return true
	case <-time.After(config.MqttTest.Timeout):
		logError("MQTT subscribe timeout: no message received")
		return false
	}
}

// sendMqttTestAlert 发送 MQTT 测试告警
func sendMqttTestAlert(successCount, totalTests, failureCount int) {
	now := time.Now().Format("2006-01-02 15:04:05")

	content := fmt.Sprintf("### %s MQTT publish 探测通知\n\n", config.MqttTest.ClassName)
	content += fmt.Sprintf("> **mqtt broker：** %s\n", config.MqttTest.Broker)
	content += fmt.Sprintf("> **topic：** %s\n", config.MqttTest.Topic)
	content += fmt.Sprintf("> **status：** publish message failed, please check!!!\n")
	content += fmt.Sprintf("> **test result：** %d/%d succeeded\n", successCount, totalTests)
	content += fmt.Sprintf("> **failure count：** %d\n\n", failureCount)
	content += fmt.Sprintf("**time：** %s\n", now)
	content += fmt.Sprintf("<@Nova006393>") // 文本中也显示

	msg := WeChatMessage{
		MsgType: "markdown",
		Markdown: &struct {
			Content string `json:"content"`
			MentionedList []string `json:"mentioned_list,omitempty"`
		}{
			Content: content,
			MentionedList: []string{"Nova006393"}, // @ 提醒用户
		},
	}

	if err := sendWeChatMessage(msg); err != nil {
		logError("Failed to send MQTT test alert: %v", err)
	} else {
		logInfo("MQTT test alert sent successfully")
	}
}

// randomString 生成随机字符串
func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[time.Now().UnixNano()%int64(len(charset))]
		time.Sleep(time.Nanosecond)
	}
	return string(b)
}

// handleMqttTest 处理 MQTT 测试请求
func handleMqttTest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 在后台执行 MQTT 测试
	go func() {
		if err := runMqttTest(); err != nil {
			logError("MQTT test failed: %v", err)
		}
	}()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "ok",
		"message": "MQTT test triggered",
	})
}
