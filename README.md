# EMQX 监控服务

EMQX 连接监控告警服务，支持连接数趋势分析、周报生成、MQTT 连通性测试。
<img width="1900" height="648" alt="image" src="https://github.com/user-attachments/assets/543efd37-8563-4c0c-bce5-e0bdb6e0106a" />


## 功能特性

### 🔍 连接监控
- 实时采集 EMQX 用户连接数据（每 30 秒）
- 自动统计各用户连接数
- 保留 30 天历史数据
- **数据持久化**：历史数据自动保存到磁盘，服务重启不丢失

### 📊 智能告警
- **环比告警**：对比 1 小时前数据，发现连接数下降
- **同比告警**：对比上周同期数据，识别长期趋势
- 支持 Claude AI 智能分析趋势

### 📱 企业微信通知
- 实时告警推送（@ 提醒）
- 每周定时周报（默认周一 9:00）
- 支持 Markdown 格式

### 🧪 MQTT 连通性测试
- Publish 测试
- 失败累积告警机制（避免误报）
- 可手动触发或定时执行

### ⚡ 热更新
- 修改配置无需重启
- HTTP API 触发重载

### 📈 Web 趋势图
- 内置可视化仪表板，浏览器直接访问
- 支持 1 天 / 7 天 / 14 天 / 30 天时间范围切换
- 总连接数趋势 + 按用户分列的折线图
- 每 30 秒自动刷新

## 安装部署

### 前置要求
- Go 1.24+
- systemd
- kubectl（连接 Kubernetes 集群）

### 编译

```bash
cd /opt/emqx/emqx-monitor
go build -o bin/emqx-monitor main.go
```

### 配置

编辑 `config/config.yaml`：

```yaml
# EMQX K8s 配置
emqx:
  namespace: ems-au
  pod: emqx-eu-0

# 采集间隔
collect_interval: 30s

# 告警检查间隔
check_interval: 1h

# HTTP 服务端口
server_port: 8080

# 企业微信 Webhook
webhook_url: https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_KEY

# 周报配置
report:
  enabled: true
  day: 1        # 0=周日, 1=周一, ..., 6=周六
  hour: 9
  minute: 0

# MQTT 连通性测试配置
mqtt_test:
  enabled: true
  broker: "YOUR_BROKER:1883"
  topic: "test/monitor"
  username: "YOUR_USERNAME"
  password: "YOUR_PASSWORD"
  class_name: "EMQX"
  timeout: 5
  test_loops: 5
  test_interval: 10
  failure_threshold: 1    # 失败几次后告警
  schedule: ""            # 定时执行（cron 格式，留空则手动触发）

# 日志配置
log:
  level: info
  file: /opt/emqx/emqx-monitor/logs/emqx-monitor.log
  max_size: 100
  max_backups: 10
  max_age: 30
  compress: true
```

### 安装服务

```bash
# 复制 systemd 服务文件
cp systemd/emqx-monitor.service /etc/systemd/system/

# 重载 systemd
systemctl daemon-reload

# 启用开机自启
systemctl enable emqx-monitor

# 启动服务
systemctl start emqx-monitor

# 查看状态
systemctl status emqx-monitor
```

## API 接口

### Web 仪表板

浏览器访问 `http://<ip>:8080/` 查看连接数趋势图。

### 1. 获取当前连接统计

```bash
curl http://localhost:8080/api/v1/clients/stats
```

响应：
```json
{
  "timestamp": "2026-02-06T08:30:00Z",
  "data": [
    {"username": "novaems", "count": 24},
    {"username": "edge", "count": 3},
    {"username": "rct_au", "count": 24}
  ],
  "total": 51
}
```

### 2. 手动触发告警检查

```bash
curl -X POST http://localhost:8080/api/v1/clients/check
```

### 3. 发送测试告警

```bash
curl -X POST http://localhost:8080/api/v1/clients/test-alert
```

### 4. 手动触发周报

```bash
curl -X POST http://localhost:8080/api/v1/clients/weekly-report
```

### 5. MQTT 连通性测试

```bash
curl -X POST http://localhost:8080/api/v1/mqtt/test
```

### 6. 热更新配置

```bash
curl -X POST http://localhost:8080/api/v1/reload
```

响应：
```json
{
  "status": "ok",
  "message": "Configuration reloaded",
  "config": {
    "mqtt_broker": "YOUR_BROKER:1883",
    "mqtt_enabled": true,
    "failure_threshold": 1,
    "webhook_url": "https://qyapi.weixin.qq.com/..."
  }
}
```

### 7. 获取历史数据

```bash
# 获取最近 7 天的历史数据（默认）
curl http://localhost:8080/api/v1/clients/history?days=7

# 获取最近 1 天
curl http://localhost:8080/api/v1/clients/history?days=1
```

响应：
```json
[
  {
    "timestamp": "2026-03-30T03:01:23Z",
    "user_stats": {"novaems": 20, "rct_au": 24},
    "total": 48
  }
]
```

## 数据持久化

历史数据以 JSON Lines 格式存储在 `data/history.jsonl`，每次采集自动追加写入。服务重启时自动加载恢复。

- **文件位置**：`data/history.jsonl`
- **保留周期**：30 天
- **自动压缩**：每天凌晨 3:00 自动清理过期数据
- **数据安全**：写入失败仅记录日志，不影响服务运行

## 告警逻辑

### 连接数告警

| 检查类型 | 对比基准 | 触发条件 |
|---------|---------|---------|
| 环比 | 1 小时前 | 连接数下降 |
| 同比 | 上周同期 | 连接数下降 |

### MQTT 测试告警

- 测试失败累积计数
- 达到 `failure_threshold` 时发送告警
- 测试成功后重置计数

## 日志

```bash
# 实时查看日志
journalctl -u emqx-monitor -f

# 查看日志文件
tail -f /opt/emqx/emqx-monitor/logs/emqx-monitor.log
```

## 常见问题

### Q: 修改配置后需要重启吗？

A: 不需要，使用热更新 API：
```bash
curl -X POST http://localhost:8080/api/v1/reload
```

### Q: 如何停止周报？

A: 在 `config.yaml` 中设置：
```yaml
report:
  enabled: false
```

### Q: 如何修改企微 @ 人员？

A: 编辑 `main.go` 中的 `sendMqttTestAlert` 函数，修改 `MentionedList`：
```go
MentionedList: []string{"USER_ID"},
```

### Q: MQTT 测试失败但不告警？

A: 检查 `failure_threshold` 设置，默认为 1，表示失败 1 次即告警。

## 项目结构

```
emqx-monitor/
├── bin/                    # 编译后的二进制文件
├── config/
│   └── config.yaml         # 配置文件
├── data/
│   └── history.jsonl       # 历史数据（持久化）
├── logs/                   # 日志目录
├── scripts/
│   └── start.sh            # 启动脚本
├── systemd/
│   └── emqx-monitor.service# systemd 服务文件
├── main.go                 # 主程序
├── go.mod
├── go.sum
└── README.md
```

## 许可证

MIT License
