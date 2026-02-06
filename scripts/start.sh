#!/bin/bash
# EMQX Monitor 启动脚本

set -e

# 项目根目录
PROJECT_DIR="/opt/emqx/emqx-monitor"
cd "$PROJECT_DIR"

# 创建日志目录
mkdir -p logs

# 检查二进制文件
if [ ! -f "bin/emqx-monitor" ]; then
    echo "Error: bin/emqx-monitor not found"
    echo "Please run: go build -o bin/emqx-monitor main.go"
    exit 1
fi

# 检查配置文件
if [ ! -f "config/config.yaml" ]; then
    echo "Warning: config/config.yaml not found, using defaults"
fi

# 启动服务
echo "Starting EMQX Monitor..."
exec bin/emqx-monitor
