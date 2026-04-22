#!/bin/bash
# 用法: ./wait_for_cool.sh [目标温度°C，默认65]
# 功能: 等待 CPU 最高温度降至目标值以下，用于两次对比实验之间

TARGET_TEMP=${1:-65}
CHECK_INTERVAL=10

echo "等待 CPU 温度降至 ${TARGET_TEMP}°C 以下..."

while true; do
    MAX_TEMP_RAW=$(cat /sys/class/thermal/thermal_zone*/temp 2>/dev/null | sort -n | tail -1)
    if [ -z "$MAX_TEMP_RAW" ]; then
        echo "未找到温度传感器，跳过等待"
        break
    fi

    MAX_TEMP_C=$((MAX_TEMP_RAW / 1000))
    echo -ne "\r当前最高温度: ${MAX_TEMP_C}°C  目标: <${TARGET_TEMP}°C   "

    if [ "$MAX_TEMP_C" -lt "$TARGET_TEMP" ]; then
        echo -e "\n温度已稳定，可以开始下一次实验"
        break
    fi

    sleep $CHECK_INTERVAL
done
