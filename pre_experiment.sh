#!/bin/bash
# 功能: 每次实验前的环境准备，确保两次对比实验的起始条件一致
# 用法: sudo ./pre_experiment.sh [baseline|experiment]
# 示例: sudo ./pre_experiment.sh baseline
#       sudo ./pre_experiment.sh experiment

MODE=${1:-""}

echo "========================================"
echo " 实验前环境准备"
echo "========================================"

# ---------- [1] 关闭 Turbo Boost ----------
echo ""
echo "[1/5] 关闭 Turbo Boost..."
if [ -f /sys/devices/system/cpu/intel_pstate/no_turbo ]; then
    echo 1 | sudo tee /sys/devices/system/cpu/intel_pstate/no_turbo > /dev/null
    VAL=$(cat /sys/devices/system/cpu/intel_pstate/no_turbo)
    if [ "$VAL" = "1" ]; then
        echo "      Turbo Boost 已关闭 (no_turbo=1)"
    else
        echo "      [警告] Turbo Boost 关闭失败，当前值: $VAL"
    fi
else
    echo "      未找到 intel_pstate，跳过（可能是 AMD CPU 或其他驱动）"
fi

# ---------- [2] 确认 CPU 调速器 ----------
# 说明: 该系统使用 Intel P-state active 模式，仅支持 performance/powersave，
#       无法切换到 schedutil。直接检查当前状态即可，不做切换。
echo ""
echo "[2/5] 确认 CPU 调速器..."
GOV=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null)
echo "      当前调速器: ${GOV:-未知}"
if [ "$GOV" = "performance" ]; then
    echo "      调速器正常 (performance)"
else
    echo "      [警告] 调速器为 ${GOV}，建议为 performance"
    echo "      可用调速器: $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_available_governors 2>/dev/null)"
fi

# ---------- [3] 等待温度稳定（仅在两次实验之间需要）----------
echo ""
echo "[3/5] 检查 CPU 温度..."
MAX_TEMP_RAW=$(cat /sys/class/thermal/thermal_zone*/temp 2>/dev/null | sort -n | tail -1)
if [ -n "$MAX_TEMP_RAW" ]; then
    MAX_TEMP_C=$((MAX_TEMP_RAW / 1000))
    echo "      当前最高温度: ${MAX_TEMP_C}°C"
    if [ "$MAX_TEMP_C" -ge 65 ]; then
        echo "      温度偏高，等待降温..."
        bash "$(dirname "$0")/wait_for_cool.sh" 65
    else
        echo "      温度正常，无需等待"
    fi
else
    echo "      未找到温度传感器，跳过"
fi

# ---------- [4] 清理容器 ----------
echo ""
echo "[4/5] 清理所有 Docker 容器..."
CONTAINERS=$(sudo docker ps -aq 2>/dev/null)
if [ -n "$CONTAINERS" ]; then
    sudo docker rm -fv $CONTAINERS
    echo "      容器已清理"
else
    echo "      无运行中的容器"
fi

# ---------- [5] 清理文件系统缓存 ----------
echo ""
echo "[5/5] 清理文件系统缓存..."
sync
echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
echo "      Page Cache 已清理"

# ---------- 完成 ----------
echo ""
echo "========================================"
echo " 环境准备完成"
if [ -n "$MODE" ]; then
    echo " 准备运行: $MODE"
fi
echo "========================================"
echo ""
echo "接下来执行（服务器端）："
echo "  sudo docker run -d --name redis --cpuset-cpus=\"1\" --cpuset-mems=\"1\" -p 6379:6379 redis"
echo "  sudo docker run -d --name couchdb-test --cpuset-cpus=\"3\" --cpuset-mems=\"1\" \\"
echo "      -p 5984:5984 -e COUCHDB_USER=openwhisk -e COUCHDB_PASSWORD=openwhisk apache/couchdb:2.3"
echo "  python3 ./actions/network/server.py &"
echo ""
if [ "$MODE" = "baseline" ]; then
    echo "  TASK_GROUPS_FILE=baseline_groups.json python3 controller.py > base.log 2>&1"
elif [ "$MODE" = "experiment" ]; then
    echo "  TASK_GROUPS_FILE=task_groups.json python3 controller.py > exp.log 2>&1"
else
    echo "  TASK_GROUPS_FILE=baseline_groups.json python3 controller.py > base.log 2>&1"
    echo "  # 或"
    echo "  TASK_GROUPS_FILE=task_groups.json python3 controller.py > exp.log 2>&1"
fi
