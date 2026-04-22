#!/bin/bash
set -eo pipefail
# 提高文件描述符上限，避免大量容器并发时 "Too many open files"
ulimit -n 65536
# =============================================================================
# run_all_experiments.sh  —  服务器端自动化多轮实验脚本
#
# 同步方式：通过 controller 的 HTTP 接口与客户端协调，无需共享目录或 SSH。
#
# 用法：sudo ./run_all_experiments.sh [轮数，默认5] [定时启动时间，如 "02:00"]
# 示例：
#   sudo ./run_all_experiments.sh 5             # 立即开始，跑5轮
#   sudo ./run_all_experiments.sh 5 02:00       # 等到凌晨2点开始，跑5轮
#   sudo ./run_all_experiments.sh 3 "2026-04-17 02:00"  # 指定日期+时间
# =============================================================================

ROUNDS=${1:-5}
START_TIME=${2:-""}
# 使用当前环境的 Python 解释器（兼容 venv）
PYTHON=${PYTHON:-$(which python3)}
CONTROLLER_URL="http://127.0.0.1:5002"
RESULTS_DIR="./closed_loop_results"
CSV_DIR="./closed_loop_results/csv"
PLOTS_DIR="./closed_loop_results/plots"
CONTROLLER_LOG_DIR="./controller_logs"
COOL_TEMP=70
CONTROLLER_PID=""
# 等待客户端完成的最大超时（秒）：TEST_DURATION + 10分钟缓冲
MAX_WAIT_CLIENT=${MAX_WAIT_CLIENT:-1800}

mkdir -p "$RESULTS_DIR" "$CSV_DIR" "$PLOTS_DIR" "$CONTROLLER_LOG_DIR"

# ── 工具函数 ──────────────────────────────────────────────────────────────────

log() { echo "[$(date '+%H:%M:%S')] $*"; }

wait_port_free() {
    local port="$1"
    local max_wait="${2:-15}"
    local waited=0
    while ss -tlnp 2>/dev/null | grep -q ":${port} " ; do
        if [ "$waited" -ge "$max_wait" ]; then
            log "[WARN] 端口 $port 在 ${max_wait}s 内未释放"
            return 1
        fi
        sleep 1
        waited=$((waited + 1))
    done
    return 0
}

stop_controller() {
    if [ -n "$CONTROLLER_PID" ] && kill -0 "$CONTROLLER_PID" 2>/dev/null; then
        log "停止 controller (PID=$CONTROLLER_PID)..."
        kill "$CONTROLLER_PID"
        wait "$CONTROLLER_PID" 2>/dev/null || true
        CONTROLLER_PID=""
    fi
    # 确保没有残留的 controller 进程占用端口
    pkill -f "controller.py" 2>/dev/null || true
    sleep 2
    # 如果端口仍被占用，强制杀占用进程
    local pid_on_port
    pid_on_port=$(ss -tlnp 2>/dev/null | grep ":5002 " | grep -oP 'pid=\K[0-9]+' | head -1 || true)
    if [ -n "$pid_on_port" ]; then
        log "[WARN] 端口 5002 仍被 PID=$pid_on_port 占用，强制终止"
        kill -9 "$pid_on_port" 2>/dev/null || true
        sleep 1
    fi
}

cleanup_env() {
    local round="$1"
    local mode="$2"
    log "[$round/$ROUNDS][$mode] 清理环境..."

    stop_controller

    # 删除所有容器（包括 redis 和 couchdb），确保每轮实验从完全干净的状态开始
    ALL=$(sudo docker ps -aq 2>/dev/null)
    if [ -n "$ALL" ]; then
        sudo docker rm -fv $ALL > /dev/null 2>&1 || true
    fi

    # 停止残留的 network server
    pkill -f "actions/network/server.py" 2>/dev/null || true

    sync && echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
    log "[$round/$ROUNDS][$mode] 清理完成"
}

start_base_services() {
    log "启动基础服务 (redis, couchdb, network)..."

    sudo docker run -d --name redis --cpuset-cpus="1" --cpuset-mems="1" \
        -p 6379:6379 redis > /dev/null

    sudo docker run -d --name couchdb-test --cpuset-cpus="3" --cpuset-mems="1" \
        -p 5984:5984 -e COUCHDB_USER=openwhisk -e COUCHDB_PASSWORD=openwhisk \
        apache/couchdb:2.3 > /dev/null

    # 等待 redis 就绪
    local waited=0
    while ! sudo docker exec redis redis-cli ping 2>/dev/null | grep -q PONG; do
        if [ "$waited" -ge 30 ]; then
            log "[ERROR] redis 启动超时"
            return 1
        fi
        sleep 1
        waited=$((waited + 1))
    done
    log "redis 已就绪"

    # 等待 couchdb 就绪
    waited=0
    while ! curl -s http://127.0.0.1:5984/ > /dev/null 2>&1; do
        if [ "$waited" -ge 30 ]; then
            log "[ERROR] couchdb 启动超时"
            return 1
        fi
        sleep 1
        waited=$((waited + 1))
    done
    log "couchdb 已就绪"

    if ! pgrep -f "actions/network/server.py" > /dev/null; then
        $PYTHON ./actions/network/server.py > /dev/null 2>&1 &
        sleep 2
    fi
}

wait_controller_up() {
    log "等待 controller 启动..."
    for i in $(seq 1 30); do
        if curl -s "$CONTROLLER_URL/experiment_ready" > /dev/null 2>&1; then
            log "controller 已就绪"
            return 0
        fi
        sleep 2
    done
    log "[ERROR] controller 启动超时"
    return 1
}

set_ready() {
    local round="$1"
    local mode="$2"
    curl -s -X POST "$CONTROLLER_URL/set_ready" \
        -H "Content-Type: application/json" \
        -d "{\"round\": $round, \"mode\": \"$mode\"}" > /dev/null
    log "[$round/$ROUNDS][$mode] 已通知客户端：服务器就绪"
}

wait_client_done() {
    local round="$1"
    local mode="$2"
    local elapsed=0
    log "[$round/$ROUNDS][$mode] 等待客户端完成 (超时: ${MAX_WAIT_CLIENT}s)..."
    while true; do
        RESP=$(curl -s "$CONTROLLER_URL/wait_client_done")
        DONE=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('client_done', False))" 2>/dev/null)
        if [ "$DONE" = "True" ]; then
            log "[$round/$ROUNDS][$mode] 客户端已完成"
            return 0
        fi
        sleep 5
        elapsed=$((elapsed + 5))
        if [ "$elapsed" -ge "$MAX_WAIT_CLIENT" ]; then
            log "[ERROR] [$round/$ROUNDS][$mode] 等待客户端超时 (${MAX_WAIT_CLIENT}s)，放弃本轮"
            return 1
        fi
    done
}

run_one_mode() {
    local round="$1"
    local mode="$2"
    local groups_file log_file

    [ "$mode" = "baseline" ] && groups_file="baseline_groups.json" || groups_file="task_groups.json"
    log_file="$CONTROLLER_LOG_DIR/round${round}_${mode}.log"

    log "[$round/$ROUNDS][$mode] ===== 开始 ====="
    log "[$round/$ROUNDS][$mode] 当前频率: $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq 2>/dev/null) kHz"

    # 等待温度
    MAX_TEMP_RAW=$(cat /sys/class/thermal/thermal_zone*/temp 2>/dev/null | sort -n | tail -1)
    if [ -n "$MAX_TEMP_RAW" ] && [ $((MAX_TEMP_RAW / 1000)) -ge "$COOL_TEMP" ]; then
        bash "$(dirname "$0")/wait_for_cool.sh" "$COOL_TEMP"
    fi
    sync && echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null

    # 启动基础服务和 controller
    start_base_services || { log "[ERROR] 基础服务启动失败"; return 1; }
    TASK_GROUPS_FILE="$groups_file" $PYTHON controller.py > "$log_file" 2>&1 &
    CONTROLLER_PID=$!
    sleep 1
    # 检查 controller 是否存活（可能端口冲突立即退出）
    if ! kill -0 "$CONTROLLER_PID" 2>/dev/null; then
        log "[ERROR] controller 启动后立即退出，请查看日志: $log_file"
        CONTROLLER_PID=""
        return 1
    fi

    wait_controller_up || { stop_controller; return 1; }

    # 启动系统监控
    curl -s -X POST "$CONTROLLER_URL/start_monitor" \
        -H "Content-Type: application/json" -d '{}' > /dev/null 2>&1 \
        && log "[$round/$ROUNDS][$mode] 系统监控已启动" \
        || log "[WARN] [$round/$ROUNDS][$mode] 启动系统监控失败"

    # 通知客户端就绪，等待其完成
    set_ready "$round" "$mode"
    wait_client_done "$round" "$mode"
    local client_rc=$?

    # 停止系统监控
    curl -s -X POST "$CONTROLLER_URL/stop_monitor" > /dev/null 2>&1 \
        && log "[$round/$ROUNDS][$mode] 系统监控已停止" \
        || log "[WARN] [$round/$ROUNDS][$mode] 停止系统监控失败"

    # 保存 server_metrics.csv 到 closed_loop_results/csv/ 目录
    if [ -f "server_metrics.csv" ] && [ -s "server_metrics.csv" ]; then
        local metrics_csv="$CSV_DIR/round${round}_${mode}_server_metrics.csv"
        cp "server_metrics.csv" "$metrics_csv"
        rm -f "server_metrics.csv"
        log "[$round/$ROUNDS][$mode] server_metrics 已保存: $(basename $metrics_csv)"

        # 自动生成 CPU 利用率和频率网格图
        local out_prefix="$PLOTS_DIR/round${round}_${mode}"
        $PYTHON plot_server_metrics.py "$metrics_csv" --out-prefix "$out_prefix" \
            && log "[$round/$ROUNDS][$mode] 服务器监控图已生成" \
            || log "[WARN] [$round/$ROUNDS][$mode] 服务器监控图生成失败"
    else
        log "[WARN] [$round/$ROUNDS][$mode] server_metrics.csv 不存在或为空"
    fi

    log "[$round/$ROUNDS][$mode] ===== 完成 ====="
    return $client_rc
}

# ── 主流程 ────────────────────────────────────────────────────────────────────

# 定时等待：如果指定了启动时间，先等到该时刻再开始
if [ -n "$START_TIME" ]; then
    # 解析目标时间戳（支持 "HH:MM" 或 "YYYY-MM-DD HH:MM" 格式）
    if echo "$START_TIME" | grep -qP '^\d{2}:\d{2}$'; then
        # 仅指定时间，自动补今天或明天的日期
        TARGET_TS=$(date -d "today $START_TIME" +%s 2>/dev/null)
        NOW_TS=$(date +%s)
        if [ "$TARGET_TS" -le "$NOW_TS" ]; then
            # 指定时间已过，推到明天
            TARGET_TS=$(date -d "tomorrow $START_TIME" +%s)
        fi
    else
        TARGET_TS=$(date -d "$START_TIME" +%s 2>/dev/null)
    fi

    if [ -z "$TARGET_TS" ]; then
        log "[ERROR] 无法解析启动时间: '$START_TIME'"
        exit 1
    fi

    TARGET_STR=$(date -d "@$TARGET_TS" '+%Y-%m-%d %H:%M:%S')
    NOW_TS=$(date +%s)
    WAIT_SECS=$((TARGET_TS - NOW_TS))

    if [ "$WAIT_SECS" -gt 0 ]; then
        log "=============================="
        log " 定时启动: $TARGET_STR"
        log " 距离启动还有 $((WAIT_SECS / 3600))h $((WAIT_SECS % 3600 / 60))m"
        log " (Ctrl+C 可取消)"
        log "=============================="
        sleep "$WAIT_SECS"
    fi
fi

log "=============================="
log " 自动化实验开始，共 $ROUNDS 轮"
log "=============================="

# 保存原始状态，实验结束后恢复（避免污染共享机器）
ORIG_GOV=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo "unknown")
ORIG_TURBO=$(cat /sys/devices/system/cpu/intel_pstate/no_turbo 2>/dev/null || echo "unknown")
ORIG_SCALING_MAX=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_max_freq 2>/dev/null || echo "unknown")
log "保存原始状态: governor=$ORIG_GOV, no_turbo=$ORIG_TURBO, scaling_max_freq=$ORIG_SCALING_MAX kHz"

restore_env() {
    log "恢复机器原始状态..."
    # 恢复调速器
    if [ "$ORIG_GOV" != "unknown" ]; then
        echo "$ORIG_GOV" | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor > /dev/null 2>&1
        log "governor 已恢复为 $ORIG_GOV"
    fi
    # 恢复 Turbo Boost
    if [ "$ORIG_TURBO" != "unknown" ] && [ -f /sys/devices/system/cpu/intel_pstate/no_turbo ]; then
        echo "$ORIG_TURBO" | sudo tee /sys/devices/system/cpu/intel_pstate/no_turbo > /dev/null 2>&1
        log "no_turbo 已恢复为 $ORIG_TURBO"
    fi
    # 恢复 scaling_max_freq
    if [ "$ORIG_SCALING_MAX" != "unknown" ]; then
        for cpu_dir in /sys/devices/system/cpu/cpu*/cpufreq; do
            echo "$ORIG_SCALING_MAX" | sudo tee "${cpu_dir}/scaling_max_freq" > /dev/null 2>&1 || true
        done
        log "scaling_max_freq 已恢复为 $ORIG_SCALING_MAX kHz"
    fi
    # 恢复 C-state
    if command -v cpupower &>/dev/null; then
        cpupower idle-set -E > /dev/null 2>&1 || true
        log "C-state 已恢复"
    fi
    # 恢复 IRQ 均衡
    systemctl start irqbalance 2>/dev/null || true
    log "irqbalance 已恢复"
    # 恢复 Swap
    sudo swapon -a 2>/dev/null || true
    log "Swap 已恢复"
    # 恢复 NTP
    systemctl start chronyd 2>/dev/null || systemctl start ntp 2>/dev/null || true
    log "NTP 已恢复"
    # 恢复 THP
    echo madvise | sudo tee /sys/kernel/mm/transparent_hugepage/enabled > /dev/null 2>&1 || true
    log "THP 已恢复为 madvise"
    # 清理残留容器
    stop_controller
    ALL=$(sudo docker ps -aq 2>/dev/null)
    if [ -n "$ALL" ]; then
        sudo docker rm -fv $ALL > /dev/null 2>&1 || true
    fi
    pkill -f "actions/network/server.py" 2>/dev/null || true
    log "机器状态已恢复"
}
# 无论正常结束还是异常退出（Ctrl+C、kill）都恢复
trap restore_env EXIT

# 关闭 Turbo Boost
if [ -f /sys/devices/system/cpu/intel_pstate/no_turbo ]; then
    echo 1 | sudo tee /sys/devices/system/cpu/intel_pstate/no_turbo > /dev/null
    log "Turbo Boost 已关闭"
fi

# 禁用深度 C-state，防止 CPU 空闲时跌到 800 MHz
if command -v cpupower &>/dev/null; then
    cpupower idle-set -D 0 > /dev/null 2>&1 \
        && log "C-state 深度睡眠已禁用" \
        || log "[WARN] cpupower idle-set 失败，C-state 未禁用"
else
    log "[WARN] cpupower 未找到，C-state 未禁用"
fi

# 禁用 Swap，避免内存压力引入延迟毛刺
sudo swapoff -a 2>/dev/null \
    && log "Swap 已禁用" \
    || log "[WARN] swapoff 失败"

# 停止 irqbalance，将所有中断集中到 CPU 5（非实验 CPU）
systemctl stop irqbalance 2>/dev/null || true
for irq_affinity in /proc/irq/*/smp_affinity_list; do
    echo 5 | sudo tee "$irq_affinity" > /dev/null 2>&1 || true
done
log "IRQ 已集中到 CPU 5"

# 暂停 NTP 校时，防止实验期间时钟跳变
systemctl stop chronyd 2>/dev/null || systemctl stop ntp 2>/dev/null || true
log "NTP 已暂停"

# 禁用透明大页（THP），避免内存合并/分裂引入延迟
echo never | sudo tee /sys/kernel/mm/transparent_hugepage/enabled > /dev/null 2>&1 \
    && log "THP 已设为 never" \
    || log "[WARN] THP 设置失败"

# 设置 CPU 调速器为 performance，并确保 scaling_max_freq 等于硬件最大频率
GOV=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null)
if [ "$GOV" != "performance" ]; then
    echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor > /dev/null
    log "CPU 调速器已从 '$GOV' 切换为 performance"
else
    log "CPU 调速器已为 performance"
fi
for cpu_dir in /sys/devices/system/cpu/cpu*/cpufreq; do
    [ -f "${cpu_dir}/cpuinfo_max_freq" ] && \
        cat "${cpu_dir}/cpuinfo_max_freq" | sudo tee "${cpu_dir}/scaling_max_freq" > /dev/null 2>&1 || true
done
log "scaling_max_freq 已重置为硬件最大频率"

for round in $(seq 1 "$ROUNDS"); do
    log "====== 第 $round / $ROUNDS 轮 ======"

    cleanup_env "$round" "baseline"
    run_one_mode "$round" "baseline" || log "[ERROR] 第 $round 轮 baseline 失败，继续下一模式"

    cleanup_env "$round" "experiment"
    run_one_mode "$round" "experiment" || log "[ERROR] 第 $round 轮 experiment 失败，继续下一轮"

done

log "=============================="
log " 全部 $ROUNDS 轮实验完成"
log " server_metrics 目录: $CSV_DIR"
log " controller 日志目录: $CONTROLLER_LOG_DIR"
log "=============================="
