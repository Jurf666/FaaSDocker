#!/bin/bash
set -eo pipefail
# =============================================================================
# run_all_experiments.sh  —  客户端自动化多轮实验脚本
#
# 同步方式：轮询服务器 controller 的 HTTP 接口，无需共享目录或 SSH。
#
# 用法：./run_all_experiments.sh [轮数，默认5]
# =============================================================================

ROUNDS=${1:-5}
# 使用当前环境的 Python 解释器（兼容 venv）
PYTHON=${PYTHON:-$(which python3)}
CONTROLLER_URL="http://${CONTROLLER_HOST:-10.2.27.23}:${CONTROLLER_PORT:-5002}"
RESULTS_DIR="./closed_loop_results/multi_round"
JSON_DIR="$RESULTS_DIR/json"
CLIENT_LOG_DIR="./client_logs"
# 等待服务器就绪的最大超时（秒）
MAX_WAIT_SERVER=${MAX_WAIT_SERVER:-1800}

mkdir -p "$RESULTS_DIR" "$JSON_DIR" "$CLIENT_LOG_DIR"

# ── 工具函数 ──────────────────────────────────────────────────────────────────

log() { echo "[$(date '+%H:%M:%S')] $*"; }

wait_server_ready() {
    local round="$1"
    local mode="$2"
    local elapsed=0
    local server_contacted=false
    log "[$round/$ROUNDS][$mode] 等待服务器就绪..."
    while true; do
        RESP=$(curl -s "$CONTROLLER_URL/experiment_ready" 2>/dev/null)
        STATUS=$(echo "$RESP" | python3 -c \
            "import sys,json; print(json.load(sys.stdin).get('status',''))" 2>/dev/null)
        R=$(echo "$RESP" | python3 -c \
            "import sys,json; print(json.load(sys.stdin).get('round',0))" 2>/dev/null)
        M=$(echo "$RESP" | python3 -c \
            "import sys,json; print(json.load(sys.stdin).get('mode',''))" 2>/dev/null)

        # 确认是当前轮次和模式的就绪信号，避免收到上一轮的残留状态
        if [ "$STATUS" = "ready" ] && [ "$R" = "$round" ] && [ "$M" = "$mode" ]; then
            log "[$round/$ROUNDS][$mode] 服务器已就绪，开始实验"
            return 0
        fi

        # 服务器已响应但还没 ready（如 not_ready），说明 controller 已启动
        # 此时才开始计算超时，避免定时启动场景下提前超时
        if [ -n "$STATUS" ]; then
            if [ "$server_contacted" = false ]; then
                server_contacted=true
                elapsed=0
                log "[$round/$ROUNDS][$mode] 已连接服务器，等待就绪信号 (超时: ${MAX_WAIT_SERVER}s)..."
            fi
            elapsed=$((elapsed + 3))
            if [ "$elapsed" -ge "$MAX_WAIT_SERVER" ]; then
                log "[ERROR] [$round/$ROUNDS][$mode] 等待服务器超时 (${MAX_WAIT_SERVER}s)，放弃本轮"
                return 1
            fi
        fi
        sleep 3
    done
}

notify_client_done() {
    local round="$1"
    local mode="$2"
    curl -s -X POST "$CONTROLLER_URL/client_done" \
        -H "Content-Type: application/json" \
        -d "{\"round\": $round, \"mode\": \"$mode\"}" > /dev/null
    log "[$round/$ROUNDS][$mode] 已通知服务器：客户端完成"
}

save_results() {
    local round="$1"
    local mode="$2"
    local prefix
    [ "$mode" = "baseline" ] && prefix="baseline_groups" || prefix="task_groups"
    local all_ok=true

    for suffix in results.json same_core_overlaps.json raw_samples.json; do
        src="closed_loop_results/${prefix}_${suffix}"
        dst="$JSON_DIR/round${round}_${mode}_${suffix}"
        if [ ! -f "$src" ]; then
            log "[WARN] [$round/$ROUNDS][$mode] 结果文件缺失: $src"
            all_ok=false
            continue
        fi
        if [ ! -s "$src" ]; then
            log "[WARN] [$round/$ROUNDS][$mode] 结果文件为空: $src"
            all_ok=false
            continue
        fi
        # 校验 JSON 格式
        if ! python3 -c "import json; json.load(open('$src'))" 2>/dev/null; then
            log "[WARN] [$round/$ROUNDS][$mode] 结果文件 JSON 格式无效: $src"
            all_ok=false
        fi
        cp "$src" "$dst"
        log "[$round/$ROUNDS][$mode] 已保存: $(basename $dst)"
    done

    if [ "$all_ok" = true ]; then
        log "[$round/$ROUNDS][$mode] 所有结果文件校验通过"
    fi
}

analyze_round() {
    local round="$1"
    local plots_dst="$RESULTS_DIR/round${round}_plots"

    log "[$round/$ROUNDS] ===== 开始可视化 ====="

    local cl_dir="closed_loop_results"

    # 将本轮 json 文件复制到 closed_loop_results/ 供 visualize.py 读取
    for mode in baseline experiment; do
        [ "$mode" = "baseline" ] && prefix="baseline_groups" || prefix="task_groups"
        for suffix in results.json same_core_overlaps.json raw_samples.json; do
            src="$JSON_DIR/round${round}_${mode}_${suffix}"
            dst="$cl_dir/${prefix}_${suffix}"
            [ -f "$src" ] && cp "$src" "$dst"
        done
    done

    # 可视化（33 张图：27 dual-view + 4 overview + 2 comparison）
    log "[$round/$ROUNDS] 运行 visualize.py..."
    $PYTHON visualize.py \
        && {
            if [ -d "$cl_dir/plots" ]; then
                rm -rf "$plots_dst"
                mv "$cl_dir/plots" "$plots_dst"
                log "[$round/$ROUNDS] 可视化图片已生成: $plots_dst"
            fi
        } \
        || log "[WARN] [$round/$ROUNDS] visualize.py 执行失败"
    
    # 清理临时 JSON 文件
    for mode in baseline experiment; do
        [ "$mode" = "baseline" ] && prefix="baseline_groups" || prefix="task_groups"
        for suffix in results.json same_core_overlaps.json raw_samples.json; do
            rm -f "$cl_dir/${prefix}_${suffix}"
        done
    done
    
    log "[$round/$ROUNDS] ===== 可视化完成 ====="
}

analyze_all_rounds() {
    log "===== 开始生成全轮汇总 CSV ====="
    local summary_dir="$RESULTS_DIR/summary_csvs"
    $PYTHON closed_loop_results/generate_summary_csvs.py \
        --json-dir "$JSON_DIR" \
        --rounds "$ROUNDS" \
        --out-dir "$summary_dir" \
        && log "汇总 CSV 已生成: $summary_dir" \
        || log "[WARN] generate_summary_csvs.py 执行失败"
    log "===== 全轮汇总完成 ====="
}

run_one_mode() {
    local round="$1"
    local mode="$2"
    local groups_file log_file

    [ "$mode" = "baseline" ] && groups_file="baseline_groups.json" || groups_file="task_groups.json"
    log_file="$CLIENT_LOG_DIR/round${round}_${mode}.log"

    log "[$round/$ROUNDS][$mode] ===== 开始 ====="

    # 1. 等待服务器就绪信号
    if ! wait_server_ready "$round" "$mode"; then
        # 即使超时也要通知服务端，避免服务端永远等待
        notify_client_done "$round" "$mode"
        return 1
    fi

    # 2. 执行 run.py
    log "[$round/$ROUNDS][$mode] 启动 run.py..."
    TASK_GROUPS_FILE="$groups_file" $PYTHON run.py > "$log_file" 2>&1
    EXIT_CODE=$?
    if [ $EXIT_CODE -ne 0 ]; then
        log "[ERROR] [$round/$ROUNDS][$mode] run.py 退出码=$EXIT_CODE，跳过结果保存"
        # 即使失败也必须通知服务端
        notify_client_done "$round" "$mode"
        return 1
    fi
    log "[$round/$ROUNDS][$mode] run.py 完成"

    # 3. 保存并校验结果
    save_results "$round" "$mode"

    # 4. 通知服务器完成
    notify_client_done "$round" "$mode"

    log "[$round/$ROUNDS][$mode] ===== 完成 ====="
}

# ── 主流程 ────────────────────────────────────────────────────────────────────

log "=============================="
log " 客户端自动化实验开始，共 $ROUNDS 轮"
log " 服务器地址: $CONTROLLER_URL"
log "=============================="

for round in $(seq 1 "$ROUNDS"); do
    log "====== 第 $round / $ROUNDS 轮 ======"

    run_one_mode "$round" "baseline" || log "[ERROR] 第 $round 轮 baseline 失败，继续下一模式"
    run_one_mode "$round" "experiment" || log "[ERROR] 第 $round 轮 experiment 失败，继续下一轮"

    # 每轮 baseline + experiment 都完成后，自动可视化
    analyze_round "$round"

done

# 所有轮次完成后，生成全轮汇总 CSV
analyze_all_rounds

log "=============================="
log " 全部 $ROUNDS 轮实验完成"
log " 结果目录: $RESULTS_DIR"
log " JSON 目录: $JSON_DIR"
log " 日志目录: $CLIENT_LOG_DIR"
log "=============================="
