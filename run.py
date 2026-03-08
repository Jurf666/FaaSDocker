# run_experiment_closed_loop.py
import time
import json
import os
import random
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

from config import *
from utils.connections import (
    init_redis_client,
    init_couchdb_client,
    init_controller_managers,
    wait_for_warmup,
)
from utils.cgroup_manager import (
    generate_cgroups_from_task_groups,
    build_func_to_group_mapping,
)
from utils.workflow_utils import prepare_workflow_caches, cleanup_workflow_data

# ===== [修改标记-主因2 + 主因3] =====
# 原始代码（注释化）：
# from utils.request_handler import client_worker, get_perf_data
# 修改后：
# 1) clear_perf_data：正式压测前清空 warmup 残留样本
# 2) get_request_counters：拿到 attempt/success/fail 分类计数
from utils.request_handler import (
    client_worker,
    get_perf_data,
    clear_perf_data,
    get_request_counters,
)
from utils.metrics_calculator import compute_stability, generate_experiment_summary


def main():
    print("=== Closed-Loop Performance Test ===")
    print(f"Duration: {TEST_DURATION}s | NUMA: {NUMA_NODE} | Seed: {RANDOM_SEED}")

    # 1. 初始化 cgroup 配置和函数映射
    cgroup_configs = generate_cgroups_from_task_groups(TASK_GROUPS_FILE, REFERENCE_GROUPS_FILE)
    func_to_group = build_func_to_group_mapping(cgroup_configs)

    # 2. 初始化 Controller 的 FunctionManager，并等待容器 warmup
    init_controller_managers(cgroup_configs, func_to_group)
    wait_for_warmup(func_to_group)

    # 3. 初始化存储连接并准备工作流缓存
    redis_client = init_redis_client()
    couchdb_client = init_couchdb_client()
    workflow_caches = prepare_workflow_caches(redis_client, couchdb_client)

    # ===== [修改标记-主因2] =====
    # 原始代码（注释化）：
    # （无）
    # 修改后：warmup 完成后、正式请求前，清空统计窗口。
    # 目的：避免 warmup 请求样本污染正式压测结果。
    clear_perf_data()

    # 4. 生成客户端配置
    client_configs = []
    mapped_funcs = set(func_to_group.keys())
    for func_name in sorted(mapped_funcs):
        payload = SIMPLE_ACTIONS.get(func_name, workflow_caches.get(func_name, {}))
        for _ in range(CLIENTS_PER_FUNCTION):
            client_configs.append((func_name, payload.copy()))

    # 5. 启动压测
    print(f"\n[INFO] Launching {len(client_configs)} clients...")
    start_time = time.time()
    end_deadline = start_time + TEST_DURATION

    try:
        requests.post(
            f"{CONTROLLER_URL}/start_monitor",
            json={"cgroup_configs": cgroup_configs},
            timeout=5,
        )
    except Exception as e:
        print(f"[WARN] Failed to start monitor: {e}")

    executor = ThreadPoolExecutor(max_workers=len(client_configs))
    client_futures = []
    for idx, (func_name, payload) in enumerate(client_configs):
        future = executor.submit(client_worker, idx, func_name, payload, end_deadline)
        client_futures.append(future)
    for future in as_completed(client_futures):
        future.result()
    executor.shutdown(wait=True)

    try:
        requests.post(f"{CONTROLLER_URL}/stop_monitor", timeout=5)
    except Exception as e:
        print(f"[WARN] Failed to stop monitor: {e}")

    # 6. 汇总实验数据
    total_time = time.time() - start_time
    perf_data = get_perf_data()
    request_counters = get_request_counters()
    summary = generate_experiment_summary(perf_data, total_time)

    # ===== [修改标记-主因3] =====
    # 原始代码（注释化）：
    # stats = {func: compute_stability(times) for func, times in perf_data.items()}
    # 修改后：
    # 1) 保留 success-only 的稳定性统计（mean/cv/p95 等）
    # 2) 叠加 attempts/success/failed/failure_rate
    # 3) 仅保留失败率与失败分类，不再引入失败惩罚均值
    total_attempts = 0
    total_success = 0
    total_http_fail = 0
    total_logic_fail = 0
    total_timeout_fail = 0
    total_exception_fail = 0

    all_funcs = sorted(set(perf_data.keys()) | set(request_counters.keys()))
    stats = {}
    for func in all_funcs:
        times = perf_data.get(func, [])
        stat = compute_stability(times)
        if not stat:
            stat = {
                "count": 0,
                "mean": 0.0,
                "variance": 0.0,
                "std": 0.0,
                "cv": 0.0,
                "min": 0.0,
                "max": 0.0,
                "iqr": 0.0,
                "p90": 0.0,
                "p95": 0.0,
            }

        counter = request_counters.get(func, {})
        attempts = int(counter.get("attempt", 0))
        success = int(counter.get("success", 0))
        http_fail = int(counter.get("http_fail", 0))
        logic_fail = int(counter.get("logic_fail", 0))
        timeout_fail = int(counter.get("timeout_fail", 0))
        exception_fail = int(counter.get("exception_fail", 0))
        failed = attempts - success
        failure_rate = (failed / attempts) if attempts > 0 else 0.0

        # ===== [修改标记-主因3-口径调整] =====
        # 原始代码（注释化）：
        # effective_mean = (
        #     (sum(times) + failed * EFFECTIVE_LATENCY_FAILURE_PENALTY) / attempts
        #     if attempts > 0
        #     else 0.0
        # )
        # 修改后：取消 effective_mean，均值/CV 只基于成功请求样本。

        stat.update(
            {
                "attempts": attempts,
                "success": success,
                "failed": failed,
                "failure_rate": failure_rate,
                "http_fail": http_fail,
                "logic_fail": logic_fail,
                "timeout_fail": timeout_fail,
                "exception_fail": exception_fail,
            }
        )
        stats[func] = stat

        total_attempts += attempts
        total_success += success
        total_http_fail += http_fail
        total_logic_fail += logic_fail
        total_timeout_fail += timeout_fail
        total_exception_fail += exception_fail

    total_failed = total_attempts - total_success
    summary.update(
        {
            "attempted_requests": total_attempts,
            "successful_requests": total_success,
            "failed_requests": total_failed,
            "failure_rate": (total_failed / total_attempts) if total_attempts > 0 else 0.0,
            "attempt_throughput": (total_attempts / total_time) if total_time > 0 else 0.0,
            "success_throughput": (total_success / total_time) if total_time > 0 else 0.0,
            "failed_breakdown": {
                "http_fail": total_http_fail,
                "logic_fail": total_logic_fail,
                "timeout_fail": total_timeout_fail,
                "exception_fail": total_exception_fail,
            },
        }
    )

    print("\n=== Performance Statistics ===")
    for func, stat in stats.items():
        print(f"\n[{func}]")
        print(f"  Count: {stat['count']} | Mean: {stat['mean']:.6f}s | CV: {stat['cv']:.4f}")
        print(
            "  Attempts: {attempts} | Success: {success} | Failed: {failed} | "
            "FailureRate: {failure_rate:.2%}".format(**stat)
        )

    output = {
        "config": {
            "test_duration": TEST_DURATION,
            "num_clients": len(client_configs),
            "numa_node": NUMA_NODE,
        },
        "summary": summary,
        "statistics": stats,
    }
    os.makedirs("closed_loop_results", exist_ok=True)
    output_file = f"closed_loop_results/{os.path.splitext(TASK_GROUPS_FILE)[0]}_results.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)
    print(f"\n[INFO] Results saved to {output_file}")

    # 7. 清理工作流临时数据
    cleanup_workflow_data(redis_client, couchdb_client)


if __name__ == "__main__":
    main()
