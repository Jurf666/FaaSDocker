# run_experiment_closed_loop.py
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from config import *
from utils.cgroup_manager import build_func_to_group_mapping, generate_cgroups_from_task_groups
from utils.connections import (
    init_controller_managers,
    init_couchdb_client,
    init_redis_client,
    wait_for_warmup,
)
from utils.core_overlap_analyzer import analyze_same_core_overlaps
from utils.metrics_calculator import compute_stability, generate_experiment_summary
from utils.request_handler import (
    clear_perf_data,
    client_worker,
    get_execution_samples,
    get_perf_data,
    get_request_counters,
)
from utils.workflow_utils import cleanup_workflow_data, prepare_workflow_caches


def main():
    print("=== Closed-Loop Performance Test ===")
    print(f"Duration: {TEST_DURATION}s | NUMA: {NUMA_NODE} | Seed: {RANDOM_SEED}")
    print("Same-core monitor: ON (for both baseline and experiment)")

    # 1) Build cgroup configs and function mapping
    cgroup_configs = generate_cgroups_from_task_groups(TASK_GROUPS_FILE, REFERENCE_GROUPS_FILE)
    func_to_group = build_func_to_group_mapping(cgroup_configs)

    # 2) Init controller managers and warmup
    init_controller_managers(cgroup_configs, func_to_group)
    wait_for_warmup(func_to_group)

    # 3) Init storage clients and workflow caches
    redis_client = init_redis_client()
    couchdb_client = init_couchdb_client()
    workflow_caches = prepare_workflow_caches(redis_client, couchdb_client)

    # Clear warmup samples before real test
    clear_perf_data()

    # 4) Build client configs
    client_configs = []
    mapped_funcs = set(func_to_group.keys())
    for func_name in sorted(mapped_funcs):
        payload = SIMPLE_ACTIONS.get(func_name, workflow_caches.get(func_name, {}))
        for _ in range(CLIENTS_PER_FUNCTION):
            client_configs.append((func_name, payload.copy()))

    # 5) Run load test
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

    # 6) Aggregate base metrics
    total_time = time.time() - start_time
    perf_data = get_perf_data()
    request_counters = get_request_counters()
    execution_samples = get_execution_samples()
    summary = generate_experiment_summary(perf_data, total_time)

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
        stat = compute_stability(times) or {
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

    core_overlap_report = analyze_same_core_overlaps(execution_samples)
    summary["core_overlap_samples"] = {
        "enabled": True,
        "total_samples": core_overlap_report["total_samples"],
        "analyzable_samples": core_overlap_report["analyzable_samples"],
        "skipped_samples": core_overlap_report["skipped_samples"],
    }

    print("\n=== Performance Statistics ===")
    for func, stat in stats.items():
        print(f"\n[{func}]")
        print(f"  Count: {stat['count']} | Mean: {stat['mean']:.6f}s | CV: {stat['cv']:.4f}")
        print(
            "  Attempts: {attempts} | Success: {success} | Failed: {failed} | "
            "FailureRate: {failure_rate:.2%}".format(**stat)
        )

    os.makedirs("closed_loop_results", exist_ok=True)
    result_prefix = os.path.splitext(TASK_GROUPS_FILE)[0]

    output = {
        "config": {
            "test_duration": TEST_DURATION,
            "num_clients": len(client_configs),
            "numa_node": NUMA_NODE,
            "task_groups_file": TASK_GROUPS_FILE,
            "same_core_monitor_enabled": True,
        },
        "summary": summary,
        "statistics": stats,
        "same_core_function_summary": core_overlap_report["function_level_summary"],
        "same_core_core_summary": core_overlap_report["core_level_summary"],
    }

    output_file = f"closed_loop_results/{result_prefix}_results.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print(f"\n[INFO] Results saved to {output_file}")

    overlap_detail_file = f"closed_loop_results/{result_prefix}_same_core_overlaps.json"
    with open(overlap_detail_file, "w", encoding="utf-8") as f:
        json.dump(core_overlap_report, f, indent=2, ensure_ascii=False)
    print(f"[INFO] Same-core per-invocation details saved to {overlap_detail_file}")

    # 7) Cleanup workflow temporary data
    cleanup_workflow_data(redis_client, couchdb_client)


if __name__ == "__main__":
    main()
