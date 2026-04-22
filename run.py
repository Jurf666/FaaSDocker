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
    get_metric_data,
    get_perf_data,
    get_request_counters,
)
from utils.workflow_utils import cleanup_workflow_data, prepare_workflow_caches


METRIC_LABELS = {
    "effective_cpu_time_s": "effective exec+main cpu",
    "container_cpu_time_s": "container/cgroup cpu",
    "process_cpu_time_s": "proxy process cpu",
    "cycle_time_s": "closed-loop cycle",
    "cgroup_nr_periods": "cgroup nr_periods",
    "cgroup_nr_throttled": "cgroup nr_throttled",
    "cgroup_throttled_time_s": "cgroup throttled time",
    "cgroup_throttle_ratio": "cgroup throttle ratio",
}

CPU_METRIC_NOTES = {
    "effective_cpu_time_s_mean": "Preferred CPU metric: container/cgroup CPU delta if available, otherwise proxy process CPU delta.",
    "container_cpu_time_s_mean": "CPU usage delta of the whole container/cgroup during exec+main; includes child processes.",
    "process_cpu_time_s_mean": "CPU time delta of the proxy Python process only; may undercount subprocess-heavy actions.",
}

METRIC_SCHEMA = {
    "statistics": {
        "mean": {
            "meaning": "Success-only exec+main wall-clock mean.",
            "unit": "seconds",
            "success_only": True,
        },
        "std": {
            "meaning": "Success-only exec+main wall-clock standard deviation.",
            "unit": "seconds",
            "success_only": True,
        },
        "cv": {
            "meaning": "Success-only exec+main wall-clock coefficient of variation.",
            "unit": "ratio",
            "success_only": True,
        },
        "p90": {
            "meaning": "Success-only exec+main wall-clock p90.",
            "unit": "seconds",
            "success_only": True,
        },
        "p95": {
            "meaning": "Success-only exec+main wall-clock p95.",
            "unit": "seconds",
            "success_only": True,
        },
        "p99": {
            "meaning": "Success-only exec+main wall-clock p99.",
            "unit": "seconds",
            "success_only": True,
        },
        "effective_cpu_time_s_mean": {
            "meaning": "Preferred CPU mean: container/cgroup CPU delta if available, otherwise proxy process CPU delta.",
            "unit": "seconds",
            "success_only": True,
        },
        "container_cpu_time_s_mean": {
            "meaning": "Whole-container/cgroup CPU usage mean during exec+main; includes child processes.",
            "unit": "seconds",
            "success_only": True,
        },
        "process_cpu_time_s_mean": {
            "meaning": "Proxy-process CPU usage mean during exec+main; may undercount subprocess-heavy actions.",
            "unit": "seconds",
            "success_only": True,
        },
        "cycle_time_s_mean": {
            "meaning": "Mean interval between consecutive request-send timestamps from the same client worker.",
            "unit": "seconds",
            "success_only": False,
        },
        "cgroup_nr_periods_mean": {
            "meaning": "Mean cgroup CPU periods delta observed during exec+main.",
            "unit": "count",
            "success_only": True,
        },
        "cgroup_nr_throttled_mean": {
            "meaning": "Mean throttled periods delta observed during exec+main.",
            "unit": "count",
            "success_only": True,
        },
        "cgroup_throttled_time_s_mean": {
            "meaning": "Mean cgroup throttled time delta during exec+main.",
            "unit": "seconds",
            "success_only": True,
        },
        "cgroup_throttle_ratio_mean": {
            "meaning": "Mean ratio of throttled periods to total periods during exec+main.",
            "unit": "ratio",
            "success_only": True,
        },
    },
    "execution_samples": {
        "exec_wall_time_s": {
            "meaning": "Per-invocation exec+main wall-clock time.",
            "unit": "seconds",
            "success_only": True,
        },
        "exec_wall_time_ns": {
            "meaning": "Per-invocation exec+main wall-clock time.",
            "unit": "nanoseconds",
            "success_only": True,
        },
        "effective_cpu_time_s": {
            "meaning": "Per-invocation preferred CPU time: container/cgroup CPU delta if available, otherwise proxy process CPU delta.",
            "unit": "seconds",
            "success_only": True,
        },
        "container_cpu_time_s": {
            "meaning": "Per-invocation whole-container/cgroup CPU time; includes child processes.",
            "unit": "seconds",
            "success_only": True,
        },
        "process_cpu_time_s": {
            "meaning": "Per-invocation proxy-process CPU time only.",
            "unit": "seconds",
            "success_only": True,
        },
        "cycle_time_s": {
            "meaning": "Closed-loop interval between consecutive request sends from the same client worker.",
            "unit": "seconds",
            "success_only": False,
        },
        "cgroup_throttled_time_s": {
            "meaning": "Per-invocation cgroup throttled time delta during exec+main.",
            "unit": "seconds",
            "success_only": True,
        },
        "cgroup_nr_throttled_delta": {
            "meaning": "Per-invocation cgroup throttled periods delta during exec+main.",
            "unit": "count",
            "success_only": True,
        },
    },
    "counters": {
        "attempt": {"meaning": "Total client attempts.", "unit": "count"},
        "success": {"meaning": "Successful requests with valid exec+main timing.", "unit": "count"},
        "http_fail": {"meaning": "Requests failed with non-200 HTTP response.", "unit": "count"},
        "logic_fail": {"meaning": "Requests returned 200 but had logical error payload or invalid timing.", "unit": "count"},
        "timeout_fail": {"meaning": "Requests failed due to client timeout.", "unit": "count"},
        "exception_fail": {"meaning": "Requests failed due to client-side exception.", "unit": "count"},
    },
}


def _empty_stability():
    return {
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
        "p99": 0.0,
    }


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

    # 3) Init storage clients; clean up stale workflow data before warming up
    redis_client = init_redis_client()
    couchdb_client = init_couchdb_client()
    cleanup_workflow_data(redis_client, couchdb_client)
    workflow_caches = prepare_workflow_caches(redis_client, couchdb_client)

    # Clear warmup samples before real test
    clear_perf_data()

    # Wait for CPU frequency to stabilize on the server before starting timed measurement.
    # Polls /freq_stable (which samples cur_freq twice with 1s gap); falls back to
    # FREQ_STABILIZE_SECS fixed sleep if the endpoint is unavailable or times out.
    _freq_wait_start = time.monotonic()
    _freq_stable_timeout = max(FREQ_STABILIZE_SECS * 3, 120)
    _freq_stable = False
    print(f"[INFO] Waiting for CPU frequency to stabilize (timeout={_freq_stable_timeout}s)...")
    while time.monotonic() - _freq_wait_start < _freq_stable_timeout:
        try:
            _resp = requests.get(f"{CONTROLLER_URL}/freq_stable", timeout=5)
            _data = _resp.json()
            if _data.get("stable"):
                _elapsed = time.monotonic() - _freq_wait_start
                print(
                    f"[INFO] CPU frequency stable after {_elapsed:.1f}s "
                    f"({_data.get('avg_cur_mhz')} MHz / {_data.get('avg_max_mhz')} MHz, "
                    f"ratio={_data.get('ratio')}, delta={_data.get('max_delta_mhz')} MHz)"
                )
                _freq_stable = True
                break
            else:
                print(
                    f"[INFO] Freq not yet stable: {_data.get('avg_cur_mhz')} MHz, "
                    f"ratio={_data.get('ratio')}, delta={_data.get('max_delta_mhz')} MHz"
                )
        except Exception as _e:
            print(f"[WARN] /freq_stable unavailable ({_e}), falling back to fixed sleep")
            time.sleep(FREQ_STABILIZE_SECS)
            _freq_stable = True
            break
        time.sleep(2)
    if not _freq_stable:
        print(f"[WARN] Frequency did not stabilize within {_freq_stable_timeout}s, proceeding anyway")

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
    end_deadline = time.monotonic_ns() + TEST_DURATION * 1_000_000_000

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
    metric_data = get_metric_data()
    request_counters = get_request_counters()
    execution_samples = get_execution_samples()
    summary = generate_experiment_summary(perf_data, total_time)

    total_attempts = 0
    total_success = 0
    total_http_fail = 0
    total_logic_fail = 0
    total_timeout_fail = 0
    total_exception_fail = 0

    all_funcs = sorted(
        set(perf_data.keys()) | set(metric_data.keys()) | set(request_counters.keys())
    )
    stats = {}
    for func in all_funcs:
        times = perf_data.get(func, [])
        stat = compute_stability(times) or _empty_stability()
        metric_stats = {}
        metric_means = {}
        for metric_name in METRIC_LABELS:
            metric_values = metric_data.get(func, {}).get(metric_name, [])
            metric_stat = compute_stability(metric_values) or _empty_stability()
            metric_stats[metric_name] = metric_stat
            metric_means[f"{metric_name}_mean"] = metric_stat["mean"]

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
                **metric_means,
                "exec_wall_time_stats": {k: stat[k] for k in _empty_stability().keys()},
                "effective_cpu_time_stats": metric_stats["effective_cpu_time_s"],
                "container_cpu_time_stats": metric_stats["container_cpu_time_s"],
                "process_cpu_time_stats": metric_stats["process_cpu_time_s"],
                "cycle_time_stats": metric_stats["cycle_time_s"],
                "metric_means": metric_means,
                "metric_stats": metric_stats,
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
    overall_metric_means = {}
    for metric_name in METRIC_LABELS:
        all_metric_values = []
        for func_metric_map in metric_data.values():
            all_metric_values.extend(func_metric_map.get(metric_name, []))
        overall_metric_means[metric_name] = (
            compute_stability(all_metric_values).get("mean", 0.0)
            if all_metric_values
            else 0.0
        )

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
            "overall_metric_means": overall_metric_means,
            "cpu_metric_notes": CPU_METRIC_NOTES,
            "metric_schema": METRIC_SCHEMA,
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
            "  exec+main CPU Mean: "
            f"{stat['metric_means']['effective_cpu_time_s_mean']:.6f}s | "
            "Container CPU Mean: "
            f"{stat['metric_means']['container_cpu_time_s_mean']:.6f}s | "
            "Process CPU Mean: "
            f"{stat['metric_means']['process_cpu_time_s_mean']:.6f}s"
        )
        print(
            "Closed-loop Cycle Mean: "
            f"{stat['metric_means']['cycle_time_s_mean']:.6f}s"
        )
        print(
            "  cgroup Throttled Mean: "
            f"{stat['metric_means']['cgroup_throttled_time_s_mean']:.6f}s | "
            "nr_throttled Mean: "
            f"{stat['metric_means']['cgroup_nr_throttled_mean']:.4f}"
        )
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
            "cpu_metric_notes": CPU_METRIC_NOTES,
            "metric_schema_version": 1,
        },
        "metric_schema": METRIC_SCHEMA,
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

    # 保存各函数的原始样本数组，供后续过滤分析和可视化使用
    raw_samples = {}
    for func in all_funcs:
        raw_samples[func] = {
            "exec_wall_time_s": perf_data.get(func, []),
        }
        for metric_name in METRIC_LABELS:
            raw_samples[func][metric_name] = metric_data.get(func, {}).get(metric_name, [])

    raw_samples_file = f"closed_loop_results/{result_prefix}_raw_samples.json"
    with open(raw_samples_file, "w", encoding="utf-8") as f:
        json.dump(raw_samples, f, indent=2, ensure_ascii=False)
    print(f"[INFO] Raw samples saved to {raw_samples_file}")

    # 7) Cleanup workflow temporary data
    cleanup_workflow_data(redis_client, couchdb_client)


if __name__ == "__main__":
    main()
