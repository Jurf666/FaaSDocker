# utils/request_handler.py
import threading
import time
from collections import defaultdict

from config import CONTROLLER_URL, SIMPLE_ACTIONS

# Metrics
perf_data = defaultdict(list)  # success-only latency samples (seconds)
metric_data = defaultdict(lambda: defaultdict(list))  # grouped metric samples
request_counters = defaultdict(
    lambda: {
        "attempt": 0,
        "success": 0,
        "http_fail": 0,
        "logic_fail": 0,
        "timeout_fail": 0,
        "exception_fail": 0,
    }
)

# Per-invocation samples for same-core overlap analysis
# Time source is runner.run() internal exec+main interval.
execution_samples = []

data_lock = threading.Lock()


def _inc_counter(func_name, key, delta=1):
    with data_lock:
        request_counters[func_name][key] += delta


def _normalize_core_list(core_val):
    if core_val is None:
        return []
    if isinstance(core_val, (list, tuple, set)):
        raw = core_val
    else:
        raw = [core_val]

    out = []
    for item in raw:
        try:
            out.append(int(item))
        except (TypeError, ValueError):
            continue
    return out


def _to_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _ns_to_seconds(value):
    v = _to_int(value)
    if v is None:
        return None
    return v / 1_000_000_000.0


def _append_metric_locked(func_name, metric_name, value):
    numeric_value = _to_float(value)
    if numeric_value is None:
        return
    metric_data[func_name][metric_name].append(numeric_value)


def dispatch_simple(func_name, payload, request_id, is_workflow=False, cycle_time_s=None):
    """Dispatch one request and collect metrics."""
    import requests

    _inc_counter(func_name, "attempt")

    try:
        payload_to_send = payload.copy()
        payload_to_send["is_workflow"] = is_workflow
        resp = requests.post(
            f"{CONTROLLER_URL}/dispatch/{func_name}",
            json=payload_to_send,
            timeout=1200,
        )

        if resp.status_code != 200:
            _inc_counter(func_name, "http_fail")
            print(f"[ERROR] {request_id} failed: {resp.status_code}")
            return None

        data = resp.json()
        out = data.get("output", {})
        meta = out.get("__meta__", {}) if isinstance(out, dict) else {}
        duration = meta.get("duration")  # seconds, exec+main

        has_logic_error = isinstance(out, dict) and ("error" in out)
        if duration and duration > 0 and (not has_logic_error):
            start_ns = _to_int(meta.get("func_main_start_ns"))
            end_ns = _to_int(meta.get("func_main_end_ns"))
            duration_ns = _to_int(meta.get("func_duration_ns"))
            process_cpu_time = _to_float(meta.get("process_cpu_time", meta.get("func_cpu_time")))
            process_cpu_time_ns = _to_int(meta.get("process_cpu_time_ns", meta.get("func_cpu_time_ns")))
            container_cpu_time = _to_float(meta.get("container_cpu_time"))
            container_cpu_time_ns = _to_int(meta.get("container_cpu_time_ns"))
            effective_cpu_time = container_cpu_time if container_cpu_time is not None else process_cpu_time
            effective_cpu_time_ns = (
                container_cpu_time_ns
                if container_cpu_time_ns is not None
                else process_cpu_time_ns
            )
            cgroup_nr_periods_delta = _to_int(meta.get("cgroup_nr_periods_delta"))
            cgroup_nr_throttled_delta = _to_int(meta.get("cgroup_nr_throttled_delta"))
            cgroup_throttled_time_s = _to_float(meta.get("cgroup_throttled_time_seconds_delta"))
            cgroup_throttled_time_ns = _to_int(meta.get("cgroup_throttled_time_ns_delta"))
            cgroup_throttle_ratio = _to_float(meta.get("cgroup_throttle_ratio_delta"))

            with data_lock:
                perf_data[func_name].append(duration)
                _append_metric_locked(func_name, "effective_cpu_time_s", effective_cpu_time)
                _append_metric_locked(func_name, "container_cpu_time_s", container_cpu_time)
                _append_metric_locked(func_name, "process_cpu_time_s", process_cpu_time)
                _append_metric_locked(func_name, "cgroup_nr_periods", cgroup_nr_periods_delta)
                _append_metric_locked(func_name, "cgroup_nr_throttled", cgroup_nr_throttled_delta)
                _append_metric_locked(func_name, "cgroup_throttled_time_s", cgroup_throttled_time_s)
                _append_metric_locked(func_name, "cgroup_throttle_ratio", cgroup_throttle_ratio)
                request_counters[func_name]["success"] += 1

                execution_samples.append(
                    {
                        "client_request_id": request_id,
                        "server_request_id": meta.get("request_id"),
                        "function_name": func_name,
                        "container_id": meta.get("container_id"),
                        "cpuset": meta.get("cpuset"),
                        "physical_cores": _normalize_core_list(meta.get("physical_cores")),
                        "start_ns": start_ns,
                        "end_ns": end_ns,
                        "duration_ns": duration_ns,
                        "exec_wall_time_s": duration,
                        "exec_wall_time_ns": duration_ns,
                        "effective_cpu_time_s": effective_cpu_time,
                        "effective_cpu_time_ns": effective_cpu_time_ns,
                        "container_cpu_time_s": container_cpu_time,
                        "container_cpu_time_ns": container_cpu_time_ns,
                        "process_cpu_time_s": process_cpu_time,
                        "process_cpu_time_ns": process_cpu_time_ns,
                        "cycle_time_s": cycle_time_s,
                        "cgroup_nr_periods_delta": cgroup_nr_periods_delta,
                        "cgroup_nr_throttled_delta": cgroup_nr_throttled_delta,
                        "cgroup_throttled_time_s": cgroup_throttled_time_s,
                        "cgroup_throttled_time_ns": cgroup_throttled_time_ns,
                        "cgroup_throttle_ratio": cgroup_throttle_ratio,
                        # fallback fields for analyzer compatibility
                        "start_time": _ns_to_seconds(start_ns),
                        "end_time": _ns_to_seconds(end_ns),
                        "duration": duration,
                        # backward-compatible aliases
                        "cpu_time_s": effective_cpu_time,
                        "cpu_time_ns": effective_cpu_time_ns,
                    }
                )
            return out

        _inc_counter(func_name, "logic_fail")
        if has_logic_error:
            print(f"[ERROR] {request_id} logic error payload: {out.get('error')}")
        else:
            print(f"[ERROR] {request_id} invalid duration: {duration}")
        return None

    except requests.exceptions.Timeout as e:
        _inc_counter(func_name, "timeout_fail")
        print(f"[ERROR] {request_id} timeout: {e}")
        return None
    except Exception as e:
        _inc_counter(func_name, "exception_fail")
        print(f"[ERROR] {request_id} exception: {e}")
        return None


def client_worker(client_id, func_name, payload, end_time):
    """Worker loop for one client thread."""
    request_counter = 0
    is_workflow = func_name not in SIMPLE_ACTIONS
    previous_send_ns = None

    try:
        while time.monotonic_ns() < end_time:
            send_start_ns = time.monotonic_ns()
            cycle_time_s = None
            if previous_send_ns is not None:
                cycle_time_s = (send_start_ns - previous_send_ns) / 1_000_000_000.0
                with data_lock:
                    _append_metric_locked(func_name, "cycle_time_s", cycle_time_s)

            req_id = f"{func_name}-{client_id}-{request_counter}"
            result = dispatch_simple(
                func_name,
                payload.copy(),
                req_id,
                is_workflow,
                cycle_time_s=cycle_time_s,
            )
            status = "Completed" if result else "Failed"
            print(f"[CLIENT {client_id}] {status} {req_id}")
            request_counter += 1
            previous_send_ns = send_start_ns
    except Exception as e:
        print(f"[CLIENT {client_id}] Exception: {e}")


def get_perf_data():
    with data_lock:
        return {func: list(times) for func, times in perf_data.items()}


def get_request_counters():
    with data_lock:
        return {func: dict(counter) for func, counter in request_counters.items()}


def get_execution_samples():
    with data_lock:
        return [dict(item) for item in execution_samples]


def get_metric_data():
    with data_lock:
        return {
            func: {metric: list(values) for metric, values in metric_map.items()}
            for func, metric_map in metric_data.items()
        }


def clear_perf_data():
    with data_lock:
        perf_data.clear()
        metric_data.clear()
        request_counters.clear()
        execution_samples.clear()
