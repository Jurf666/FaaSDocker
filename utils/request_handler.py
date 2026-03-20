# utils/request_handler.py
import threading
import time
from collections import defaultdict

from config import CONTROLLER_URL, SIMPLE_ACTIONS

# Metrics
perf_data = defaultdict(list)  # success-only latency samples (seconds)
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


def _ns_to_seconds(value):
    v = _to_int(value)
    if v is None:
        return None
    return v / 1_000_000_000.0


def dispatch_simple(func_name, payload, request_id, is_workflow=False):
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

            with data_lock:
                perf_data[func_name].append(duration)
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
                        # fallback fields for analyzer compatibility
                        "start_time": _ns_to_seconds(start_ns),
                        "end_time": _ns_to_seconds(end_ns),
                        "duration": duration,
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

    try:
        while time.time() < end_time:
            req_id = f"{func_name}-{client_id}-{request_counter}"
            result = dispatch_simple(func_name, payload.copy(), req_id, is_workflow)
            status = "Completed" if result else "Failed"
            print(f"[CLIENT {client_id}] {status} {req_id}")
            request_counter += 1
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


def clear_perf_data():
    with data_lock:
        perf_data.clear()
        request_counters.clear()
        execution_samples.clear()
