# utils/request_handler.py
import time
import threading
from collections import defaultdict

from config import CONTROLLER_URL, SIMPLE_ACTIONS

# ===== [修改标记-主因3] =====
# 原始代码（注释化）：
# perf_data = defaultdict(list)
# data_lock = threading.Lock()
# 修改后：
# 1) 继续保留 perf_data（仅成功请求时延）
# 2) 新增 request_counters（尝试/成功/失败分类）
# 3) 为 run.py 提供 failure_rate 与失败分类统计所需数据
perf_data = defaultdict(list)
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
data_lock = threading.Lock()


def _inc_counter(func_name, key, delta=1):
    """线程安全地增加计数器。"""
    with data_lock:
        request_counters[func_name][key] += delta


def dispatch_simple(func_name, payload, request_id, is_workflow=False):
    """发送请求并记录统计。"""
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
        duration = meta.get("duration")

        # ===== [修改标记-主因3] =====
        # 原始代码（注释化）：
        # if duration and duration > 0 and 'error' not in out:
        #     perf_data[func_name].append(duration)
        # return out
        # 修改后：
        # - 成功：记录 perf_data 并 success +1
        # - 逻辑失败：HTTP=200 但返回 error 或 duration 无效
        has_logic_error = isinstance(out, dict) and ("error" in out)
        if duration and duration > 0 and (not has_logic_error):
            with data_lock:
                perf_data[func_name].append(duration)
                request_counters[func_name]["success"] += 1
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
    """单个客户端线程循环。"""
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
    """返回成功请求时延样本。"""
    with data_lock:
        return {func: list(times) for func, times in perf_data.items()}


def get_request_counters():
    """返回请求计数器快照。"""
    with data_lock:
        return {func: dict(counter) for func, counter in request_counters.items()}


def clear_perf_data():
    """清空时延样本与计数器。"""
    # ===== [修改标记-主因2 + 主因3] =====
    # 原始代码（注释化）：
    # with data_lock:
    #     perf_data.clear()
    # 修改后：同时清空 perf_data + request_counters
    # 目的：保证 warmup 后进入正式压测时统计窗口是干净的
    with data_lock:
        perf_data.clear()
        request_counters.clear()
