# utils/request_handler.py
import time
import threading
from collections import defaultdict
from config import CONTROLLER_URL, SIMPLE_ACTIONS

# 线程安全的性能数据存储
perf_data = defaultdict(list)
data_lock = threading.Lock()

def dispatch_simple(func_name, payload, request_id, is_workflow=False):
    """发送请求并记录性能数据"""
    import requests
    try:
        payload_to_send = payload.copy() 
        payload_to_send['is_workflow'] = is_workflow
        resp = requests.post(
            f"{CONTROLLER_URL}/dispatch/{func_name}",
            json=payload_to_send,
            timeout=1200
        )
        if resp.status_code != 200:
            print(f"[ERROR] {request_id} failed: {resp.status_code}")
            return None
        
        data = resp.json()
        out = data.get('output', {})
        meta = out.get('__meta__', {})
        duration = meta.get('duration')
        
        # 过滤无效数据
        if duration and duration > 0 and 'error' not in out:# 检查是否返回了错误信息 (Proxy 抛异常时通常返回 {"error": ...})
            with data_lock:
                perf_data[func_name].append(duration)
        
        return out
    except Exception as e:
        print(f"[ERROR] {request_id} exception: {e}")
        return None

def client_worker(client_id, func_name, payload, end_time):
    """客户端线程：持续发送请求直到超时"""
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
    """获取性能数据（对外暴露）"""
    return perf_data.copy()

def clear_perf_data():
    """清空性能数据"""
    with data_lock:
        perf_data.clear()