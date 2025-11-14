import requests
import os
import sys

# --- 配置 ---
CONTROLLER_URL = 'http://localhost:5000'
IMAGE_NAME = 'video-proxy:latest'
PROXY_CONTAINER_PORT = 5000
ACTION_NAME = "matmul"

# --- 1. 注册 Matmul Manager (此函数不变) ---
def setup_manager():
    """
    注册 matmul manager。
    **注意**：我们故意不传递 'host_storage_path'，
    来测试不需要共享存储 的 Action。
    """
    print(f"正在向 Controller 注册 '{ACTION_NAME}' Manager...")
    config = {
        "function_name": ACTION_NAME,
        "image_name": IMAGE_NAME,
        "container_port": PROXY_CONTAINER_PORT,
        "min_idle_containers": 1,
        # (host_storage_path 被省略了，因为这个 Action 不需要它)
    }
    try:
        resp = requests.post(f"{CONTROLLER_URL}/create_manager", json=config)
        resp.raise_for_status()
        print(f"  > Manager '{ACTION_NAME}' 已创建/已存在。")
    except requests.RequestException as e:
        if e.response and e.response.status_code != 200: # 忽略 "exists"
            print(f"  > 创建 manager 失败: {e}")
            sys.exit(1)

# --- 2. 触发 Action (此函数已修改) ---
def trigger_action():
    print(f"\n正在 {CONTROLLER_URL} 上直接调度 '{ACTION_NAME}'...")
    
    # --- ↓↓↓ 修改点 1 ↓↓↓ ---
    # 您的 main.py 期望的键是 "param"，而不是 "n"
    payload = {
        "param": 20000 
    }
    # --- ↑↑↑ 修改点 1 结束 ↑↑↑ ---
    
    try:
        resp = requests.post(
            f"{CONTROLLER_URL}/dispatch/{ACTION_NAME}",
            json=payload
        )
        resp.raise_for_status()
        
        print("\n--- 来自 Controller 的响应 ---")
        print(f"状态码: {resp.status_code}")
        print(f"响应体: {resp.json()}")
        
        # --- ↓↓↓ 修改点 2 ↓↓↓ ---
        # 您的 main.py 返回的键是 "latency"
        result = resp.json().get('result', {})
        if result and 'latency' in result:
            print(f"\n--- 成功! ---")
            print(f"Action '{ACTION_NAME}' 执行完毕。")
            print(f"矩阵大小: {payload['param']}x{payload['param']}")
            print(f"计算耗时 (latency): {result.get('latency'):.4f} 秒")
        # --- ↑↑↑ 修改点 2 结束 ↑↑↑ ---
        
    except requests.RequestException as e:
        print(f"\n--- 触发 Action 时出错 ---")
        print(f"错误: {e}")
        if e.response:
            print(f"响应体: {e.response.text}")

if __name__ == '__main__':
    setup_manager()
    trigger_action()