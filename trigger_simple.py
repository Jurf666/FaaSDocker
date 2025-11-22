import requests
import sys
import argparse
import json

# --- 全局配置 ---
CONTROLLER_URL = 'http://localhost:5000'
IMAGE_NAME = 'video-proxy:latest'  # 假设您的所有 Action 都在这个镜像里
PROXY_CONTAINER_PORT = 5000

# --- Action 默认参数配置 ---
# 用于定义如果不传参时的默认行为
ACTION_DEFAULTS = {
    "float_operation": {"param": 1000000},
    "matmul":          {"param": 5000},
    "linpack":         {"param": 5000},
    "k-means":         {"param": 1},       # 占位参数
    "image":           {},                 # 代码内定读取 test_image.png
    "network":         {"name": "5mb"},
    "markdown2html":   {},
    "map_reduce":      {},
    "disk":            {"bs": 1024, "count": 50000},
    "couchdb_test":    {},
    "noop": {}  # 默认参数为空
}

def setup_manager(action_name):
    """
    注册 Manager。
    因为文件都在镜像里，所以这里不需要传递 host_storage_path，
    也不需要挂载 /storage 卷。
    """
    print(f"[-] 正在注册 Manager: '{action_name}' ...")
    
    config = {
        "function_name": action_name,
        "image_name": IMAGE_NAME,
        "container_port": PROXY_CONTAINER_PORT,
        "min_idle_containers": 1
        # 不传 host_storage_path，完全依赖镜像内部文件
    }
    
    try:
        resp = requests.post(f"{CONTROLLER_URL}/create_manager", json=config)
        resp.raise_for_status()
        print(f"    > Manager 就绪。")
    except requests.RequestException as e:
        print(f"    > 错误: 注册 Manager 失败: {e}")
        sys.exit(1)

def trigger_action(action_name, payload):
    """
    发送触发请求并打印结果
    """
    print(f"[-] 正在触发 Action: '{action_name}'")
    print(f"    参数 (Payload): {payload}")
    
    try:
        resp = requests.post(
            f"{CONTROLLER_URL}/dispatch/{action_name}",
            json=payload
        )
        resp.raise_for_status()
        
        data = resp.json()
        result = data.get('result', {})
        container_id = data.get('container', 'unknown')
        
        print(f"\n[+] --- 执行成功 (Container: {container_id}) ---")
        
        # 尝试提取 Latency，如果不存在则打印完整结果
        if isinstance(result, dict) and 'latency' in result:
            print(f"    耗时 (Latency): {result['latency']} 秒")
        else:
            # 有些结果可能是长字符串或HTML，截断打印以免刷屏
            res_str = str(result)
            if len(res_str) > 200:
                print(f"    结果片段: {res_str[:200]} ...")
            else:
                print(f"    结果: {res_str}")
            
    except requests.RequestException as e:
        print(f"\n[!] --- 执行失败 ---")
        print(f"    错误: {e}")
        if e.response:
            print(f"    响应: {e.response.text}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="触发简单的 FaaS Action")
    
    # 1. 必需参数: Action 名称
    parser.add_argument("action", help="Action 名称 (例如: matmul, image, disk)", 
                        choices=ACTION_DEFAULTS.keys())
    
    # 2. 可选参数: 覆盖 param
    parser.add_argument("--param", type=int, help="修改默认的数值参数 (例如: 5000)")
    
    # 3. 可选参数: 传递任意 JSON (针对 disk 等复杂参数)
    parser.add_argument("--json", type=str, help="传递 JSON 字符串参数覆盖 (例如: '{\"bs\":2048}')")

    args = parser.parse_args()
    action_name = args.action
    
    # 准备参数
    payload = ACTION_DEFAULTS[action_name].copy()
    
    if args.param:
        payload["param"] = args.param
    
    if args.json:
        try:
            extra = json.loads(args.json)
            payload.update(extra)
        except json.JSONDecodeError:
            print("错误: --json 参数格式不正确。")
            sys.exit(1)

    # 执行
    setup_manager(action_name)
    trigger_action(action_name, payload)