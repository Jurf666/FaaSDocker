import requests
import time
import json

import os

# Controller 地址可通过环境变量 `CONTROLLER_HOST`/`CONTROLLER_PORT` 覆盖
controller_host = os.environ.get('CONTROLLER_HOST', 'localhost')
controller_port = os.environ.get('CONTROLLER_PORT', '5000')
CONTROLLER_URL = f"http://{controller_host}:{controller_port}"
REPEAT_TIMES = 1  # 每个任务重复 n 次

# 定义重负载参数 (根据之前的讨论优化)
# 简单 Action 使用 /dispatch/<name>
SIMPLE_ACTIONS = {
    "float_operation": {"param": 5000000},#可调整
    "matmul":          {"param": 1000},#可调整
    "linpack":         {"param": 1000},#可调整
    "k-means":         {},
    "image":           {},# 代码内定读取文件夹下的test_image.png，可更换图片文件
    "network":         {"name": "10mb"},#上传文件夹下的指定文件到服务器，可调整
    "markdown2html":   {},# 代码内定读取文件夹下的example.md，理论可调整但是不好调整
    "map_reduce":      {},#代码内定读取文件夹下的data.txt，理论可调整但是不好调整
    "disk":            {"bs": "1M", "count": 1000},#可调整
    "couchdb_test":    {},
}

# 工作流使用 /dispatch_workflow (payload 放在 body 中)
WORKFLOWS = [
    "video", 
    "recognizer", 
    "svd", 
    "wordcount"
]

def run_simple(name, payload):
    print(f"  [Simple] Running {name} ...", end="", flush=True)
    try:
        resp = requests.post(f"{CONTROLLER_URL}/dispatch/{name}", json=payload, timeout=1200)
        if resp.status_code == 200:
            print(" Done.")
        else:
            print(f" Failed ({resp.status_code}): {resp.text}")
    except Exception as e:
        print(f" Error: {e}")

def run_workflow(name, payload={}):
    print(f"  [Workflow] Running {name} ...", end="", flush=True)
    try:
        # 1. 发送请求，拿到 Task ID
        resp = requests.post(
            f"{CONTROLLER_URL}/dispatch_workflow", 
            json={"workflow_name": name, "payload": payload}
        )
        
        if resp.status_code == 202:
            data = resp.json()
            task_id = data.get("task_id")
            print(f" (ID: {task_id}) Waiting ...", end="", flush=True)
            
            # 2. 轮询 (Polling) 直到结束
            while True:
                # 每 2 秒问一次
                time.sleep(2)
                
                check_resp = requests.get(f"{CONTROLLER_URL}/check_task/{task_id}")
                status = check_resp.json().get("status")
                
                if status == "completed":
                    print(" Done.")
                    break
                elif status == "failed":
                    print(" Failed (Server Error).")
                    break
                # 如果是 "running" 或 "unknown"，继续循环等待
        else:
            print(f" Failed to start ({resp.status_code}): {resp.text}")
            
    except Exception as e:
        print(f" Error: {e}")

def main():
    print(f"=== Starting Experiment (Repeat {REPEAT_TIMES}) ===")
    print("!!! Ensure Controller is running with timeout=1200 !!!")
    
    for i in range(1, REPEAT_TIMES + 1):
        print(f"\n--- Round {i}/{REPEAT_TIMES} ---")
        
        # 1. 运行所有简单 Action
        for name, payload in SIMPLE_ACTIONS.items():
            run_simple(name, payload)
            time.sleep(2) # 短暂冷却，确保资源释放
        '''
        # 2. 运行所有 Workflow
        for name in WORKFLOWS:
            run_workflow(name)
            time.sleep(2)'''

    print("\n=== Experiment Finished ===")

if __name__ == "__main__":
    main()