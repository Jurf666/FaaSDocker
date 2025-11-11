import requests
import os
import sys

# --- 配置 ---
CONTROLLER_URL = 'http://localhost:5000'
HOST_STORAGE_PATH = '/home/jywang/FaaSDocker/storage'
IMAGE_NAME = 'video-proxy:latest'
PROXY_CONTAINER_PORT = 5000

# --- 1. 注册 Recognizer 相关的 Function Managers ---
def setup_recognizer_managers():
    print("正在向 Controller 注册 Recognizer Function Managers...")
    
    recognizer_functions = [
        {"function_name": "recognizer_upload", "min_idle": 1},
        {"function_name": "recognizer_extract", "min_idle": 1},
        {"function_name": "recognizer_adult", "min_idle": 1},
        {"function_name": "recognizer_violence", "min_idle": 1},
        {"function_name": "recognizer_censor", "min_idle": 1},
        {"function_name": "recognizer_translate", "min_idle": 1},
        {"function_name": "recognizer_mosaic", "min_idle": 0}
    ]
    
    for func in recognizer_functions:
        config = {
            "function_name": func["function_name"],
            "image_name": IMAGE_NAME,
            "container_port": PROXY_CONTAINER_PORT,
            "min_idle_containers": func.get("min_idle", 0),
            "host_storage_path": HOST_STORAGE_PATH 
        }
        try:
            resp = requests.post(f"{CONTROLLER_URL}/create_manager", json=config)
            resp.raise_for_status()
            print(f"  > Manager '{func['function_name']}' 已创建/已存在。")
        except requests.RequestException as e:
            if e.response and e.response.status_code != 200: # 忽略 200 (exists)
                print(f"  > 创建 manager '{func['function_name']}' 失败: {e}")
                sys.exit(1) # 如果 manager 创建失败，则停止

# --- 2. 准备文件 (验证输入并清理输出) ---
def prepare_files(image_to_process):
    # 验证源文件是否存在
    source_dir = os.path.join(os.path.expanduser('~'), 'FaaSDocker', 'sources')
    SOURCE_IMAGE_PATH = os.path.join(source_dir, image_to_process)
    
    if not os.path.exists(SOURCE_IMAGE_PATH):
        print(f"错误: 源文件未在 {SOURCE_IMAGE_PATH} 找到！")
        sys.exit(1)

    # 确保 /storage/source 目录存在 (如果 upload action 需要)
    storage_source_dir = os.path.join(HOST_STORAGE_PATH, 'sources')
    os.makedirs(storage_source_dir, exist_ok=True)
    
    # 复制源文件到 /storage/source/，供 upload action 使用
    os.system(f'cp {SOURCE_IMAGE_PATH} {os.path.join(storage_source_dir, image_to_process)}')
    print(f"已将 '{image_to_process}' 复制到共享工作区。")

    # 清理上一次的输出目录
    mosaic_output_dir = os.path.join(HOST_STORAGE_PATH, 'mosaic_output')
    os.system(f'rm -rf {mosaic_output_dir}')
    os.makedirs(mosaic_output_dir, exist_ok=True)

# --- 3. 触发工作流 ---
def trigger_workflow(image_to_process):
    print(f"\n正在 {CONTROLLER_URL} 上触发 'recognizer' 工作流...")
    
    workflow_payload = {
        "image_filename": image_to_process
    }
    
    try:
        resp = requests.post(
            f"{CONTROLLER_URL}/dispatch_workflow",
            json={
                "workflow_name": "recognizer",
                "payload": workflow_payload
            }
        )
        resp.raise_for_status()
        
        print("\n--- 来自 Controller 的响应 ---")
        print(f"状态码: {resp.status_code}")
        print(f"响应体: {resp.json()}")
        
        if resp.status_code == 202:
            print("\n工作流已在后台启动。")
            print("请在运行 controller.py 的终端中查看实时日志！")
        
    except requests.RequestException as e:
        print(f"\n--- 触发工作流时出错 ---")
        print(f"错误: {e}")
        if e.response:
            print(f"响应体: {e.response.text}")

if __name__ == '__main__':
    # --- 在这里配置您想测试的图片 ---
    IMAGE_FILE_TO_TEST = "test.png"  # 确保这个文件在 FaaSDocker/source/ 中
    
    setup_recognizer_managers()
    prepare_files(IMAGE_FILE_TO_TEST)
    trigger_workflow(IMAGE_FILE_TO_TEST)