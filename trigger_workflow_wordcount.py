import requests
import os
import sys

# --- 配置 ---
CONTROLLER_URL = 'http://localhost:5000'
HOST_STORAGE_PATH = '/home/jywang/FaaSDocker/storage'
IMAGE_NAME = 'video-proxy:latest'
PROXY_CONTAINER_PORT = 5000

# --- 1. 注册 WordCount 相关的 Function Managers ---
def setup_wordcount_managers():
    print("正在向 Controller 注册 WordCount Function Managers...")
    
    wordcount_functions = [
        {"function_name": "wordcount_start", "min_idle": 1},
        {"function_name": "wordcount_count", "min_idle": 2}, # 预热 2 个
        {"function_name": "wordcount_merge", "min_idle": 1}
    ]
    
    for func in wordcount_functions:
        config = {
            "function_name": func["function_name"],
            "image_name": IMAGE_NAME,
            "container_port": PROXY_CONTAINER_PORT,
            "min_idle_containers": func.get("min_idle", 0),
            "host_storage_path": HOST_STORAGE_PATH # 需要共享存储
        }
        try:
            resp = requests.post(f"{CONTROLLER_URL}/create_manager", json=config)
            resp.raise_for_status()
            print(f"  > Manager '{func['function_name']}' 已创建/已存在。")
        except requests.RequestException as e:
            if e.response and e.response.status_code != 200: # 忽略 200 (exists)
                print(f"  > 创建 manager '{func['function_name']}' 失败: {e}")
                sys.exit(1)

# --- 2. 准备输入文件和清理旧输出 ---
def prepare_files(input_filename):
    print("正在准备输入文件并清理存储目录...")
    
    # --- 准备输入文件 ---
    # (您需要自己准备一个测试用的文本文件)
    source_dir = os.path.join(os.path.expanduser('~'), 'FaaSDocker', 'sources')
    SOURCE_FILE_PATH = os.path.join(source_dir, input_filename)
    
    if not os.path.exists(SOURCE_FILE_PATH):
        print(f"!! 错误: 输入文件未在 {SOURCE_FILE_PATH} 找到！")
        print("!! 请在该位置放置一个测试用的 .txt 文件 (例如: book.txt)")
        # 自动创建一个假的测试文件，以防万一
        print("!! 正在创建一个临时的 'dummy_text.txt' 用于测试...")
        dummy_path = os.path.join(source_dir, 'dummy_text.txt')
        with open(dummy_path, 'w') as f:
            f.write("hello world hello faas world hello python")
        input_filename = 'dummy_text.txt' # 切换到使用这个文件
        
    # 确保 /storage/sources 目录存在
    storage_source_dir = os.path.join(HOST_STORAGE_PATH, 'sources')
    os.makedirs(storage_source_dir, exist_ok=True)
    
    # 复制源文件到 /storage/sources/
    os.system(f'cp {os.path.join(source_dir, input_filename)} {storage_source_dir}/')
    print(f"已将 '{input_filename}' 复制到共享工作区。")

    # --- 清理旧的输出 ---
    for subdir in ['wordcount_input', 'wordcount_output', 'wordcount_final']:
        full_path = os.path.join(HOST_STORAGE_PATH, subdir)
        os.system(f'rm -rf {full_path}')
        os.makedirs(full_path, exist_ok=True)
        
    return input_filename # 返回实际使用的文件名

# --- 3. 触发工作流 ---
def trigger_workflow(input_filename):
    print(f"\n正在 {CONTROLLER_URL} 上触发 'wordcount' 工作流...")
    
    workflow_payload = {
        "input_filename": input_filename,
        "slice_num": 4  # 将文本切分成 4 片
    }
    
    try:
        resp = requests.post(
            f"{CONTROLLER_URL}/dispatch_workflow",
            json={
                "workflow_name": "wordcount",
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
    # 在这里配置您想测试的 .txt 文件
    # 确保这个文件在 /home/jywang/FaaSDocker/sources/ 目录下
    INPUT_FILE = "book.txt" 
    
    setup_wordcount_managers()
    # prepare_files 会检查文件是否存在，如果不存在，它会创建一个
    # 虚拟文件 "dummy_text.txt" 并使用它。
    actual_file = prepare_files(INPUT_FILE)
    trigger_workflow(actual_file)