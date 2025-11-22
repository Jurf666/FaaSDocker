# controller.py
from flask import Flask, json, request, jsonify
import threading
from function_manager import FunctionManager 
import atexit
import time
import requests
from concurrent.futures import ThreadPoolExecutor
import subprocess
import os
import signal

app = Flask(__name__)

# 定义日志和存储路径
PERF_LOG_DIR = '/home/jywang/FaaSDocker/storage/perf_logs'
# 确保基础日志目录存在
os.makedirs(PERF_LOG_DIR, exist_ok=True)

function_managers = {}
manager_lock = threading.Lock()

# --- Perf 日志解析工具 ---
def parse_perf_log(log_path):
    """
    读取 perf 输出文件，返回一个包含关键指标的字典。
    增强版：能够正确处理带单位的指标（如 task-clock）和总结行（time elapsed）。
    """
    metrics = {}
    if not os.path.exists(log_path):
        print(f"[Parse] Warning: Log file not found: {log_path}")
        return metrics

    try:
        with open(log_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                
                # 移除逗号以便转换数字
                parts = line.replace(',', '').split()
                
                if len(parts) < 2:
                    continue

                # 提取数值
                try:
                    val = float(parts[0])
                except ValueError:
                    continue 

                # 提取键名 (Key)
                second_part = parts[1]
                
                if second_part in ['msec', 'ms', 'sec', 'seconds']:
                    # 情况 A: 带单位的指标 (e.g., "20537.19 msec task-clock")
                    if len(parts) >= 3:
                        key = parts[2]
                        # 特殊情况: "100.89 seconds time elapsed"
                        if key == 'time' and len(parts) >= 4 and parts[3] == 'elapsed':
                            metrics['seconds'] = val 
                        else:
                            metrics[key] = val
                else:
                    # 情况 B: 标准指标 (e.g., "16950758454 cycles")
                    key = parts[1]
                    metrics[key] = val
                    
    except Exception as e:
        print(f"[Parse] Error parsing {log_path}: {e}")
    
    return metrics

def calculate_clean_metrics(real_metrics, noise_metrics):
    """
    计算差值：Real - Noise，确保不为负数
    """
    clean = {}
    # 我们关心的核心指标
    keys_of_interest = [
        'cycles', 'instructions', 'task-clock', 'context-switches', 
        'cache-misses', 'L1-dcache-load-misses', 'LLC-load-misses', 
        'page-faults'
    ]
    
    for k in keys_of_interest:
        r_val = real_metrics.get(k, 0.0)
        n_val = noise_metrics.get(k, 0.0)
        clean[k] = max(0.0, r_val - n_val)
    
    # 计算 IPC (Instructions Per Cycle)
    if clean.get('cycles', 0) > 0:
        clean['IPC'] = clean['instructions'] / clean['cycles']
    else:
        clean['IPC'] = 0.0
        
    return clean


# --- 核心调度函数 (含 perf 采集) ---
# 修改点：增加了 custom_log_dir 参数
def _dispatch_request(function_name, payload, run_perf=True, custom_log_dir=None):
    """
    内部共享逻辑：为函数获取、初始化、运行(带perf)并释放一个容器。
    返回: (result_payload, container_id)
    """
    # print(f"[_dispatch_request] 正在为 '{function_name}' 寻找 manager...")
    with manager_lock:
        if function_name not in function_managers:
            raise Exception(f"未知的函数: {function_name}")
        manager = function_managers[function_name]

    # print(f"[_dispatch_request] 正在为 '{function_name}' 获取容器...")
    host_port, container_id = manager.get_container_for_request()
    if not host_port:
        print(f"[_dispatch_request] 错误: 无法获取容器 {function_name}")
        raise Exception(f"无法获取容器 {function_name}")

    perf_process = None
    output_file = ""
    perf_log_file = None
    pid = None

    try:
        # --- 1. 运行 INIT ---
        try:
            init_data = {"action": function_name}
            manager_url = f"http://127.0.0.1:{host_port}"
            requests.post(f"{manager_url}/init", json=init_data, timeout=10)
        except Exception as e:
            print(f"[_dispatch_request] init 错误 (非致命): {e}")


        # --- 2. 启动 PERF ---
        if run_perf:
            try:
                with manager.lock:
                    container_obj = manager.containers[container_id]["container_obj"]
                    container_obj.reload()
                    pid = container_obj.attrs['State']['Pid']
                
                if pid:
                    # 修改点：确定日志保存目录
                    # 如果传了 custom_log_dir 就用它，否则用默认的 PERF_LOG_DIR
                    log_target_dir = custom_log_dir if custom_log_dir else PERF_LOG_DIR
                    os.makedirs(log_target_dir, exist_ok=True)
                    
                    output_file = os.path.join(log_target_dir, f"{function_name}_{container_id[:12]}.txt")
                    
                    events = (
                        'cycles,instructions,task-clock,context-switches,'
                        'cache-misses,L1-dcache-load-misses,LLC-load-misses,'
                        'page-faults,major-faults,minor-faults'
                    )
                    
                    perf_cmd = [
                        'sudo', 'perf', 'stat',
                        '-p', str(pid),
                        '-e', events,
                        'sleep', '300' 
                    ]
                    
                    perf_log_file = open(output_file, 'w')
                    perf_process = subprocess.Popen(
                        perf_cmd, 
                        stdout=subprocess.PIPE, 
                        stderr=perf_log_file, 
                        preexec_fn=os.setsid 
                    )
                    
                    # 保留短暂 Sleep 防止 Race Condition
                    time.sleep(0.1)
                    
            except Exception as e:
                print(f"[_dispatch_request] 警告: 启动 perf 失败 (将继续执行): {e}")
                if perf_log_file:
                    perf_log_file.close()

        # --- 3. 运行 RUN ---
        r = requests.post(f"http://127.0.0.1:{host_port}/run", json=payload, timeout=600)
        r.raise_for_status()
        
        try:
            data = r.json()
        except Exception:
            data = {"raw": r.text}
        
        return data.get("result"), container_id
    
    except Exception as e:
        print(f"[_dispatch_request] 调用容器 {container_id[:12]} 时出错: {e}")
        try:
            print(f"--- 正在抓取容器 {container_id[:12]} 的日志 ---")
            with manager.lock:
                if container_id in manager.containers:
                    logs = manager.containers[container_id]["container_obj"].logs(tail=50).decode('utf-8', errors='ignore')
                    print(logs)
            print(f"--- 容器日志结束 ---")
        except Exception as log_e:
            pass
        raise e
        
    finally:
        # --- 4. 停止 PERF ---
        if run_perf and perf_process:
            try:
                os.killpg(os.getpgid(perf_process.pid), signal.SIGINT)
            except ProcessLookupError:
                pass 
            
            try:
                perf_process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                perf_process.kill()
            
            if perf_log_file:
                perf_log_file.close()
        
        # --- 5. 释放容器 ---
        manager.release_container(container_id)


# --- 自动去噪的调度逻辑 (Wrapper) ---
def dispatch_with_denoising(target_function, payload):
    """
    1. 运行 noop (带相同 payload) -> 获取 Noise Metrics
    2. 运行 target_function -> 获取 Real Metrics
    3. 计算 Clean Metrics 并保存
    """
    print(f"\n>>> [Auto-Denoise] Starting sequence for '{target_function}'...")

    # 修改点：定义该 Action 专属的日志文件夹
    action_log_dir = os.path.join(PERF_LOG_DIR, target_function)
    os.makedirs(action_log_dir, exist_ok=True)

    # --- 步骤 A: 运行基准 (Noop) ---
    if target_function == 'noop':
        # 如果直接调 noop，也存到 noop 文件夹下
        return _dispatch_request('noop', payload, custom_log_dir=action_log_dir)

    print(f">>> [Auto-Denoise] Phase 1: Running Baseline (noop)...")
    noise_metrics = {}
    try:
        # 自动检查并创建 noop Manager
        with manager_lock:
            if 'noop' not in function_managers:
                print(f">>> [Auto-Denoise] 'noop' manager not found. Creating it now...")
                function_managers['noop'] = FunctionManager(
                    function_name='noop',
                    image_name='video-proxy:latest', 
                    container_port=5000,
                    host_storage_path='/home/jywang/FaaSDocker/storage',
                    min_idle_containers=1 
                )
        
        # 修改点：传入 custom_log_dir
        _, noop_container_id = _dispatch_request('noop', payload, custom_log_dir=action_log_dir)
        
        # 修改点：从子文件夹读取
        noop_log_path = os.path.join(action_log_dir, f"noop_{noop_container_id[:12]}.txt")
        noise_metrics = parse_perf_log(noop_log_path)
        
    except Exception as e:
        print(f">>> [Auto-Denoise] Warning: Failed to run baseline (noop). Error: {e}")
        print(f">>> [Auto-Denoise] Proceeding without denoising...")

    # --- 步骤 B: 运行真实任务 ---
    print(f">>> [Auto-Denoise] Phase 2: Running Target ({target_function})...")
    # 修改点：传入 custom_log_dir
    result_data, container_id = _dispatch_request(target_function, payload, custom_log_dir=action_log_dir)
    
    # 修改点：从子文件夹读取
    real_log_path = os.path.join(action_log_dir, f"{target_function}_{container_id[:12]}.txt")
    real_metrics = parse_perf_log(real_log_path)

    # --- 步骤 C: 计算并保存 ---
    print(f">>> [Auto-Denoise] Phase 3: Calculating & Saving...")
    clean_metrics = calculate_clean_metrics(real_metrics, noise_metrics)
    
    # 修改点：保存到子文件夹
    clean_output_path = os.path.join(action_log_dir, f"clean_{target_function}_{container_id[:12]}.json")
    
    final_record = {
        "function": target_function,
        "timestamp": time.time(),
        "raw_metrics": real_metrics,
        "noise_baseline": noise_metrics,
        "clean_metrics": clean_metrics, 
        "result_payload": result_data 
    }
    
    with open(clean_output_path, 'w') as f:
        json.dump(final_record, f, indent=2)
        
    print(f">>> [Auto-Denoise] Success! Clean data saved to: {clean_output_path}")

    return result_data, container_id


# --- 接口: Create Manager ---
@app.route('/create_manager', methods=['POST'])
def create_manager():
    body = request.get_json(silent=True) or {}
    function_name = body.get("function_name")
    if not function_name:
        return jsonify({"error": "function_name required"}), 400

    with manager_lock:
        if function_name in function_managers:
            return jsonify({"status": "exists", "message": f"Manager {function_name} already exists."}), 200

        image_name = body.get("image_name", "video-proxy:latest") 
        container_port = int(body.get("container_port", 5000))
        host_storage_path = body.get("host_storage_path", None)
        host_port_start = int(body.get("host_port_start", 8000))
        idle_timeout = int(body.get("idle_timeout", 300))
        min_idle = int(body.get("min_idle_containers", 0))

        manager = FunctionManager(
            function_name=function_name,
            image_name=image_name,
            container_port=container_port,
            host_storage_path=host_storage_path,
            host_port_start=host_port_start,
            idle_timeout=idle_timeout,
            min_idle_containers=min_idle
        )
        function_managers[function_name] = manager
        return jsonify({"status": "created", "function": function_name}), 201


# --- 接口: Dispatch Single Request ---
@app.route('/dispatch/<function_name>', methods=['POST'])
def dispatch(function_name):
    payload = request.get_json(silent=True) or {}
    try:
        result_data, container_id = dispatch_with_denoising(function_name, payload)
        
        response_data = {
            "status": "success", 
            "result": result_data, 
            "container": container_id[:12]
        }
        return jsonify(response_data), 200

    except Exception as e:
        print(f"[dispatch_route] 调度时出错: {e}")
        data = {"status": "error", "message": str(e)}
        return jsonify(data), 502


# --- Workflows: Video ---
def _run_video_workflow(payload):
    print("[video_workflow] 视频工作流已启动...")
    try:
        video_name = payload.get("video_name")
        segment_time = payload.get("segment_time", 10)
        target_type = payload.get("target_type", "avi")
        output_prefix = payload.get("output_prefix", "final_video")

        # 1. Split
        print("[video_workflow] 正在调度 SPLIT...")
        split_payload = {"video_name": video_name, "segment_time": segment_time}
        split_result, _ = dispatch_with_denoising("video_split", split_payload)
        split_keys = split_result['split_keys']
        print(f"[video_workflow] SPLIT 完成。创建了 {len(split_keys)} 个分片。")

        # 2. Transcode (Parallel)
        print("[video_workflow] 正在调度 TRANSCODE (并行)...")
        def _transcode_task(split_file):
            task_payload = {'split_file': split_file, 'target_type': target_type}
            result, _ = dispatch_with_denoising("video_transcode", task_payload)
            return result['transcoded_file']

        with ThreadPoolExecutor(max_workers=len(split_keys)) as executor:
            transcoded_files = list(executor.map(_transcode_task, split_keys))
        print("[video_workflow] TRANSCODE 完成。")

        # 3. Merge
        print("[video_workflow] 正在调度 MERGE...")
        merge_payload = {
            'transcoded_files': transcoded_files,
            'target_type': target_type,
            'output_prefix': output_prefix,
            'video_name': video_name
        }
        merge_result, _ = dispatch_with_denoising("video_merge", merge_payload)
        final_video = merge_result['final_video']
        print(f"[video_workflow] 成功! 最终文件: {final_video}")

    except Exception as e:
        print(f"[video_workflow] 失败: {e}")

# --- Workflows: Recognizer ---
def _run_recognizer_workflow(payload):
    print("[recognizer_workflow] 图像审查工作流已启动...")
    try:
        image_filename = payload.get("image_filename")
        
        # 1. Upload
        upload_payload = {"image_filename": image_filename}
        upload_result, _ = dispatch_with_denoising("recognizer_upload", upload_payload)
        image_path = upload_result['image_path']

        # 2. Parallel Analysis
        print("[recognizer_workflow] 正在调度并行分析...")
        analysis_results = {}
        text_from_extract = ""

        with ThreadPoolExecutor(max_workers=3) as executor:
            future_adult = executor.submit(dispatch_with_denoising, "recognizer_adult", {"image_path": image_path})
            future_violence = executor.submit(dispatch_with_denoising, "recognizer_violence", {"image_path": image_path})
            future_extract = executor.submit(dispatch_with_denoising, "recognizer_extract", {"image_path": image_path})

            analysis_results["adult"] = future_adult.result()[0]
            analysis_results["violence"] = future_violence.result()[0]
            extract_result = future_extract.result()[0]
            analysis_results["extract"] = extract_result
            text_from_extract = extract_result.get("text", "")

        # 3. Parallel Text Analysis
        print("[recognizer_workflow] 正在调度并行文本分析...")
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_censor = executor.submit(dispatch_with_denoising, "recognizer_censor", {"text": text_from_extract})
            future_translate = executor.submit(dispatch_with_denoising, "recognizer_translate", {"text": text_from_extract})

            analysis_results["censor"] = future_censor.result()[0]
            analysis_results["translate"] = future_translate.result()[0]

        # 4. Decision
        is_illegal_adult = analysis_results["adult"].get("illegal", False)
        is_illegal_violence = analysis_results["violence"].get("illegal", False)
        is_illegal_censor = analysis_results["censor"].get("illegal", False)
        final_illegal_flag = is_illegal_adult or is_illegal_violence or is_illegal_censor
        
        final_image_path = image_path 

        # 5. Mosaic (if needed)
        if final_illegal_flag:
            print("[recognizer_workflow] 图像非法。调度 MOSAIC...")
            mosaic_result, _ = dispatch_with_denoising("recognizer_mosaic", {"image_path": image_path})
            final_image_path = mosaic_result.get("mosaic_image_path")
        
        print(f"[recognizer_workflow] 成功! 结果: {final_illegal_flag}, Path: {final_image_path}")

    except Exception as e:
        print(f"[recognizer_workflow] 失败: {e}")

# --- Workflows: SVD ---
def _run_svd_workflow(payload):
    print("[svd_workflow] SVD 工作流已启动...")
    try:
        row_num = payload.get("row_num", 2000)
        col_num = payload.get("col_num", 100)
        slice_num = payload.get("slice_num", 2)
        
        # 1. Start
        start_payload = {"row_num": row_num, "col_num": col_num, "slice_num": slice_num}
        start_result, _ = dispatch_with_denoising("svd_start", start_payload)
        slice_paths = start_result['slice_paths'] 

        # 2. Compute (Parallel)
        def _compute_task(task_input):
            mat_index, slice_path = task_input
            task_payload = {'slice_path': slice_path, 'mat_index': mat_index}
            result, _ = dispatch_with_denoising("svd_compute", task_payload)
            return result 

        compute_tasks = list(enumerate(slice_paths))
        with ThreadPoolExecutor(max_workers=len(compute_tasks)) as executor:
            compute_results = list(executor.map(_compute_task, compute_tasks))
        
        # 3. Merge
        merge_payload = {'results': compute_results}
        merge_result, _ = dispatch_with_denoising("svd_merge", merge_payload)
        print(f"[svd_workflow] 成功! SVD 结果已保存。")

    except Exception as e:
        print(f"[svd_workflow] 失败: {e}")

# --- Workflows: WordCount ---
def _run_wordcount_workflow(payload):
    print("[wordcount_workflow] WordCount 工作流已启动...")
    try:
        input_filename = payload.get("input_filename")
        slice_num = payload.get("slice_num", 4)
        
        # 1. Start
        start_payload = {"input_filename": input_filename, "slice_num": slice_num}
        start_result, _ = dispatch_with_denoising("wordcount_start", start_payload)
        chunk_paths = start_result['chunk_paths']

        # 2. Count (Parallel)
        def _count_task(chunk_path):
            task_payload = {'chunk_path': chunk_path}
            result, _ = dispatch_with_denoising("wordcount_count", task_payload)
            return result['result_path']

        with ThreadPoolExecutor(max_workers=len(chunk_paths)) as executor:
            count_results_paths = list(executor.map(_count_task, chunk_paths))
        
        # 3. Merge
        merge_payload = {'result_paths': count_results_paths}
        merge_result, _ = dispatch_with_denoising("wordcount_merge", merge_payload)
        final_word_count = merge_result['final_word_count']
        
        top_10 = sorted(final_word_count.items(), key=lambda item: item[1], reverse=True)[:10]
        print(f"[wordcount_workflow] 成功! 单词总数: {len(final_word_count)}, Top10: {top_10}")

    except Exception as e:
        print(f"[wordcount_workflow] 失败: {e}")


# --- 接口: Dispatch Workflow ---
@app.route('/dispatch_workflow', methods=['POST'])
def dispatch_workflow():
    body = request.get_json(silent=True) or {}
    workflow_name = body.get("workflow_name")
    payload = body.get("payload", {})

    if not workflow_name:
        return jsonify({"error": "workflow_name required"}), 400

    target_func = None
    if workflow_name == "video":
        target_func = _run_video_workflow
    elif workflow_name == "recognizer":
        target_func = _run_recognizer_workflow
    elif workflow_name == "svd":
        target_func = _run_svd_workflow
    elif workflow_name == "wordcount":
        target_func = _run_wordcount_workflow
    
    if target_func:
        thread = threading.Thread(target=target_func, args=(payload,))
        thread.daemon = True
        thread.start()
        return jsonify({"status": "started", "workflow": workflow_name}), 202
    else:
        return jsonify({"error": f"Unknown workflow: {workflow_name}"}), 404


@app.route('/manager_status/<function_name>', methods=['GET'])
def manager_status(function_name):
    with manager_lock:
        if function_name not in function_managers:
            return jsonify({"error": "unknown function"}), 404
        m = function_managers[function_name]

    with m.lock:
        total = len(m.containers)
        idle = sum(1 for d in m.containers.values() if d["status"] == "idle")
        busy = sum(1 for d in m.containers.values() if d["status"] == "busy")
        ports = [ {"id": cid[:12], "host_port": d.get("host_port")} for cid,d in m.containers.items() ]
    return jsonify({"function": function_name, "total": total, "idle": idle, "busy": busy, "containers": ports})


def clean_up_all_containers_on_exit():
    print("Application exiting. Stopping all function containers...")
    with manager_lock:
        for manager in function_managers.values():
            try:
                manager.stop_all_containers()
            except Exception as e:
                print("Error cleaning manager:", e)
    print("All containers stopped on exit.")

atexit.register(clean_up_all_containers_on_exit)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, threaded=True)