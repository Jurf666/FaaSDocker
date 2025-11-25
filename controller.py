import os
import time
import json
import uuid
import redis
import couchdb
import threading
import requests
import atexit
import logging
import subprocess
import signal
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, request, jsonify

# 复用您原有的 FunctionManager
from function_manager import FunctionManager 

# --- 日志配置 ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("Controller")

# --- 全局配置 ---
REDIS_HOST = os.environ.get('REDIS_HOST', '172.17.0.1')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
COUCHDB_URL = os.environ.get('COUCHDB_URL', 'http://openwhisk:openwhisk@172.17.0.1:5984/')
PERF_LOG_DIR = '/home/jywang/FaaSDocker/storage/perf_logs'

app = Flask(__name__)

# --- 全局状态 ---
function_managers = {}
manager_lock = threading.Lock()
redis_client = None
couch_db = None

# --- 数据库初始化 ---
try:
    redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    redis_client.ping()
    couch_server = couchdb.Server(COUCHDB_URL)
    if 'faas_data' not in couch_server:
        couch_server.create('faas_data')
    couch_db = couch_server['faas_data']
    logger.info("Controller connected to Redis & CouchDB (Workflow Ready).")
except Exception as e:
    logger.warning(f"DB Connection Failed: {e}. Workflows will fail, Simple Actions ok.")

# -------------------------------------------------------------------------
# Part 1: Perf 数据解析与去噪工具
# -------------------------------------------------------------------------

def parse_perf_log(log_path):
    """读取 perf 输出文件，返回指标字典"""
    metrics = {}
    if not os.path.exists(log_path):
        return metrics

    try:
        with open(log_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                
                parts = line.replace(',', '').split()
                if len(parts) < 2: continue

                try:
                    val = float(parts[0])
                except ValueError: continue 

                second_part = parts[1]
                if second_part in ['msec', 'ms', 'sec', 'seconds']:
                    if len(parts) >= 3:
                        key = parts[2]
                        if key == 'time' and len(parts) >= 4 and parts[3] == 'elapsed':
                            metrics['seconds'] = val 
                        else:
                            metrics[key] = val
                else:
                    key = parts[1]
                    metrics[key] = val
    except Exception as e:
        logger.error(f"[Perf] Error parsing {log_path}: {e}")
    return metrics

def calculate_clean_metrics(real_metrics, noise_metrics):
    """计算 Real - Noise"""
    clean = {}
    keys_of_interest = [
        'cycles', 'instructions', 'task-clock', 'context-switches', 
        'cache-misses', 'L1-dcache-load-misses', 'LLC-load-misses', 
        'page-faults'
    ]
    for k in keys_of_interest:
        r_val = real_metrics.get(k, 0.0)
        n_val = noise_metrics.get(k, 0.0)
        clean[k] = max(0.0, r_val - n_val)
    
    if clean.get('cycles', 0) > 0:
        clean['IPC'] = clean['instructions'] / clean['cycles']
    else:
        clean['IPC'] = 0.0
    return clean

# -------------------------------------------------------------------------
# Part 2: 核心执行逻辑 (支持 run_id 用于文件命名)
# -------------------------------------------------------------------------

def _dispatch_core(function_name, payload, is_workflow=False, custom_log_dir=None, run_id=None):
    """
    底层调度函数。
    新增参数 run_id: 用于生成唯一的 perf 日志文件名，防止覆盖。
    """
    request_id = f"req-{uuid.uuid4().hex[:8]}"
    
    # 1. 获取 Manager
    with manager_lock:
        if function_name not in function_managers:
            function_managers[function_name] = FunctionManager(
                function_name=function_name,
                image_name='jywang_test', 
                container_port=5000,
                host_storage_path=None, 
                min_idle_containers=1
            )
        manager = function_managers[function_name]

    container_id = None
    perf_process = None
    perf_log_file = None
    
    try:
        # 2. 获取容器
        host_port, container_id = manager.get_container_for_request()
        if not host_port:
            raise Exception(f"No container available for {function_name}")

        # 3. 构造 Payload
        if is_workflow:
            proxy_payload = {
                "request_id": request_id,
                "action": function_name,
                "input_mapping": payload 
            }
        else:
            proxy_payload = payload.copy() if isinstance(payload, dict) else {}
            proxy_payload['action'] = function_name

        # 4. Init
        try:
            requests.post(f"http://127.0.0.1:{host_port}/init", json={"action": function_name}, timeout=5)
        except requests.RequestException:
            pass 

        # 5. START PERF
        perf_output_filename = None
        if custom_log_dir:
            try:
                pid = 0
                with manager.lock:
                    if container_id in manager.containers:
                        container_obj = manager.containers[container_id]["container_obj"]
                        container_obj.reload()
                        pid = container_obj.attrs['State']['Pid']
                
                if pid:
                    os.makedirs(custom_log_dir, exist_ok=True)
                    
                    # --- 关键修改：文件名加入 run_id 前缀 ---
                    prefix = f"{run_id}_" if run_id else ""
                    perf_output_filename = os.path.join(custom_log_dir, f"{prefix}{function_name}_{container_id[:12]}.txt")
                    
                    events = 'cycles,instructions,task-clock,context-switches,cache-misses,L1-dcache-load-misses,LLC-load-misses,page-faults,major-faults,minor-faults'
                    perf_cmd = ['sudo', 'perf', 'stat', '-p', str(pid), '-e', events, 'sleep', '300']
                    
                    perf_log_file = open(perf_output_filename, 'w')
                    perf_process = subprocess.Popen(perf_cmd, stdout=subprocess.PIPE, stderr=perf_log_file, preexec_fn=os.setsid)
                    time.sleep(0.1) 
            except Exception as e:
                logger.warning(f"Perf start failed: {e}")
                if perf_log_file: perf_log_file.close()

        # 6. RUN
        start = time.time()
        # 调整了 timeout 为 600s 以避免 matmul 超时
        resp = requests.post(f"http://127.0.0.1:{host_port}/run", json=proxy_payload, timeout=600)
        
        if resp.status_code != 200:
            logger.error(f"Container Error ({resp.status_code}): {resp.text}")
        resp.raise_for_status()
        
        # 7. STOP PERF
        if perf_process:
            try: os.killpg(os.getpgid(perf_process.pid), signal.SIGINT)
            except: pass
            try: perf_process.communicate(timeout=2)
            except subprocess.TimeoutExpired: perf_process.kill()
            if perf_log_file: perf_log_file.close()

        # 8. 解析结果
        full_data = resp.json() 
        proxy_result = full_data.get("result", {})
        
        duration = time.time() - start
        mode_str = "WORKFLOW" if is_workflow else "SIMPLE"
        logger.info(f"[Run][{mode_str}] {function_name} finished in {duration:.2f}s")
        
        final_result = proxy_result.get("output_keys", {}) if is_workflow else proxy_result.get("func_result")
        
        # 返回结果、容器ID以及本次生成的日志路径（供去噪使用）
        return final_result, container_id, perf_output_filename

    except Exception as e:
        if perf_process:
            try: perf_process.kill()
            except: pass
        if perf_log_file: perf_log_file.close()
        
        logger.error(f"[Dispatch] Failed for {function_name}: {e}")
        raise e
    finally:
        if container_id:
            manager.release_container(container_id)

# -------------------------------------------------------------------------
# Part 3: 去噪调度器 Wrapper (生成 Run ID)
# -------------------------------------------------------------------------

def dispatch(function_name, payload, is_workflow=False):
    """
    智能调度器：自动执行去噪逻辑 (noop -> target)。
    每次调用生成唯一的 run_id (时间戳)，确保文件不覆盖。
    """
    action_log_dir = os.path.join(PERF_LOG_DIR, function_name)
    os.makedirs(action_log_dir, exist_ok=True)
    
    # --- 关键修改：生成唯一 Run ID ---
    # 使用纳秒级时间戳确保唯一性
    run_id = str(time.time_ns())

    # Step 1: Baseline (Noop)
    if function_name == 'noop':
        res, _, _ = _dispatch_core('noop', payload, is_workflow=False, custom_log_dir=action_log_dir, run_id=run_id)
        return res

    noise_metrics = {}
    noop_log_path = None
    
    try:
        with manager_lock:
            if 'noop' not in function_managers:
                function_managers['noop'] = FunctionManager('noop', 'jywang_test', 5000, None, min_idle_containers=1)
        
        # 运行 noop，传入 run_id
        _, noop_cid, noop_log_path = _dispatch_core('noop', payload, is_workflow=False, custom_log_dir=action_log_dir, run_id=run_id)
        
        if noop_log_path:
            noise_metrics = parse_perf_log(noop_log_path)
    except Exception as e:
        logger.warning(f"[Denoise] Baseline (noop) failed: {e}. Proceeding without denoising.")

    # Step 2: Target
    # 运行 target，传入相同的 run_id，这样它们的文件前缀一致
    result, target_cid, real_log_path = _dispatch_core(function_name, payload, is_workflow, custom_log_dir=action_log_dir, run_id=run_id)

    # Step 3: Calculate & Save
    try:
        if real_log_path:
            real_metrics = parse_perf_log(real_log_path)
            clean_metrics = calculate_clean_metrics(real_metrics, noise_metrics)
            
            # --- 关键修改：文件名加入 run_id ---
            clean_output_path = os.path.join(action_log_dir, f"{run_id}_clean_{function_name}_{target_cid[:12]}.json")
            
            record = {
                "function": function_name,
                "run_id": run_id,
                "timestamp": time.time(),
                "is_workflow": is_workflow,
                "raw_metrics": real_metrics,
                "noise_baseline": noise_metrics,
                "clean_metrics": clean_metrics
            }
            with open(clean_output_path, 'w') as f:
                json.dump(record, f, indent=2)
            logger.info(f"[Denoise] Metrics saved to {clean_output_path}")
    except Exception as e:
        logger.warning(f"[Denoise] Calculation failed: {e}")

    return result

# -------------------------------------------------------------------------
# Part 4: 辅助函数 & Workflows (适配修改后的 dispatch 返回值)
# 注意：dispatch 现在只返回 result 数据，内部逻辑处理了 run_id
# -------------------------------------------------------------------------

def save_result(db_key, filename):
    if not redis_client: return
    output_dir = '/home/jywang/FaaSDocker/results'
    if not os.path.exists(output_dir): os.makedirs(output_dir)
    filepath = os.path.join(output_dir, filename)
    try:
        val = redis_client.get(db_key)
        if val and val.startswith("COUCH_REF:") and couch_db:
            doc_id = val.split(":", 1)[1]
            if doc_id in couch_db:
                with open(filepath, 'wb') as f:
                    f.write(couch_db.get_attachment(doc_id, 'data').read())
        elif val:
            with open(filepath, 'w') as f:
                try:
                    json.dump(json.loads(val), f, indent=2)
                except:
                    f.write(str(val))
    except Exception as e:
        logger.error(f"[Save] Error: {e}")

def workflow_video(video_path):
    logger.info("=== Starting Video Workflow ===")
    if not redis_client: return
    try:
        # Upload
        upload_out = dispatch("video_upload", {}, is_workflow=True)
        video_key = upload_out['video'][0]
        name_key = upload_out['video_name'][0]
        time_key = upload_out['segment_time'][0]

        # Split
        split_out = dispatch("video_split", {
            "video": video_key, "video_name": name_key, "segment_time": time_key
        }, is_workflow=True)
        chunks_keys = split_out.get('splited_video', [])

        # Transcode
        target_type_key = f"const_target_{uuid.uuid4().hex[:4]}"
        redis_client.set(target_type_key, "avi")

        def _run_transcode(chunk_key):
            res = dispatch("video_transcode", {
                "video": chunk_key, "target_type": target_type_key
            }, is_workflow=True)
            return res.get('transcoded_video', [None])[0]

        with ThreadPoolExecutor(max_workers=4) as executor:
            transcode_results = list(executor.map(_run_transcode, chunks_keys))
        transcode_results = [k for k in transcode_results if k]

        # Merge
        merge_input_key = f"sys-merge-list-{uuid.uuid4().hex}"
        redis_client.set(merge_input_key, "LIST_REF:" + json.dumps(transcode_results))
        merge_out = dispatch("video_merge", {
            "video": merge_input_key, "target_type": target_type_key
        }, is_workflow=True)
        
        final_key = merge_out.get('final_video', [None])[0]
        if final_key: save_result(final_key, "final_video.avi")
        logger.info("Video Workflow Finished.")
    except Exception as e:
        logger.error(f"Video workflow failed: {e}", exc_info=True)

def workflow_recognizer(img_path):
    logger.info("=== Starting Recognizer Workflow ===")
    if not redis_client: return
    try:
        # 1. Upload
        upload_out = dispatch("recognizer_upload", {}, is_workflow=True)
        img_key = upload_out['img'][0]
        
        # 2. Parallel Analysis
        def _run_branch(action):
            return dispatch(action, {"img": img_key}, is_workflow=True)

        with ThreadPoolExecutor(max_workers=3) as ex:
            f_adult = ex.submit(_run_branch, "recognizer_adult")
            f_viol = ex.submit(_run_branch, "recognizer_violence")
            f_extr = ex.submit(_run_branch, "recognizer_extract")
            
            key_adult = f_adult.result().get('illegal', [None])[0]
            key_viol = f_viol.result().get('illegal', [None])[0]
            key_text = f_extr.result().get('text', [None])[0]

        is_adult = json.loads(redis_client.get(key_adult))
        is_viol = json.loads(redis_client.get(key_viol))
        logger.info(f"  > Adult: {is_adult}, Violence: {is_viol}")
        
        # 3. Text Analysis
        res_censor_out = dispatch("recognizer_censor", {"text": key_text}, is_workflow=True)
        dispatch("recognizer_translate", {"text": key_text}, is_workflow=True)
        
        key_censor = res_censor_out.get('illegal', [None])[0]
        is_censor = json.loads(redis_client.get(key_censor))
        logger.info(f"  > Censor Illegal: {is_censor}")

        # 4. Decision & Mosaic
        if is_adult or is_viol or is_censor:
            logger.warning("!!! ILLEGAL DETECTED !!! Mosaic...")
            mosaic_out = dispatch("recognizer_mosaic", {"img": img_key}, is_workflow=True)
            mosaic_keys = mosaic_out.get('mosaic_image', [])
            if mosaic_keys: 
                save_result(mosaic_keys[0], "mosaic_result.jpg")
        else:
            logger.info("Content clean. (No mosaic image generated)")
        
        # 5. Report (修复点：保存 JSON 报告)
        report = {"is_adult": is_adult, "is_violence": is_viol, "is_censor": is_censor}
        report_key = f"report-{uuid.uuid4().hex}"
        
        # 先存入 Redis
        redis_client.set(report_key, json.dumps(report))
        # 再保存到本地文件
        save_result(report_key, "recognizer_report.json")
        
        logger.info("Recognizer Workflow Finished.")
        
    except Exception as e:
        logger.error(f"Recognizer workflow failed: {e}", exc_info=True)
def workflow_svd():
    logger.info("=== Starting SVD Workflow ===")
    if not redis_client: return
    try:
        start_out = dispatch("svd_start", {}, is_workflow=True)
        matrix_keys = start_out.get('matrix', [])
        
        def _run_compute(m_key):
            res = dispatch("svd_compute", {"matrix": m_key}, is_workflow=True)
            return res.get('res', [None])[0]
            
        with ThreadPoolExecutor(max_workers=len(matrix_keys) or 1) as ex:
            compute_results = list(ex.map(_run_compute, matrix_keys))
        compute_results = [k for k in compute_results if k]
        
        merge_input_key = f"sys-svd-list-{uuid.uuid4().hex}"
        redis_client.set(merge_input_key, "LIST_REF:" + json.dumps(compute_results))
        merge_out = dispatch("svd_merge", {"res": merge_input_key}, is_workflow=True)
        
        res_keys = merge_out.get('final_res', [])
        if res_keys: save_result(res_keys[0], "svd_result.pkl")
        logger.info("SVD Workflow Finished.")
    except Exception as e:
        logger.error(f"SVD workflow failed: {e}", exc_info=True)

def workflow_wordcount():
    logger.info("=== Starting WordCount Workflow ===")
    if not redis_client: return
    try:
        start_out = dispatch("wordcount_start", {}, is_workflow=True)
        file_keys = start_out.get('file', [])
        if not file_keys: return

        def _run_count(f_key):
            res = dispatch("wordcount_count", {"file": f_key}, is_workflow=True)
            return res.get('res', [None])[0]
        
        with ThreadPoolExecutor(max_workers=len(file_keys)) as ex:
            count_results = list(ex.map(_run_count, file_keys))
        count_results = [k for k in count_results if k]
        
        merge_input_key = f"sys-wc-list-{uuid.uuid4().hex}"
        redis_client.set(merge_input_key, "LIST_REF:" + json.dumps(count_results))
        merge_out = dispatch("wordcount_merge", {"res": merge_input_key}, is_workflow=True)
        
        count_keys = merge_out.get('final_count', [])
        if count_keys: save_result(count_keys[0], "wordcount_result.txt")
        logger.info("WordCount Workflow Finished.")
    except Exception as e:
        logger.error(f"WordCount workflow failed: {e}", exc_info=True)

# -------------------------------------------------------------------------
# Part 5: HTTP 接口
# -------------------------------------------------------------------------

@app.route('/create_manager', methods=['POST'])
def create_manager():
    body = request.get_json(silent=True) or {}
    function_name = body.get("function_name")
    if not function_name: return jsonify({"error": "function_name required"}), 400
    with manager_lock:
        if function_name in function_managers: return jsonify({"status": "exists"}), 200
        function_managers[function_name] = FunctionManager(
            function_name=function_name,
            image_name='jywang_test', container_port=5000, host_storage_path=None, min_idle_containers=1
        )
    return jsonify({"status": "created"}), 201

@app.route('/dispatch_workflow', methods=['POST'])
def dispatch_workflow_api():
    body = request.get_json(silent=True) or {}
    name = body.get("workflow_name")
    t = None
    if name == "video": t = threading.Thread(target=workflow_video, args=(None,), name="WF-Video")
    elif name == "recognizer": t = threading.Thread(target=workflow_recognizer, args=(None,), name="WF-Recognizer")
    elif name == "svd": t = threading.Thread(target=workflow_svd, name="WF-SVD")
    elif name == "wordcount": t = threading.Thread(target=workflow_wordcount, name="WF-WordCount")
    
    if t:
        t.start()
        return jsonify({"status": "started", "workflow": name}), 202
    return jsonify({"error": "Unknown workflow"}), 400

@app.route('/dispatch/<function_name>', methods=['POST'])
def dispatch_single(function_name):
    payload = request.get_json(silent=True) or {}
    try:
        output = dispatch(function_name, payload, is_workflow=False)
        return jsonify({"status": "success", "output": output}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

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

def clean_up():
    logger.info("Stopping all containers...")
    with manager_lock:
        for m in function_managers.values(): m.stop_all_containers()
atexit.register(clean_up)

if __name__ == '__main__':
    os.makedirs(PERF_LOG_DIR, exist_ok=True)
    app.run(host='0.0.0.0', port=5000, threaded=True)