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

# --- 新增：全局任务状态存储 ---
# 结构: { "task-uuid": "running" | "completed" | "failed" }
task_status_store = {}
task_store_lock = threading.Lock()

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
# 定义日志存储路径, 使用相对路径更加健全
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PERF_LOG_DIR = os.path.join(BASE_DIR, 'storage', 'perf_logs')
# Perf 开关：设置为 'false' 或 '0' 可禁用 perf 记录
ENABLE_PERF = os.environ.get('ENABLE_PERF', 'false').lower() not in ['false', '0', 'no']

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
        'cycles','instructions',
        'task-clock','context-switches',
        'cache-misses','L1-dcache-load-misses',
        'LLC-load-misses','page-faults',
        'major-faults','minor-faults',
        'branch-misses'
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
    manager = _get_or_create_manager(function_name)

    container_id = None
    perf_process = None
    perf_log_file = None
    pid = None
    
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
        if custom_log_dir and ENABLE_PERF:
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
                    
                    events = 'cycles,instructions,task-clock,context-switches,cache-misses,L1-dcache-load-misses,LLC-load-misses,page-faults,major-faults,minor-faults,branch-misses'
                    perf_cmd = ['sudo', 'perf', 'stat', '-p', str(pid), '-e', events, 'sleep', '2000']
                    
                    perf_log_file = open(perf_output_filename, 'w')
                    perf_process = subprocess.Popen(perf_cmd, stdout=subprocess.PIPE, stderr=perf_log_file, preexec_fn=os.setsid)
                    time.sleep(0.1) 
            except Exception as e:
                logger.warning(f"Perf start failed: {e}")
                if perf_log_file: perf_log_file.close()

        # 6. RUN
        start = time.time()
        # 调整了 timeout 为 2000s 以避免 matmul 超时
        resp = requests.post(f"http://127.0.0.1:{host_port}/run", json=proxy_payload, timeout=2000)
        
        if resp.status_code != 200:
            logger.error(f"Container Error ({resp.status_code}): {resp.text}")
            # 显式抛出包含错误信息的异常，方便调试
            raise Exception(f"Container Error: {resp.text}") 
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
        # proxy 返回结构: { start_time, end_time, duration(HTTP 全过程), result: { func_result, func_duration, ... } }
        duration_from_proxy = proxy_result.get("func_duration", 0)  # 只取函数执行时间
        
        duration = time.time() - start
        mode_str = "WORKFLOW" if is_workflow else "SIMPLE"
        logger.info(f"[Run][{mode_str}] {function_name} finished in {duration:.2f}s (func duration: {duration_from_proxy:.4f}s) [proxy_result keys: {list(proxy_result.keys())}]")
        
        final_result = proxy_result.get("output_keys", {}) if is_workflow else proxy_result.get("func_result")

        # 返回结果、容器ID、perf 日志路径、容器 pid 以及函数执行时间（供 cgroup 操作使用）
        return final_result, container_id, perf_output_filename, pid, duration_from_proxy

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
    
    注意：如果 ENABLE_PERF=false，则不会生成 perf 日志。
    """
    action_log_dir = os.path.join(PERF_LOG_DIR, function_name)
    os.makedirs(action_log_dir, exist_ok=True)
    
    # --- 关键修改：生成唯一 Run ID ---
    # 使用纳秒级时间戳确保唯一性
    run_id = str(time.time_ns())

    # Step 1: Baseline (Noop)
    if function_name == 'noop':
        res, _, _, _, duration = _dispatch_core('noop', payload, is_workflow=False, custom_log_dir=action_log_dir, run_id=run_id)
        return res, {'container_pid': None, 'container_id': None, 'perf_log': None, 'duration': duration, 'func_duration': duration}

    noise_metrics = {}
    noop_log_path = None
    
    try:
        '''
        with manager_lock:
            if 'noop' not in function_managers:
                function_managers['noop'] = FunctionManager('noop', 'jywang_test', 5000, None, min_idle_containers=1)
        '''
        # 运行 noop，传入 run_id
        _, noop_cid, noop_log_path, noop_pid, _ = _dispatch_core('noop', payload, is_workflow=False, custom_log_dir=action_log_dir, run_id=run_id)
        
        if noop_log_path:
            noise_metrics = parse_perf_log(noop_log_path)
    except Exception as e:
        logger.warning(f"[Denoise] Baseline (noop) failed: {e}. Proceeding without denoising.")

    # Step 2: Target
    # 运行 target，传入相同的 run_id，这样它们的文件前缀一致
    result, target_cid, real_log_path, target_pid, target_duration = _dispatch_core(function_name, payload, is_workflow, custom_log_dir=action_log_dir, run_id=run_id)

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

    # 对于 simple 调用，确保返回一个 dict，并在 '__meta__' 中附加 pid/container/perf 信息，便于客户端进行 cgroup/pinning
    try:
        if not is_workflow:
            if isinstance(result, dict):
                out = dict(result)  # shallow copy
            else:
                out = {"_value": result}

            out.setdefault('__meta__', {})
            out['__meta__'].update({
                'container_pid': target_pid,
                'container_id': target_cid,
                'perf_log': real_log_path,
                'duration': target_duration,
                'func_duration': target_duration
            })
            return out
        else:
            # workflow 调用：返回结果和一个附加的元数据字典（包含本次调用的 pid/container/duration）
            # 注意：workflow 内部会多次调用 dispatch，每次都会有自己的 pid
            # 这里只返回最外层的 target action 的 pid，子任务由 workflow 函数收集
            return result, {
                'container_pid': target_pid,
                'container_id': target_cid,
                'perf_log': real_log_path,
                'duration': target_duration,
                'func_duration': target_duration
            }
    except Exception:
        pass

    return result

# -------------------------------------------------------------------------
# Part 4: 辅助函数 & Workflows (适配修改后的 dispatch 返回值)
# 注意：dispatch 现在只返回 result 数据，内部逻辑处理了 run_id
# -------------------------------------------------------------------------

def save_result(db_key, filename):
    if not redis_client: return
    output_dir = './results'
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
    if not redis_client: 
        raise Exception("Redis not connected") # 修正1：主动抛错

    subtasks = []  # 收集所有子任务元数据

    # Upload
    upload_out, meta = dispatch("video_upload", {}, is_workflow=True)
    subtasks.append({'name': 'video_upload', **meta})
    video_key = upload_out['video'][0]
    name_key = upload_out['video_name'][0]
    time_key = upload_out['segment_time'][0]

    # Split
    split_out, meta = dispatch("video_split", {
        "video": video_key, "video_name": name_key, "segment_time": time_key
    }, is_workflow=True)
    subtasks.append({'name': 'video_split', **meta})
    chunks_keys = split_out.get('splited_video', [])

    # Transcode
    target_type_key = f"const_target_{uuid.uuid4().hex[:4]}"
    redis_client.set(target_type_key, "avi")

    def _run_transcode(chunk_key):
        res, meta = dispatch("video_transcode", {
            "video": chunk_key, "target_type": target_type_key
        }, is_workflow=True)
        subtasks.append({'name': 'video_transcode', **meta})
        return res.get('transcoded_video', [None])[0]

    with ThreadPoolExecutor(max_workers=4) as executor:
        transcode_results = list(executor.map(_run_transcode, chunks_keys))
    transcode_results = [k for k in transcode_results if k]

    # Merge
    merge_input_key = f"sys-merge-list-{uuid.uuid4().hex}"
    redis_client.set(merge_input_key, "LIST_REF:" + json.dumps(transcode_results))
    merge_out, meta = dispatch("video_merge", {
        "video": merge_input_key, "target_type": target_type_key
    }, is_workflow=True)
    subtasks.append({'name': 'video_merge', **meta})
        
    final_key = merge_out.get('final_video', [None])[0]
    if final_key: save_result(final_key, "final_video.avi")
    logger.info("Video Workflow Finished.")
    return subtasks

def workflow_recognizer(img_path):
    logger.info("=== Starting Recognizer Workflow ===")
    if not redis_client: 
        raise Exception("Redis not connected") # 修正1：主动抛错

    subtasks = []  # 收集所有子任务元数据

    # 1. Upload
    upload_out, meta = dispatch("recognizer_upload", {}, is_workflow=True)
    subtasks.append({'name': 'recognizer_upload', **meta})
    img_key = upload_out['img'][0]
        
    # 2. Parallel Analysis
    def _run_branch(action):
        res, meta = dispatch(action, {"img": img_key}, is_workflow=True)
        subtasks.append({'name': action, **meta})
        return res

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
    res_censor_out, meta = dispatch("recognizer_censor", {"text": key_text}, is_workflow=True)
    subtasks.append({'name': 'recognizer_censor', **meta})
    _, meta = dispatch("recognizer_translate", {"text": key_text}, is_workflow=True)
    subtasks.append({'name': 'recognizer_translate', **meta})
        
    key_censor = res_censor_out.get('illegal', [None])[0]
    is_censor = json.loads(redis_client.get(key_censor))
    logger.info(f"  > Censor Illegal: {is_censor}")

    # 4. Decision & Mosaic
    if is_adult or is_viol or is_censor:
        logger.warning("!!! ILLEGAL DETECTED !!! Mosaic...")
        mosaic_out, meta = dispatch("recognizer_mosaic", {"img": img_key}, is_workflow=True)
        subtasks.append({'name': 'recognizer_mosaic', **meta})
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
    return subtasks
       
def workflow_svd():
    logger.info("=== Starting SVD Workflow ===")
    if not redis_client: 
        raise Exception("Redis not connected") # 修正1：主动抛错

    subtasks = []  # 收集所有子任务元数据

    start_out, meta = dispatch("svd_start", {}, is_workflow=True)
    subtasks.append({'name': 'svd_start', **meta})
    matrix_keys = start_out.get('matrix', [])
        
    def _run_compute(m_key):
        res, meta = dispatch("svd_compute", {"matrix": m_key}, is_workflow=True)
        subtasks.append({'name': 'svd_compute', **meta})
        return res.get('res', [None])[0]
            
    with ThreadPoolExecutor(max_workers=len(matrix_keys) or 1) as ex:
        compute_results = list(ex.map(_run_compute, matrix_keys))
    compute_results = [k for k in compute_results if k]
        
    merge_input_key = f"sys-svd-list-{uuid.uuid4().hex}"
    redis_client.set(merge_input_key, "LIST_REF:" + json.dumps(compute_results))
    merge_out, meta = dispatch("svd_merge", {"res": merge_input_key}, is_workflow=True)
    subtasks.append({'name': 'svd_merge', **meta})
        
    res_keys = merge_out.get('final_res', [])
    if res_keys: save_result(res_keys[0], "svd_result.pkl")
    logger.info("SVD Workflow Finished.")
    return subtasks

def workflow_wordcount():
    logger.info("=== Starting WordCount Workflow ===")
    if not redis_client: 
        raise Exception("Redis not connected") # 修正1：主动抛错

    subtasks = []  # 收集所有子任务元数据

    start_out, meta = dispatch("wordcount_start", {}, is_workflow=True)
    subtasks.append({'name': 'wordcount_start', **meta})
    file_keys = start_out.get('file', [])
    if not file_keys: return subtasks

    def _run_count(f_key):
        res, meta = dispatch("wordcount_count", {"file": f_key}, is_workflow=True)
        subtasks.append({'name': 'wordcount_count', **meta})
        return res.get('res', [None])[0]
        
    with ThreadPoolExecutor(max_workers=len(file_keys)) as ex:
        count_results = list(ex.map(_run_count, file_keys))
    count_results = [k for k in count_results if k]
        
    merge_input_key = f"sys-wc-list-{uuid.uuid4().hex}"
    redis_client.set(merge_input_key, "LIST_REF:" + json.dumps(count_results))
    merge_out, meta = dispatch("wordcount_merge", {"res": merge_input_key}, is_workflow=True)
    subtasks.append({'name': 'wordcount_merge', **meta})
        
    count_keys = merge_out.get('final_count', [])
    if count_keys: save_result(count_keys[0], "wordcount_result.txt")
    logger.info("WordCount Workflow Finished.")
    return subtasks


def update_task_status(task_id, status, subtasks=None):
    with task_store_lock:
        task_status_store[task_id] = {"status": status, "subtasks": subtasks or []}

def run_workflow_async(name, payload, task_id):
    subtasks = []
    try:
        update_task_status(task_id, "running")
        
        # 执行原有的逻辑并收集子任务元数据
        if name == "video":
            subtasks = workflow_video(payload)
        elif name == "recognizer":
            subtasks = workflow_recognizer(payload)
        elif name == "svd":
            subtasks = workflow_svd()
        elif name == "wordcount":
            subtasks = workflow_wordcount()
            
        update_task_status(task_id, "completed", subtasks=subtasks)
        logger.info(f"Task {task_id} ({name}) completed with {len(subtasks)} subtasks.")
    except Exception as e:
        logger.error(f"Task {task_id} failed: {e}")
        update_task_status(task_id, "failed", subtasks=subtasks)

def clean_up():
    logger.info("Stopping all containers...")
    with manager_lock:
        for m in function_managers.values(): m.stop_all_containers()

# --- 新增的辅助函数 ---
def _get_or_create_manager(function_name):
    """
    线程安全地获取 FunctionManager，如果不存在则使用默认配置创建。
    """
    # 1. 快速检查（无锁），稍微提高性能（可选）
    if function_name in function_managers:
        return function_managers[function_name]

    # 2. 加锁进行创建或获取
    with manager_lock:
        # 双重检查：防止在等待锁的过程中已经被别的线程创建了
        if function_name not in function_managers:
            # 统一在这里配置镜像名、端口等参数
            function_managers[function_name] = FunctionManager(
                function_name=function_name,
                image_name='yyxie-test2',     # 统一配置
                container_port=5000,          # 统一配置
                host_storage_path=None, 
                min_idle_containers=1
            )
        return function_managers[function_name]

# -------------------------------------------------------------------------
# Part 5: HTTP 接口
# -------------------------------------------------------------------------

@app.route('/create_manager', methods=['POST'])
def create_manager():
    body = request.get_json(silent=True) or {}
    function_name = body.get("function_name")
    if not function_name: return jsonify({"error": "function_name required"}), 400
    already_exists = function_name in function_managers
    _get_or_create_manager(function_name)
    
    if already_exists:
        return jsonify({"status": "exists"}), 200
    else:
        return jsonify({"status": "created"}), 201
'''
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
'''
@app.route('/dispatch_workflow', methods=['POST'])
def dispatch_workflow_api():
    body = request.get_json(silent=True) or {}
    name = body.get("workflow_name")
    payload = body.get("payload", {})
    
    # 1. 生成 Task ID
    task_id = f"task-{uuid.uuid4().hex[:8]}"
    
    # 2. 启动线程，传入 task_id
    t = threading.Thread(target=run_workflow_async, args=(name, payload, task_id))
    t.start()
    
    # 3. 立即返回 Task ID
    return jsonify({
        "status": "accepted", 
        "task_id": task_id,
        "message": "Task running in background"
    }), 202
    
@app.route('/dispatch/<function_name>', methods=['POST'])
def dispatch_single(function_name):
    payload = request.get_json(silent=True) or {}
    try:
        output = dispatch(function_name, payload, is_workflow=False)
        return jsonify({"status": "success", "output": output}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# =========================================================================
# 工作流子函数独立 REST 端点 (方案 A)
# =========================================================================

# --- Video Workflow Sub-functions ---
@app.route('/dispatch/video_upload', methods=['POST'])
def dispatch_video_upload():
    payload = request.get_json(silent=True) or {}
    try:
        result, meta = dispatch('video_upload', payload, is_workflow=True)
        # 将结果包装为与 dispatch_single 一致的格式
        if isinstance(result, dict):
            output = dict(result)
        else:
            output = {"_value": result}
        output.setdefault('__meta__', {})
        output['__meta__'].update(meta)
        return jsonify({"status": "success", "output": output}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/dispatch/video_split', methods=['POST'])
def dispatch_video_split():
    payload = request.get_json(silent=True) or {}
    try:
        result, meta = dispatch('video_split', payload, is_workflow=True)
        if isinstance(result, dict):
            output = dict(result)
        else:
            output = {"_value": result}
        output.setdefault('__meta__', {})
        output['__meta__'].update(meta)
        return jsonify({"status": "success", "output": output}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/dispatch/video_transcode', methods=['POST'])
def dispatch_video_transcode():
    payload = request.get_json(silent=True) or {}
    try:
        result, meta = dispatch('video_transcode', payload, is_workflow=True)
        if isinstance(result, dict):
            output = dict(result)
        else:
            output = {"_value": result}
        output.setdefault('__meta__', {})
        output['__meta__'].update(meta)
        return jsonify({"status": "success", "output": output}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/dispatch/video_merge', methods=['POST'])
def dispatch_video_merge():
    payload = request.get_json(silent=True) or {}
    try:
        result, meta = dispatch('video_merge', payload, is_workflow=True)
        if isinstance(result, dict):
            output = dict(result)
        else:
            output = {"_value": result}
        output.setdefault('__meta__', {})
        output['__meta__'].update(meta)
        return jsonify({"status": "success", "output": output}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# --- Recognizer Workflow Sub-functions ---
@app.route('/dispatch/recognizer_upload', methods=['POST'])
def dispatch_recognizer_upload():
    payload = request.get_json(silent=True) or {}
    try:
        result, meta = dispatch('recognizer_upload', payload, is_workflow=True)
        if isinstance(result, dict):
            output = dict(result)
        else:
            output = {"_value": result}
        output.setdefault('__meta__', {})
        output['__meta__'].update(meta)
        return jsonify({"status": "success", "output": output}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/dispatch/recognizer_adult', methods=['POST'])
def dispatch_recognizer_adult():
    payload = request.get_json(silent=True) or {}
    try:
        result, meta = dispatch('recognizer_adult', payload, is_workflow=True)
        if isinstance(result, dict):
            output = dict(result)
        else:
            output = {"_value": result}
        output.setdefault('__meta__', {})
        output['__meta__'].update(meta)
        return jsonify({"status": "success", "output": output}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/dispatch/recognizer_violence', methods=['POST'])
def dispatch_recognizer_violence():
    payload = request.get_json(silent=True) or {}
    try:
        result, meta = dispatch('recognizer_violence', payload, is_workflow=True)
        if isinstance(result, dict):
            output = dict(result)
        else:
            output = {"_value": result}
        output.setdefault('__meta__', {})
        output['__meta__'].update(meta)
        return jsonify({"status": "success", "output": output}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/dispatch/recognizer_extract', methods=['POST'])
def dispatch_recognizer_extract():
    payload = request.get_json(silent=True) or {}
    try:
        result, meta = dispatch('recognizer_extract', payload, is_workflow=True)
        if isinstance(result, dict):
            output = dict(result)
        else:
            output = {"_value": result}
        output.setdefault('__meta__', {})
        output['__meta__'].update(meta)
        return jsonify({"status": "success", "output": output}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/dispatch/recognizer_censor', methods=['POST'])
def dispatch_recognizer_censor():
    payload = request.get_json(silent=True) or {}
    try:
        result, meta = dispatch('recognizer_censor', payload, is_workflow=True)
        if isinstance(result, dict):
            output = dict(result)
        else:
            output = {"_value": result}
        output.setdefault('__meta__', {})
        output['__meta__'].update(meta)
        return jsonify({"status": "success", "output": output}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/dispatch/recognizer_translate', methods=['POST'])
def dispatch_recognizer_translate():
    payload = request.get_json(silent=True) or {}
    try:
        result, meta = dispatch('recognizer_translate', payload, is_workflow=True)
        if isinstance(result, dict):
            output = dict(result)
        else:
            output = {"_value": result}
        output.setdefault('__meta__', {})
        output['__meta__'].update(meta)
        return jsonify({"status": "success", "output": output}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/dispatch/recognizer_mosaic', methods=['POST'])
def dispatch_recognizer_mosaic():
    payload = request.get_json(silent=True) or {}
    try:
        result, meta = dispatch('recognizer_mosaic', payload, is_workflow=True)
        if isinstance(result, dict):
            output = dict(result)
        else:
            output = {"_value": result}
        output.setdefault('__meta__', {})
        output['__meta__'].update(meta)
        return jsonify({"status": "success", "output": output}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# --- SVD Workflow Sub-functions ---
@app.route('/dispatch/svd_start', methods=['POST'])
def dispatch_svd_start():
    payload = request.get_json(silent=True) or {}
    try:
        result, meta = dispatch('svd_start', payload, is_workflow=True)
        if isinstance(result, dict):
            output = dict(result)
        else:
            output = {"_value": result}
        output.setdefault('__meta__', {})
        output['__meta__'].update(meta)
        return jsonify({"status": "success", "output": output}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/dispatch/svd_compute', methods=['POST'])
def dispatch_svd_compute():
    payload = request.get_json(silent=True) or {}
    try:
        result, meta = dispatch('svd_compute', payload, is_workflow=True)
        if isinstance(result, dict):
            output = dict(result)
        else:
            output = {"_value": result}
        output.setdefault('__meta__', {})
        output['__meta__'].update(meta)
        return jsonify({"status": "success", "output": output}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/dispatch/svd_merge', methods=['POST'])
def dispatch_svd_merge():
    payload = request.get_json(silent=True) or {}
    try:
        result, meta = dispatch('svd_merge', payload, is_workflow=True)
        if isinstance(result, dict):
            output = dict(result)
        else:
            output = {"_value": result}
        output.setdefault('__meta__', {})
        output['__meta__'].update(meta)
        return jsonify({"status": "success", "output": output}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# --- WordCount Workflow Sub-functions ---
@app.route('/dispatch/wordcount_start', methods=['POST'])
def dispatch_wordcount_start():
    payload = request.get_json(silent=True) or {}
    try:
        result, meta = dispatch('wordcount_start', payload, is_workflow=True)
        if isinstance(result, dict):
            output = dict(result)
        else:
            output = {"_value": result}
        output.setdefault('__meta__', {})
        output['__meta__'].update(meta)
        return jsonify({"status": "success", "output": output}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/dispatch/wordcount_count', methods=['POST'])
def dispatch_wordcount_count():
    payload = request.get_json(silent=True) or {}
    try:
        result, meta = dispatch('wordcount_count', payload, is_workflow=True)
        if isinstance(result, dict):
            output = dict(result)
        else:
            output = {"_value": result}
        output.setdefault('__meta__', {})
        output['__meta__'].update(meta)
        return jsonify({"status": "success", "output": output}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/dispatch/wordcount_merge', methods=['POST'])
def dispatch_wordcount_merge():
    payload = request.get_json(silent=True) or {}
    try:
        result, meta = dispatch('wordcount_merge', payload, is_workflow=True)
        if isinstance(result, dict):
            output = dict(result)
        else:
            output = {"_value": result}
        output.setdefault('__meta__', {})
        output['__meta__'].update(meta)
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

# --- 新增：查询状态接口 ---432
@app.route('/check_task/<task_id>', methods=['GET'])
def check_task(task_id):
    with task_store_lock:
        task_info = task_status_store.get(task_id, {"status": "unknown", "subtasks": []})
    # 兼容旧格式（字符串）和新格式（字典）
    if isinstance(task_info, str):
        return jsonify({"task_id": task_id, "status": task_info, "subtasks": []}), 200
    return jsonify({"task_id": task_id, "status": task_info.get("status", "unknown"), "subtasks": task_info.get("subtasks", [])}), 200

atexit.register(clean_up)

if __name__ == '__main__':
    os.makedirs(PERF_LOG_DIR, exist_ok=True)
    # 支持通过环境变量改变监听地址与端口，方便多用户在同机运行
    host = os.environ.get('CONTROLLER_HOST', '0.0.0.0')
    port = int(os.environ.get('CONTROLLER_PORT', '5001'))
    logger.info(f"Starting Controller on {host}:{port}")
    logger.info(f"ENABLE_PERF={ENABLE_PERF} (set ENABLE_PERF=false to disable perf recording)")
    app.run(host=host, port=port, threaded=True)