import psutil 
import csv    
import requests
import time
import json
import os
import random
import uuid
import math
import numpy as np
import redis
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import shutil
import signal

controller_host = os.environ.get('CONTROLLER_HOST', 'localhost')
controller_port = os.environ.get('CONTROLLER_PORT', '5001')
CONTROLLER_URL = f"http://{controller_host}:{controller_port}"

# 配置参数
TEST_DURATION = int(os.environ.get('TEST_DURATION', '300'))        # 实验时长(秒)
RANDOM_SEED = int(os.environ.get('RANDOM_SEED', '42'))             # 随机种子
NUMA_NODE = int(os.environ.get('NUMA_NODE', '0'))                  # NUMA节点号

TASK_GROUPS_FILE = 'baseline_groups.json'

# Redis 配置(用于预热工作流缓存)
REDIS_HOST = os.environ.get('REDIS_HOST', '172.17.0.1')
REDIS_PORT = int(os.environ.get('REDIS_PORT', '6379'))
redis_client = None

# CouchDB 配置(用于清理工作流中间数据)
COUCHDB_URL = os.environ.get('COUCHDB_URL', 'http://openwhisk:openwhisk@172.17.0.1:5984/')
couchdb_client = None

# 全局cgroup配置字典, 将在main()中基于task_groups.json生成
CGROUP_CONFIGS = {}
# 全局函数到分组的映射, 用于快速查找函数属于哪个分组
FUNC_TO_GROUP = {}


# 简单函数及其参数
SIMPLE_ACTIONS = {
    "float_operation": {"param": 500000},
    "matmul":          {"param": 1000},
    "linpack":         {"param": 1000},
    "k-means":         {},
    "image":           {},
    "network":         {"name": "10mb"},
    "markdown2html":   {},
    "map_reduce":      {},
    "disk":            {"bs": "1M", "count": 100},
    "couchdb_test":    {},
}

# 全局数据收集
perf_data = defaultdict(list)  # {function_name: [duration1, duration2, ...]}
data_lock = threading.Lock()

# -------------------------------------------------------------------------
# Part 1: 准备工作
# -------------------------------------------------------------------------
def init_redis_client():
    """初始化 Redis 连接, 用于工作流中间结果缓存。"""
    global redis_client
    if redis_client:
        return redis_client
    try:
        redis_client = redis.StrictRedis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=0,
            decode_responses=True
        )
        redis_client.ping()
        print(f"[INFO] Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
    except Exception as e:
        redis_client = None
        print(f"[WARN] Redis not available, workflow cache warmup skipped: {e}")
    return redis_client

def init_couchdb_client():
    """初始化 CouchDB 连接, 用于清理工作流中间数据。"""
    global couchdb_client
    if couchdb_client:
        return couchdb_client
    try:
        import couchdb
        couchdb_client = couchdb.Server(COUCHDB_URL)
        print(f"[INFO] Connected to CouchDB at {COUCHDB_URL}")
    except Exception as e:
        couchdb_client = None
        print(f"[WARN] CouchDB not available, cleanup may be incomplete: {e}")
    return couchdb_client
  
def init_controller_managers(cgroup_configs, func_to_group):
    """
    遍历所有函数，根据计算出的分组信息，调用 Controller 的 API 进行初始化。
    这样 Controller 在创建容器时就会直接加上 --cpuset-cpus 参数。
    """
    print("[INFO] Initializing Function Managers on Controller with CPU sets...")
    
    # 获取所有涉及的函数
    all_funcs = set(func_to_group.keys())
    
    # 同时也包含那些可能没在 task_groups 但在 SIMPLE_ACTIONS 里的（如果有默认组的话）
    # 这里主要关注 task_groups.json 里定义的
    
    for func_name in all_funcs:
        group_id = func_to_group[func_name]
        group_name = f"group_{group_id}"
        
        cpuset = None
        if group_name in cgroup_configs:
            cpuset = cgroup_configs[group_name]['cpus']
        
        # 调用 Controller
        try:
            resp = requests.post(
                f"{CONTROLLER_URL}/create_manager",
                json={
                    "function_name": func_name,
                    "cpuset_cpus": cpuset
                },
                timeout=5
            )
            if resp.status_code in [200, 201]:
                print(f"   > Init {func_name}: cpuset={cpuset} (OK)")
            else:
                print(f"   > Init {func_name}: Failed {resp.text}")
        except Exception as e:
            print(f"   > Init {func_name}: Error {e}")

def prepare_workflow_caches():
    """预热工作流一次, 固定生成各子函数可重复使用的输入。
    
    新增功能：利用 controller.py 中新增的工作流子函数 REST 端点, 
    可以直接调用各个子函数进行预热, 获取中间结果用于后续 client 复用。
    """
    caches = {}
    if init_redis_client() is None:
        print(f"[WARN] Redis client init failed, skipping workflow cache warmup")
        return caches

    def get_output(res):
        if res is None:
            return {}
        return res.get('output') if isinstance(res, dict) else {}

    # --- Video workflow ---
    print("[INFO] === Warming up Video workflow ===")
    try:
        video_up = dispatch_simple('video_upload', {}, 'warmup-video_upload')
        if video_up is None:
            print("[WARN] video_upload dispatch failed")
        else:
            up_out = get_output(video_up) or {}
            print(f"[DEBUG] video_upload output keys: {list(up_out.keys())}")
            video_key = first_item(up_out.get('video'))
            video_name_key = first_item(up_out.get('video_name'))
            segment_time_key = first_item(up_out.get('segment_time'))
            print(f"[DEBUG] Extracted keys: video={video_key}, name={video_name_key}, time={segment_time_key}")

            if video_key and video_name_key and segment_time_key:
                caches['video_upload'] = {}
                split_payload = {
                    "video": video_key,
                    "video_name": video_name_key,
                    "segment_time": segment_time_key
                }
                caches['video_split'] = split_payload

                split_res = dispatch_simple('video_split', split_payload, 'warmup-video_split')
                if split_res:
                    split_out = get_output(split_res) or {}
                    chunks = split_out.get('splited_video') or []
                    chunk_key = first_item(chunks)
                    print(f"[DEBUG] Extracted chunk_key: {chunk_key}")

                    target_type_key = f"const_target_{uuid.uuid4().hex[:4]}"
                    try:
                        redis_client.set(target_type_key, "avi")
                    except Exception:
                        pass

                    if chunk_key:
                        transcode_payload = {"video": chunk_key, "target_type": target_type_key}
                        caches['video_transcode'] = transcode_payload

                        trans_res = dispatch_simple('video_transcode', transcode_payload, 'warmup-video_transcode')
                        if trans_res:
                            trans_out = get_output(trans_res) or {}
                            trans_list = trans_out.get('transcoded_video') or []
                            if not isinstance(trans_list, list):
                                trans_list = [trans_list] if trans_list else []
                            print(f"[DEBUG] Extracted transcode result: {trans_list}")

                            if trans_list:
                                merge_input_key = f"sys-merge-list-{uuid.uuid4().hex}"
                                try:
                                    redis_client.set(merge_input_key, "LIST_REF:" + json.dumps(trans_list))
                                    caches['video_merge'] = {"video": merge_input_key, "target_type": target_type_key}
                                    print(f"[DEBUG] Created video_merge cache with key: {merge_input_key}")
                                except Exception as e:
                                    print(f"[WARN] Failed to set video merge cache: {e}")
    except Exception as e:
        print(f"[WARN] Video workflow warmup failed: {e}")

    # --- Recognizer workflow ---
    print("[INFO] === Warming up Recognizer workflow ===")
    try:
        rec_up = dispatch_simple('recognizer_upload', {}, 'warmup-recognizer_upload')
        if rec_up is None:
            print("[WARN] recognizer_upload dispatch failed")
        else:
            rec_out = get_output(rec_up) or {}
            print(f"[DEBUG] recognizer_upload output keys: {list(rec_out.keys())}")
            img_key = first_item(rec_out.get('img'))
            print(f"[DEBUG] Extracted img_key: {img_key}")

            if img_key:
                caches['recognizer_upload'] = {}
                caches['recognizer_adult'] = {"img": img_key}
                caches['recognizer_violence'] = {"img": img_key}
                caches['recognizer_extract'] = {"img": img_key}

                extr_res = dispatch_simple('recognizer_extract', {"img": img_key}, 'warmup-recognizer_extract')
                if extr_res:
                    extr_out = get_output(extr_res) or {}
                    print(f"[DEBUG] recognizer_extract output keys: {list(extr_out.keys())}")
                    text_key = first_item(extr_out.get('text'))
                    print(f"[DEBUG] Extracted text_key: {text_key}")
                    if text_key:
                        caches['recognizer_censor'] = {"text": text_key}
                        caches['recognizer_translate'] = {"text": text_key}

                caches['recognizer_mosaic'] = {"img": img_key}
                print(f"[DEBUG] Added recognizer caches: {list(caches.keys())}")
            else:
                print("[WARN] img_key is None, recognizer caches not created")
    except Exception as e:
        print(f"[WARN] Recognizer workflow warmup failed: {e}")

    # --- SVD workflow ---
    print("[INFO] === Warming up SVD workflow ===")
    try:
        svd_start = dispatch_simple('svd_start', {}, 'warmup-svd_start')
        if svd_start is None:
            print("[WARN] svd_start dispatch failed")
        else:
            svd_out = get_output(svd_start) or {}
            print(f"[DEBUG] svd_start output keys: {list(svd_out.keys())}")
            matrix_key = first_item(svd_out.get('matrix'))
            print(f"[DEBUG] Extracted matrix_key: {matrix_key}")

            if matrix_key:
                caches['svd_start'] = {}
                caches['svd_compute'] = {"matrix": matrix_key}

                compute_res = dispatch_simple('svd_compute', {"matrix": matrix_key}, 'warmup-svd_compute')
                if compute_res:
                    comp_out = get_output(compute_res) or {}
                    comp_key = first_item(comp_out.get('res'))
                    print(f"[DEBUG] Extracted svd compute result: {comp_key}")
                    if comp_key:
                        merge_list = [comp_key]
                        merge_key = f"sys-svd-list-{uuid.uuid4().hex}"
                        try:
                            redis_client.set(merge_key, "LIST_REF:" + json.dumps(merge_list))
                            caches['svd_merge'] = {"res": merge_key}
                            print(f"[DEBUG] Created svd_merge cache with key: {merge_key}")
                        except Exception as e:
                            print(f"[WARN] Failed to set svd merge cache: {e}")
    except Exception as e:
        print(f"[WARN] SVD workflow warmup failed: {e}")

    # --- WordCount workflow ---
    print("[INFO] === Warming up WordCount workflow ===")
    try:
        wc_start = dispatch_simple('wordcount_start', {}, 'warmup-wordcount_start')
        if wc_start is None:
            print("[WARN] wordcount_start dispatch failed")
        else:
            wc_out = get_output(wc_start) or {}
            print(f"[DEBUG] wordcount_start output keys: {list(wc_out.keys())}")
            file_key = first_item(wc_out.get('file'))
            print(f"[DEBUG] Extracted file_key: {file_key}")

            if file_key:
                caches['wordcount_start'] = {}
                caches['wordcount_count'] = {"file": file_key}

                count_res = dispatch_simple('wordcount_count', {"file": file_key}, 'warmup-wordcount_count')
                if count_res:
                    count_out = get_output(count_res) or {}
                    count_key = first_item(count_out.get('res'))
                    print(f"[DEBUG] Extracted wordcount result: {count_key}")
                    if count_key:
                        merge_list = [count_key]
                        merge_key = f"sys-wc-list-{uuid.uuid4().hex}"
                        try:
                            redis_client.set(merge_key, "LIST_REF:" + json.dumps(merge_list))
                            caches['wordcount_merge'] = {"res": merge_key}
                            print(f"[DEBUG] Created wordcount_merge cache with key: {merge_key}")
                        except Exception as e:
                            print(f"[WARN] Failed to set wordcount merge cache: {e}")
    except Exception as e:
        print(f"[WARN] WordCount workflow warmup failed: {e}")

    print(f"[INFO] Prepared workflow caches for {len(caches)} subfunctions: {list(caches.keys())}")
    return caches

# -------------------------------------------------------------------------
# Part 2: 核心代码
# -------------------------------------------------------------------------
def generate_cgroups_from_task_groups(task_groups_file, numa_node):
    """
    基于task_groups.json生成cgroup配置
    
    逻辑：
     1. 读取task_groups.json, 任务按分组号分组(数字表示分组)
     2. 对于同一分组的所有函数, 计算总的 client 数：分组内函数数 × 2
     3. 计算该分组需要的CPU核数：ceil(total_client_count / 5) 后向上取到最近的偶数
     4. 为每个分组创建一个cgroup, 分配计算出的CPU核心
     5. CPU编号：按物理核对端逻辑核成对分配
         - NUMA 0: (0,64), (2,66), (4,68), ...
         - NUMA 1: (1,65), (3,67), (5,69), ...
    """
    if not os.path.exists(task_groups_file):
        print(f"[WARN] {task_groups_file} not found, using default cgroup")
        return {
            'default': {
                'cpus': '0',
                'mems': str(numa_node)
            }
        }
    
    with open(task_groups_file, 'r') as f:
        task_groups = json.load(f)
    
    # 按分组号将函数分组
    groups = defaultdict(list)
    for func_name, group_id in task_groups.items():
        groups[group_id].append(func_name)
    
    configs = {}
    
    # 生成 NUMA 节点对应的 CPU 成对列表
    # NUMA 0 使用偶数对：0/64, 2/66, 4/68, ...
    # NUMA 1 使用奇数对：1/65, 3/67, 5/69, ...
    if numa_node == 0:
        cpu_pairs = [(i, i + 64) for i in range(0, 64, 2)]
    else:
        cpu_pairs = [(i, i + 64) for i in range(1, 64, 2)]

    # 展开为扁平列表, 顺序为 pair1[0], pair1[1], pair2[0], pair2[1], ...
    all_cpus = []
    for a, b in cpu_pairs:
        all_cpus.extend([a, b])

    cpu_idx = 0  # 当前使用的CPU索引(按成对顺序分配)
    
    for group_id in sorted(groups.keys()):
        funcs_in_group = groups[group_id]
        
        # 计算该分组的总client数
        # 每个函数在该分组中贡献 2 个 client
        total_clients = len(funcs_in_group) * 2
        
        """
        # 计算所需CPU核数：ceil(total_clients / 5), 然后向上取到最近的偶数
        cpus_needed = math.ceil(total_clients / 5.0)
        # 向上取到最近的偶数：如果已经是偶数就保持, 否则加1
        if cpus_needed % 2 != 0:
            cpus_needed += 1
        """
        is_baseline = (len(groups) == 1) or ('baseline' in task_groups_file)
        
        if is_baseline:
            print(f"[INFO] Baseline Detected (Group {group_id}): Allocating ALL {len(all_cpus)} CPUs to reduce contention.")
            cpus_needed = len(all_cpus) # 直接给满 64 个核 (假设 NUMA 0)
        else:
            # 原有逻辑：仅对非 Baseline 的分组实验进行严苛的资源隔离
            cpus_needed = math.ceil(total_clients / 5.0)
            if cpus_needed % 2 != 0:
                cpus_needed += 1
        
        # 分配CPU
        cpus_list = []
        while len(cpus_list) < cpus_needed and cpu_idx < len(all_cpus):
            cpus_list.append(all_cpus[cpu_idx])
            cpu_idx += 1
        
        if cpus_list:
            cpus_str = ','.join(map(str, cpus_list))
            # 使用group_id作为cgroup名称
            group_name = f"group_{group_id}"
            configs[group_name] = {
                'cpus': cpus_str,
                'mems': str(numa_node),
                'functions': funcs_in_group  # 记录该分组包含的函数
            }
            print(f"[INFO] Group {group_id}: {len(funcs_in_group)} functions, "
                  f"{total_clients} total clients, {cpus_needed} CPUs needed: {cpus_str}")
    
    return configs

def get_cgroup_for_function(func_name):
    """根据函数名获取对应的 cgroup 配置"""
    # 如果函数在映射中, 找到它属于的分组, 返回该分组的cgroup配置
    if func_name in FUNC_TO_GROUP:
        group_id = FUNC_TO_GROUP[func_name]
        group_name = f"group_{group_id}"
        if group_name in CGROUP_CONFIGS:
            return CGROUP_CONFIGS[group_name]
    
    # 如果找不到, 返回default cgroup
    if 'default' in CGROUP_CONFIGS:
        return CGROUP_CONFIGS['default']
    
    # 最后的备选方案
    return {'cpus': '0', 'mems': '0'}

def dispatch_simple(func_name, payload, request_id):
    """发送简单函数请求"""
    start_time = time.time()
    try:
        resp = requests.post(
            f"{CONTROLLER_URL}/dispatch/{func_name}",
            json=payload,
            timeout=1200
        )
        
        if resp.status_code != 200:
            print(f"[ERROR] Request {request_id} ({func_name}) failed: {resp.status_code}")
            print(f"[ERROR] Response body: {resp.text}")
            return None
        
        data = resp.json()
        out = data.get('output') if isinstance(data, dict) else None
        print(f"[DEBUG] {request_id} ({func_name}): response keys={list(data.keys())}, output type={type(out)}")
        
        container_id = None
        duration = None
        
        if isinstance(out, dict):
            meta = out.get('__meta__', {})
            container_id = meta.get('container_id')
            duration = meta.get('duration') or meta.get('func_duration')
            print(f"[DEBUG] {request_id} ({func_name}): meta={meta}, duration={duration}")
        else:
            print(f"[DEBUG] {request_id} ({func_name}): output is not dict, out={out}")
        
        # 检查 duration 是否有效，且 output 不包含错误信息
        is_valid_duration = (duration is not None) and (duration > 0)
        
        # 检查是否返回了错误信息 (Proxy 抛异常时通常返回 {"error": ...})
        has_error = False
        if isinstance(out, dict) and 'error' in out:
            has_error = True
            print(f"[WARN] Request {request_id} failed with error: {out['error']}")

        if is_valid_duration and not has_error:
            with data_lock:
                perf_data[func_name].append(duration)
        else:
            # 可选：记录一下被丢弃的数据，方便调试
            if duration == 0 or duration is None:
                print(f"[FILTER] Ignored invalid duration {duration} for {func_name}")
        
        end_time = time.time()
        latency = end_time - start_time
        
        return {
            'request_id': request_id,
            'function': func_name,
            'duration': duration,
            'latency': latency,
            'container_id': container_id,
            'output': out
        }
    
    except Exception as e:
        print(f"[ERROR] Request {request_id} ({func_name}) exception: {e}")
        return None

def client_worker(client_id, func_name, payload_template, end_time):
    """固定功能的闭环client: 在截止时间前持续发送请求"""
    request_counter = 0

    try:
        while time.time() < end_time:
            request_id = f"{func_name}-{client_id}-{request_counter}"
            payload = payload_template.copy() if isinstance(payload_template, dict) else {}
            result = dispatch_simple(func_name, payload, request_id)

            if result:
                print(f"[CLIENT {client_id}] Completed request {request_id} ({func_name})")
            else:
                print(f"[CLIENT {client_id}] Failed request {request_id} ({func_name})")

            request_counter += 1
    except Exception as e:
        print(f"[CLIENT {client_id}] Exception: {e}")

# -------------------------------------------------------------------------
# Part 3: 指标监控与计算
# -------------------------------------------------------------------------
def compute_stability(times):
    """计算统计指标"""
    if not times:
        return {}
    arr = np.array(times)
    mean = float(np.mean(arr))
    std = float(np.std(arr))
    cv = float(std / mean) if mean != 0 else 0.0
    
    return {
        "count": len(times),
        "mean": mean,
        "variance": float(np.var(arr)),
        "std": std,
        "cv": cv,
        "min": float(np.min(arr)),
        "max": float(np.max(arr)),
        "iqr": float(np.percentile(arr, 75) - np.percentile(arr, 25)),
        "p90": float(np.percentile(arr, 90)),
        "p95": float(np.percentile(arr, 95))
    }

def monitor_system(stop_event, cgroup_configs, filename="system_metric.csv"):
    print(f"[MONITOR] Starting monitor (w_await, aqu-sz), saving to {filename}...")
    
    # 1. 优化 CPU 列排序 (同组紧邻)
    ordered_cpu_list = []
    sorted_groups = sorted(cgroup_configs.keys(), key=lambda x: int(x.split('_')[1]) if '_' in x else x)
    
    for group_name in sorted_groups:
        config = cgroup_configs[group_name]
        if 'cpus' in config:
            cpu_ids = [int(x) for x in config['cpus'].split(',') if x.strip()]
            for cid in cpu_ids:
                ordered_cpu_list.append((cid, group_name))
    
    # 3. 初始化 CSV
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        
        # --- 构建表头 ---
        headers = ["timestamp"]
        for cid, gname in ordered_cpu_list:
            headers.append(f"CPU_{cid}({gname})") 
        writer.writerow(headers)
        
        last_time = time.time()

        # --- 5. 循环采样 ---
        while not stop_event.is_set():
            try:
                # 采样间隔控制
                current_time = time.time()
                time_delta = current_time - last_time
                if time_delta < 1.0:
                    time.sleep(1.0 - time_delta)
                    current_time = time.time()
                    time_delta = current_time - last_time
                
                row = [current_time]
                
                # A. 获取 CPU 数据
                all_cpus = psutil.cpu_percent(interval=None, percpu=True)
                for cid, _ in ordered_cpu_list:
                    if cid < len(all_cpus):
                        row.append(all_cpus[cid])
                    else:
                        row.append(-1)

                writer.writerow(row)
                f.flush()
                last_time = current_time
                
            except Exception as e:
                print(f"[MONITOR] Error: {e}")
                break
    print("[MONITOR] Monitoring stopped.")

# -------------------------------------------------------------------------
# Part 4: 数据清理与辅助函数
# -------------------------------------------------------------------------
def cleanup_workflow_data():
    """清理工作流产生的中间数据（Redis + CouchDB）"""
    print("\n[INFO] === Cleaning up workflow intermediate data ===")
    
    # 1. 清理 Redis 中的工作流相关 key
    if redis_client:
        try:
            # 获取所有 key 并过滤出工作流相关的
            all_keys = redis_client.keys('*')
            workflow_patterns = [
                'req-*', 'warmup-*', 'sys-*', 'const_target_*',
                '*video*', '*recognizer*', '*svd*', '*wordcount*',
                '*split*', '*transcode*', '*merge*', '*upload*',
                '*adult*', '*violence*', '*extract*', '*censor*',
                '*translate*', '*mosaic*', '*compute*', '*count*'
            ]
            
            keys_to_delete = []
            for key in all_keys:
                # 检查是否匹配工作流模式
                for pattern in workflow_patterns:
                    import fnmatch
                    if fnmatch.fnmatch(key, pattern):
                        keys_to_delete.append(key)
                        break
            
            if keys_to_delete:
                # 批量删除
                deleted = redis_client.delete(*keys_to_delete)
                print(f"[INFO] Deleted {deleted} workflow keys from Redis")
            else:
                print(f"[INFO] No workflow keys found in Redis (total keys: {len(all_keys)})")
        except Exception as e:
            print(f"[WARN] Failed to cleanup Redis: {e}")
    
    # 2. 清理 CouchDB 中的 faas_data 数据库
    if init_couchdb_client():
        try:
            import couchdb
            if 'faas_data' in couchdb_client:
                db = couchdb_client['faas_data']
                doc_count = 0
                docs_to_delete = []
                
                # 收集所有文档
                for doc_id in db:
                    # 跳过设计文档
                    if not doc_id.startswith('_'):
                        doc = db[doc_id]
                        docs_to_delete.append({'_id': doc_id, '_rev': doc['_rev'], '_deleted': True})
                        doc_count += 1
                
                # 批量删除
                if docs_to_delete:
                    db.update(docs_to_delete)
                    print(f"[INFO] Deleted {doc_count} documents from CouchDB faas_data database")
                else:
                    print(f"[INFO] No documents found in CouchDB faas_data database")
            else:
                print(f"[INFO] CouchDB faas_data database does not exist, nothing to clean")
        except Exception as e:
            print(f"[WARN] Failed to cleanup CouchDB: {e}")
    
    print("[INFO] === Cleanup completed ===")
    
def first_item(val):
    """提取列表首元素或直接返回值。"""
    if isinstance(val, list) and val:
        return val[0]
    return val



def main():
    global CGROUP_CONFIGS, FUNC_TO_GROUP
    
    print(f"=== Closed-Loop Performance Test ===")
    print(f"Test Duration: {TEST_DURATION}s")
    print(f"NUMA Node: {NUMA_NODE}")
    print(f"Random Seed: {RANDOM_SEED}")
    print()
    
    # 获得CGROUP_CONFIGS（基于task_groups.json生成cgroup配置）
    print(f"[INFO] Generating cgroup configurations from {TASK_GROUPS_FILE}...")
    CGROUP_CONFIGS = generate_cgroups_from_task_groups(TASK_GROUPS_FILE, NUMA_NODE)

    # 获得FUNC_TO_GROUP（构建函数到分组的映射）
    for group_name, group_config in CGROUP_CONFIGS.items():
        if 'functions' in group_config:
            for func_name in group_config['functions']:
                # 从group_name提取group_id (例如 "group_0" -> 0)
                if group_name.startswith('group_'):
                    group_id = int(group_name.split('_')[1])
                    FUNC_TO_GROUP[func_name] = group_id
                    
    # 在开始任何请求之前，先把配置推送到 Controller
    init_controller_managers(CGROUP_CONFIGS, FUNC_TO_GROUP)
        
    # 预热工作流缓存, 固定各子函数输入
    init_redis_client()
    workflow_cached_payloads = prepare_workflow_caches()

    # 仅基于分组映射的函数创建初始 clients(每函数 2 个), 避免超出分组文件造成计数偏差
    client_configs = []
    mapped_funcs = set(FUNC_TO_GROUP.keys())
    for func_name in sorted(mapped_funcs):
        if func_name in SIMPLE_ACTIONS:
            payload_src = SIMPLE_ACTIONS[func_name]
        elif func_name in workflow_cached_payloads:
            payload_src = workflow_cached_payloads[func_name]
        else:
            payload_src = {}
        for _ in range(2):
            payload_copy = payload_src.copy() if isinstance(payload_src, dict) else {}
            client_configs.append((func_name, payload_copy))

    # 按分组统计当前的 client 数量
    group_clients_count = defaultdict(int)
    for func_name, payload in client_configs:
        if func_name in FUNC_TO_GROUP:
            group_id = FUNC_TO_GROUP[func_name]
            group_clients_count[group_id] += 1
    
    """
    # 将每组 client 数量补齐到该分组 CPU 数的整数倍
    random.seed(RANDOM_SEED)
    for group_name, config in CGROUP_CONFIGS.items():
        if not group_name.startswith('group_'):
            continue
        
        group_id = int(group_name.split('_')[1])
        cpus_allocated = len(config['cpus'].split(','))
        current_clients = group_clients_count[group_id]
        
        # 需要补齐到 CPU 数量的整数倍
        if cpus_allocated > 0:
            remainder = current_clients % cpus_allocated
            if remainder != 0:
                padding_needed = cpus_allocated - remainder
                target_clients = current_clients + padding_needed
                funcs_in_group = config['functions']
                
                print(f"[INFO] Padding Group {group_id}: {current_clients} -> {target_clients} clients "
                      f"(CPU={cpus_allocated}, +{padding_needed} padding clients)")
                
                for _ in range(padding_needed):
                    # 从该分组的函数中随机选择
                    func_name = random.choice(funcs_in_group)
                    
                    # 获取该函数的 payload
                    if func_name in SIMPLE_ACTIONS:
                        payload = SIMPLE_ACTIONS[func_name].copy()
                    elif func_name in workflow_cached_payloads:
                        payload = workflow_cached_payloads[func_name].copy() if isinstance(workflow_cached_payloads[func_name], dict) else {}
                    else:
                        payload = {}
                    
                    client_configs.append((func_name, payload))
                    group_clients_count[group_id] += 1
    """
    """
    # 将每组 client 数量补满
    random.seed(RANDOM_SEED)
    for group_name, config in CGROUP_CONFIGS.items():
        if not group_name.startswith('group_'):
            continue
        
        group_id = int(group_name.split('_')[1])
        cpus_allocated = len(config['cpus'].split(','))
        current_clients = group_clients_count[group_id]
        
        # 需要补齐到 CPU 数量的整数倍
        if cpus_allocated > 0:
            TARGET_DENSITY = 5 
    
            # 3. 计算该组 CPU 在满载时应该跑多少个 Client
            # 例如：2个核 * 5 = 10个 Client
            target_clients = cpus_allocated * TARGET_DENSITY
    
            # 4. 计算需要补齐的数量
            # 注意：如果初始任务极多导致密度超过5(比如刚好除尽)，padding_needed 会是 0
            if current_clients < target_clients:
                padding_needed = target_clients - current_clients
                funcs_in_group = config['functions']
            else:
                padding_needed = 0
        
            print(f"[INFO] Saturating Group {group_id}: {current_clients} -> {target_clients} clients "
                f"(CPU={cpus_allocated}, Density={TARGET_DENSITY}, +{padding_needed} padding)")
                
            for _ in range(padding_needed):
                # 从该分组的函数中随机选择
                func_name = random.choice(funcs_in_group)
                    
                # 获取该函数的 payload
                if func_name in SIMPLE_ACTIONS:
                    payload = SIMPLE_ACTIONS[func_name].copy()
                elif func_name in workflow_cached_payloads:
                    payload = workflow_cached_payloads[func_name].copy() if isinstance(workflow_cached_payloads[func_name], dict) else {}
                else:
                    payload = {}
                    
                client_configs.append((func_name, payload))
                group_clients_count[group_id] += 1
    
    """
    num_clients = len(client_configs)
    # 统计：按是否来自 SIMPLE_ACTIONS 或工作流缓存分类
    simple_count = len([c for c in client_configs if c[0] in SIMPLE_ACTIONS])
    workflow_count = num_clients - simple_count
    
    print(f"[INFO] Launching {num_clients} clients:")
    print(f"       - {simple_count} simple function clients")
    print(f"       - {workflow_count} workflow subfunction clients")
    #print(f"       (each group padded to the nearest multiple of its CPU count)")
    
    # 创建并启动客户端线程(固定时长, 每函数一个client)
    print(f"\n[INFO] Starting {num_clients} client threads (closed-loop, fixed duration {TEST_DURATION}s)...")
    
    import threading
    monitor_stop_event = threading.Event()
    monitor_thread = threading.Thread(
        target=monitor_system,
        # 参数: event, cgroup配置, 文件名, 目标磁盘
        args=(monitor_stop_event, CGROUP_CONFIGS, "system_metrics.csv")
    )
    monitor_thread.daemon = True
    monitor_thread.start()

    print(f"\n[INFO] Starting {num_clients} client threads...")
    start_experiment = time.time()
    end_experiment_deadline = start_experiment + TEST_DURATION
    
    executor = ThreadPoolExecutor(max_workers=num_clients)
    client_futures = []
    
    for idx, (func_name, payload) in enumerate(client_configs):
        future = executor.submit(client_worker, idx, func_name, payload, end_experiment_deadline)
        client_futures.append(future)
    
    for future in as_completed(client_futures):
        future.result()

    executor.shutdown(wait=True)
    monitor_stop_event.set() # 通知监控线程停止
    monitor_thread.join()    # 等待监控线程结束
    
    end_experiment = time.time()
    total_time = end_experiment - start_experiment
    total_completed = sum(len(times) for times in perf_data.values())
    
    print(f"\n[INFO] All clients finished in {total_time:.2f}s")
    print(f"[INFO] Completed Requests: {total_completed}")
    print(f"[INFO] Throughput: {total_completed / total_time:.2f} req/s")
    
    # 计算统计数据
    print("\n=== Performance Statistics ===")
    stats = {}
    for func_name in sorted(perf_data.keys()):
        times = perf_data[func_name]
        stat = compute_stability(times)
        stats[func_name] = stat
        
        print(f"\n[{func_name}]")
        print(f"  Count: {stat['count']}")
        print(f"  Mean: {stat['mean']:.6f}s")
        print(f"  Std: {stat['std']:.6f}s")
        print(f"  CV (Coefficient of Variation): {stat['cv']:.4f}")
        print(f"  Min: {stat['min']:.6f}s")
        print(f"  Max: {stat['max']:.6f}s")
        print(f"  P90: {stat['p90']:.6f}s")
        print(f"  P95: {stat['p95']:.6f}s")
    
    # 保存结果
    output = {
        "config": {
            "test_duration": TEST_DURATION,
            "num_clients": num_clients,
            "numa_node": NUMA_NODE,
            "random_seed": RANDOM_SEED,
            "test_mode": "closed_loop"
        },
        "summary": {
            "total_time": total_time,
            "throughput": total_completed / total_time,
            "completed_requests": total_completed
        },
        "statistics": stats
    }
    
    # 以 TASK_GROUPS_FILE(去掉扩展名)为基础生成结果文件名, 例如
    # task_groups1.json -> task_groups1_results.json
    base_name = os.path.splitext(TASK_GROUPS_FILE)[0]
    os.makedirs('closed_loop_results', exist_ok=True)
    output_file = os.path.join('closed_loop_results', f"{base_name}_results.json")
    with open(output_file, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"\n[INFO] Results saved to {output_file}")
    
    # 清理工作流产生的中间数据，防止磁盘占用不断增长
    cleanup_workflow_data()


if __name__ == '__main__':
    main()