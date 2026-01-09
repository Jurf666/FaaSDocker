#!/usr/bin/env python3
"""
Closed-Loop 性能测试: 固定数量的客户端串行处理请求队列
每个客户端只有在完成当前请求后, 才会从队列取下一个请求

使用示例: 
---------
# 基础用法: 基于task_groups.json配置自动生成cgroups, 为各函数分配CPU核心
# TEST_DURATION=180 NUMA_NODE=0 python3 run_experiment_closed_loop.py

环境变量: 
---------
TEST_DURATION: 实验时长秒(默认60)
NUMA_NODE: NUMA节点号(默认0, 用于选择CPU范围)
RANDOM_SEED: 随机种子(默认42)
"""
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

controller_host = os.environ.get('CONTROLLER_HOST', 'localhost')
controller_port = os.environ.get('CONTROLLER_PORT', '5000')
CONTROLLER_URL = f"http://{controller_host}:{controller_port}"

# 配置参数
TEST_DURATION = int(os.environ.get('TEST_DURATION', '120'))        # 实验时长(秒)
RANDOM_SEED = int(os.environ.get('RANDOM_SEED', '42'))             # 随机种子
NUMA_NODE = int(os.environ.get('NUMA_NODE', '0'))                  # NUMA节点号

# Cgroup 配置
CGROUP_PARENT = '/sys/fs/cgroup/user_experiments'
TASK_GROUPS_FILE = 'task_groups.json'

# Redis 配置(用于预热工作流缓存)
REDIS_HOST = os.environ.get('REDIS_HOST', '172.17.0.1')
REDIS_PORT = int(os.environ.get('REDIS_PORT', '6379'))
redis_client = None

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
    "disk":            {"bs": "1M", "count": 1000},
    "couchdb_test":    {},
}

# 全局数据收集
perf_data = defaultdict(list)  # {function_name: [duration1, duration2, ...]}
data_lock = threading.Lock()


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


def first_item(val):
    """提取列表首元素或直接返回值。"""
    if isinstance(val, list) and val:
        return val[0]
    return val


def ensure_cgroup(cgroup_path, cpus, mems):
    """创建 cgroup 并配置 cpuset"""
    try:
        if not os.path.exists(CGROUP_PARENT):
            os.makedirs(CGROUP_PARENT, exist_ok=True)
        
        parent_cpus = os.path.join(CGROUP_PARENT, 'cpuset.cpus')
        parent_mems = os.path.join(CGROUP_PARENT, 'cpuset.mems')
        try:
            with open(parent_cpus, 'w') as f:
                f.write(cpus)
        except Exception:
            pass
        try:
            with open(parent_mems, 'w') as f:
                f.write(mems)
        except Exception:
            pass
        
        subtree = os.path.join(CGROUP_PARENT, 'cgroup.subtree_control')
        try:
            with open(subtree, 'r+') as f:
                txt = f.read()
                if '+cpuset' not in txt:
                    f.write('+cpuset')
        except Exception:
            os.system(f"echo +cpuset | sudo tee {subtree} > /dev/null")
        
        os.makedirs(cgroup_path, exist_ok=True)
        child_cpus = os.path.join(cgroup_path, 'cpuset.cpus')
        child_mems = os.path.join(cgroup_path, 'cpuset.mems')
        
        with open(child_cpus, 'w') as f:
            f.write(cpus)
        with open(child_mems, 'w') as f:
            f.write(mems)
        
        procs = os.path.join(cgroup_path, 'cgroup.procs')
        if not os.path.exists(procs):
            open(procs, 'w').close()
        
        print(f"[INFO] Created cgroup: {cgroup_path} (CPUs: {cpus})")
        return True
    except Exception as e:
        print(f"[ERROR] Failed to create cgroup {cgroup_path}: {e}")
        return False


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
    return {'path': CGROUP_PARENT, 'cpus': '0', 'mems': '0'}


def write_pid_to_cgroup(pid, cgroup_path):
    """将 PID 写入指定的 cgroup"""
    procs_path = os.path.join(cgroup_path, 'cgroup.procs')
    try:
        with open(procs_path, 'a') as f:
            f.write(str(int(pid)) + '\n')
        return True
    except Exception:
        try:
            os.system(f"echo {int(pid)} | sudo tee {procs_path} > /dev/null")
            return True
        except Exception as e:
            print(f"[ERROR] Failed to write pid {pid} to {procs_path}: {e}")
            return False


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
        
        pid = None
        container_id = None
        duration = None
        
        if isinstance(out, dict):
            meta = out.get('__meta__', {})
            pid = meta.get('container_pid')
            container_id = meta.get('container_id')
            duration = meta.get('duration') or meta.get('func_duration')
            print(f"[DEBUG] {request_id} ({func_name}): meta={meta}, duration={duration}")
        else:
            print(f"[DEBUG] {request_id} ({func_name}): output is not dict, out={out}")
        
        if not pid and container_id:
            try:
                import subprocess
                outp = subprocess.check_output(
                    ['docker', 'inspect', '--format', '{{.State.Pid}}', container_id],
                    stderr=subprocess.DEVNULL
                )
                pid = int(outp.decode().strip()) if outp else None
            except Exception:
                pass
        
        if pid:
            cgroup_config = get_cgroup_for_function(func_name)
            write_pid_to_cgroup(pid, cgroup_config['path'])
        
        if duration is not None:
            with data_lock:
                perf_data[func_name].append(duration)
        
        end_time = time.time()
        latency = end_time - start_time
        
        return {
            'request_id': request_id,
            'function': func_name,
            'duration': duration,
            'latency': latency,
            'pid': pid,
            'container_id': container_id,
            'output': out
        }
    
    except Exception as e:
        print(f"[ERROR] Request {request_id} ({func_name}) exception: {e}")
        return None


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


def generate_cgroups_from_task_groups(task_groups_file, numa_node, cgroup_parent):
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
                'path': os.path.join(cgroup_parent, 'default'),
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
        
        # 计算所需CPU核数：ceil(total_clients / 5), 然后向上取到最近的偶数
        cpus_needed = math.ceil(total_clients / 5.0)
        # 向上取到最近的偶数：如果已经是偶数就保持, 否则加1
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
            cgroup_path = os.path.join(cgroup_parent, group_name)
            configs[group_name] = {
                'path': cgroup_path,
                'cpus': cpus_str,
                'mems': str(numa_node),
                'functions': funcs_in_group  # 记录该分组包含的函数
            }
            print(f"[INFO] Group {group_id}: {len(funcs_in_group)} functions, "
                  f"{total_clients} total clients, {cpus_needed} CPUs needed: {cpus_str}")
    
    return configs


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


def main():
    global CGROUP_CONFIGS, FUNC_TO_GROUP
    
    print(f"=== Closed-Loop Performance Test ===")
    print(f"Test Duration: {TEST_DURATION}s")
    print(f"NUMA Node: {NUMA_NODE}")
    print(f"Random Seed: {RANDOM_SEED}")
    print()
    
    # 基于task_groups.json生成cgroup配置
    print(f"[INFO] Generating cgroup configurations from {TASK_GROUPS_FILE}...")
    CGROUP_CONFIGS = generate_cgroups_from_task_groups(TASK_GROUPS_FILE, NUMA_NODE, CGROUP_PARENT)
    
    # 构建函数到分组的映射
    for group_name, group_config in CGROUP_CONFIGS.items():
        if 'functions' in group_config:
            for func_name in group_config['functions']:
                # 从group_name提取group_id (例如 "group_0" -> 0)
                if group_name.startswith('group_'):
                    group_id = int(group_name.split('_')[1])
                    FUNC_TO_GROUP[func_name] = group_id
    
    # 创建 cgroup
    print("[INFO] Setting up cgroups...")
    for func_name, config in CGROUP_CONFIGS.items():
        if not ensure_cgroup(config['path'], config['cpus'], config['mems']):
            print(f"[WARN] Failed to create cgroup {func_name}, continuing...")
        else:
            print(f"[INFO] Created {func_name} cgroup: {config['path']} (CPUs: {config['cpus']})")
    
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
    
    # 对每个分组进行补齐：将 client 数量补齐到该分组 CPU 数的整数倍
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

    num_clients = len(client_configs)
    # 统计：按是否来自 SIMPLE_ACTIONS 或工作流缓存分类
    simple_count = len([c for c in client_configs if c[0] in SIMPLE_ACTIONS])
    workflow_count = num_clients - simple_count
    
    print(f"[INFO] Launching {num_clients} clients:")
    print(f"       - {simple_count} simple function clients")
    print(f"       - {workflow_count} workflow subfunction clients")
    print(f"       (each group padded to the nearest multiple of its CPU count)")
    
    # 创建并启动客户端线程(固定时长, 每函数一个client)
    print(f"\n[INFO] Starting {num_clients} client threads (closed-loop, fixed duration {TEST_DURATION}s)...")
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
    output_file = f"{base_name}_results.json"
    with open(output_file, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"\n[INFO] Results saved to {output_file}")


if __name__ == '__main__':
    main()
