#!/usr/bin/env python3
"""
Closed-Loop 性能测试 (动态版): 在运行中动态调整 client 数量和 CPU 分配
每过 1/10 个 TEST_DURATION，随机增加或减少一个 client（对每个函数）
根据 client 数量变化自动调整 cgroup CPU 分配

使用示例: 
---------
# TEST_DURATION=300 NUMA_NODE=0 python3 run_experiment_closed_loop_dynamic.py

环境变量: 
---------
TEST_DURATION: 实验时长秒(默认300)
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
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

controller_host = os.environ.get('CONTROLLER_HOST', 'localhost')
controller_port = os.environ.get('CONTROLLER_PORT', '5000')
CONTROLLER_URL = f"http://{controller_host}:{controller_port}"

# 配置参数
TEST_DURATION = int(os.environ.get('TEST_DURATION', '600'))        # 实验时长(秒)
RANDOM_SEED = int(os.environ.get('RANDOM_SEED', '42'))             # 随机种子
NUMA_NODE = int(os.environ.get('NUMA_NODE', '0'))                  # NUMA节点号

# 动态调整参数
ADJUSTMENT_INTERVAL = TEST_DURATION / 10.0  # 每次调整间隔(秒)
MIN_CLIENTS_PER_FUNC = 2                     # 每函数最少 client 数
MAX_CLIENTS_PER_FUNC = 10                    # 每函数最多 client 数

# Cgroup 配置
CGROUP_PARENT = '/sys/fs/cgroup/user_experiments'
TASK_GROUPS_FILE = 'task_groups1.json'

# Redis 配置(用于预热工作流缓存)
REDIS_HOST = os.environ.get('REDIS_HOST', '172.17.0.1')
REDIS_PORT = int(os.environ.get('REDIS_PORT', '6379'))
redis_client = None

# 全局cgroup配置字典, 将在main()中基于task_groups.json生成
CGROUP_CONFIGS = {}
# 全局函数到分组的映射, 用于快速查找函数属于哪个分组
FUNC_TO_GROUP = {}
# 每个函数当前的 client 数
FUNC_CLIENT_COUNT = {}
# 客户端线程的句柄字典
CLIENT_THREADS = {}

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
dynamic_adjustment_lock = threading.Lock()
stop_flag = False  # 全局停止标志


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


def update_cgroup_cpus(cgroup_path, cpus):
    """更新 cgroup 的 CPU 配置"""
    try:
        child_cpus = os.path.join(cgroup_path, 'cpuset.cpus')
        with open(child_cpus, 'w') as f:
            f.write(cpus)
        print(f"[INFO] Updated cgroup {cgroup_path} CPUs to: {cpus}")
        return True
    except Exception as e:
        print(f"[ERROR] Failed to update cgroup {cgroup_path}: {e}")
        return False


def get_cgroup_for_function(func_name):
    """根据函数名获取对应的 cgroup 配置"""
    if func_name in FUNC_TO_GROUP:
        group_id = FUNC_TO_GROUP[func_name]
        group_name = f"group_{group_id}"
        if group_name in CGROUP_CONFIGS:
            return CGROUP_CONFIGS[group_name]
    
    if 'default' in CGROUP_CONFIGS:
        return CGROUP_CONFIGS['default']
    
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
            return None
        
        data = resp.json()
        out = data.get('output') if isinstance(data, dict) else None
        
        pid = None
        container_id = None
        duration = None
        
        if isinstance(out, dict):
            meta = out.get('__meta__', {})
            pid = meta.get('container_pid')
            container_id = meta.get('container_id')
            duration = meta.get('duration') or meta.get('func_duration')
        
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
    """预热工作流一次, 固定生成各子函数可重复使用的输入。"""
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
            video_key = first_item(up_out.get('video'))
            video_name_key = first_item(up_out.get('video_name'))
            segment_time_key = first_item(up_out.get('segment_time'))

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

                            if trans_list:
                                merge_input_key = f"sys-merge-list-{uuid.uuid4().hex}"
                                try:
                                    redis_client.set(merge_input_key, "LIST_REF:" + json.dumps(trans_list))
                                    caches['video_merge'] = {"video": merge_input_key, "target_type": target_type_key}
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
            img_key = first_item(rec_out.get('img'))

            if img_key:
                caches['recognizer_upload'] = {}
                caches['recognizer_adult'] = {"img": img_key}
                caches['recognizer_violence'] = {"img": img_key}
                caches['recognizer_extract'] = {"img": img_key}

                extr_res = dispatch_simple('recognizer_extract', {"img": img_key}, 'warmup-recognizer_extract')
                if extr_res:
                    extr_out = get_output(extr_res) or {}
                    text_key = first_item(extr_out.get('text'))
                    if text_key:
                        caches['recognizer_censor'] = {"text": text_key}
                        caches['recognizer_translate'] = {"text": text_key}

                caches['recognizer_mosaic'] = {"img": img_key}
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
            matrix_key = first_item(svd_out.get('matrix'))

            if matrix_key:
                caches['svd_start'] = {}
                caches['svd_compute'] = {"matrix": matrix_key}

                compute_res = dispatch_simple('svd_compute', {"matrix": matrix_key}, 'warmup-svd_compute')
                if compute_res:
                    comp_out = get_output(compute_res) or {}
                    comp_key = first_item(comp_out.get('res'))
                    if comp_key:
                        merge_list = [comp_key]
                        merge_key = f"sys-svd-list-{uuid.uuid4().hex}"
                        try:
                            redis_client.set(merge_key, "LIST_REF:" + json.dumps(merge_list))
                            caches['svd_merge'] = {"res": merge_key}
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
            file_key = first_item(wc_out.get('file'))

            if file_key:
                caches['wordcount_start'] = {}
                caches['wordcount_count'] = {"file": file_key}

                count_res = dispatch_simple('wordcount_count', {"file": file_key}, 'warmup-wordcount_count')
                if count_res:
                    count_out = get_output(count_res) or {}
                    count_key = first_item(count_out.get('res'))
                    if count_key:
                        merge_list = [count_key]
                        merge_key = f"sys-wc-list-{uuid.uuid4().hex}"
                        try:
                            redis_client.set(merge_key, "LIST_REF:" + json.dumps(merge_list))
                            caches['wordcount_merge'] = {"res": merge_key}
                        except Exception as e:
                            print(f"[WARN] Failed to set wordcount merge cache: {e}")
    except Exception as e:
        print(f"[WARN] WordCount workflow warmup failed: {e}")

    print(f"[INFO] Prepared workflow caches for {len(caches)} subfunctions: {list(caches.keys())}")
    return caches


def client_worker(client_id, func_name, payload_template, end_time, workflow_cached_payloads):
    """固定功能的闭环client: 在截止时间前持续发送请求"""
    request_counter = 0
    global stop_flag

    try:
        while time.time() < end_time and not stop_flag:
            # 检查是否应该跳过此客户端（动态调整时）
            with dynamic_adjustment_lock:
                if func_name not in FUNC_CLIENT_COUNT or FUNC_CLIENT_COUNT[func_name] < 1:
                    # 此客户端应该停止
                    print(f"[CLIENT {client_id}] {func_name} client count reduced, stopping")
                    break
            
            request_id = f"{func_name}-{client_id}-{request_counter}"
            payload = payload_template.copy() if isinstance(payload_template, dict) else {}
            result = dispatch_simple(func_name, payload, request_id)

            if result:
                print(f"[CLIENT {client_id}] Completed request {request_id}")
            else:
                print(f"[CLIENT {client_id}] Failed request {request_id}")

            request_counter += 1
    except Exception as e:
        print(f"[CLIENT {client_id}] Exception: {e}")


def generate_cgroups_from_task_groups(task_groups_file, numa_node, cgroup_parent):
    """基于task_groups.json生成cgroup配置"""
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
    
    groups = defaultdict(list)
    for func_name, group_id in task_groups.items():
        groups[group_id].append(func_name)
    
    configs = {}
    
    if numa_node == 0:
        cpu_pairs = [(i, i + 64) for i in range(0, 64, 2)]
    else:
        cpu_pairs = [(i, i + 64) for i in range(1, 64, 2)]

    all_cpus = []
    for a, b in cpu_pairs:
        all_cpus.extend([a, b])

    cpu_idx = 0
    
    for group_id in sorted(groups.keys()):
        funcs_in_group = groups[group_id]
        
        total_clients = len(funcs_in_group) * 2
        
        cpus_needed = math.ceil(total_clients / 5.0)
        if cpus_needed % 2 != 0:
            cpus_needed += 1
        
        cpus_list = []
        while len(cpus_list) < cpus_needed and cpu_idx < len(all_cpus):
            cpus_list.append(all_cpus[cpu_idx])
            cpu_idx += 1
        
        if cpus_list:
            cpus_str = ','.join(map(str, cpus_list))
            group_name = f"group_{group_id}"
            cgroup_path = os.path.join(cgroup_parent, group_name)
            configs[group_name] = {
                'path': cgroup_path,
                'cpus': cpus_str,
                'mems': str(numa_node),
                'functions': funcs_in_group
            }
            print(f"[INFO] Group {group_id}: {len(funcs_in_group)} functions, "
                  f"{total_clients} total clients, {cpus_needed} CPUs needed: {cpus_str}")
    
    return configs


def calculate_required_cpus(total_clients):
    """根据总client数计算需要的CPU数"""
    cpus_needed = math.ceil(total_clients / 5.0)
    if cpus_needed % 2 != 0:
        cpus_needed += 1
    return cpus_needed


def get_available_cpus(numa_node, start_idx):
    """获取可用的CPU列表"""
    if numa_node == 0:
        cpu_pairs = [(i, i + 64) for i in range(0, 64, 2)]
    else:
        cpu_pairs = [(i, i + 64) for i in range(1, 64, 2)]
    
    all_cpus = []
    for a, b in cpu_pairs:
        all_cpus.extend([a, b])
    
    return all_cpus[start_idx:]


def dynamic_adjustment_thread(workflow_cached_payloads, end_time):
    """动态调整 client 数量和 CPU 分配的后台线程"""
    global stop_flag
    
    next_adjustment_time = time.time() + ADJUSTMENT_INTERVAL
    adjustment_round = 0
    
    while time.time() < end_time and not stop_flag:
        current_time = time.time()
        
        if current_time >= next_adjustment_time:
            adjustment_round += 1
            print(f"\n{'='*80}")
            print(f"[DYNAMIC ADJUSTMENT ROUND {adjustment_round}] Adjusting client counts and CPUs")
            print(f"{'='*80}")
            
            with dynamic_adjustment_lock:
                # 随机调整每个函数的 client 数
                for func_name in FUNC_CLIENT_COUNT:
                    old_count = FUNC_CLIENT_COUNT[func_name]
                    # 随机增加或减少 1 个 client
                    delta = random.choice([-1, 1])
                    new_count = max(MIN_CLIENTS_PER_FUNC, min(MAX_CLIENTS_PER_FUNC, old_count + delta))
                    FUNC_CLIENT_COUNT[func_name] = new_count
                    
                    if old_count != new_count:
                        print(f"  [{func_name}] {old_count} -> {new_count} clients")
                
                # 重新计算每个分组需要的 CPU
                print(f"\n[CGROUP CPU REALLOCATION]")
                for group_name, config in CGROUP_CONFIGS.items():
                    if not group_name.startswith('group_'):
                        continue
                    
                    group_id = int(group_name.split('_')[1])
                    funcs_in_group = config['functions']
                    
                    # 统计该分组的总 client 数
                    total_clients_in_group = sum(
                        FUNC_CLIENT_COUNT.get(func, 2) for func in funcs_in_group
                    )
                    
                    # 计算所需 CPU
                    cpus_needed = calculate_required_cpus(total_clients_in_group)
                    
                    # 获取该分组的当前 CPU 分配
                    current_cpus_str = config['cpus']
                    current_cpus_list = list(map(int, current_cpus_str.split(',')))
                    current_cpus_count = len(current_cpus_list)
                    
                    # 如果需要调整
                    if cpus_needed != current_cpus_count:
                        print(f"  [Group {group_id}] Total clients: {total_clients_in_group}, "
                              f"Current CPUs: {current_cpus_count}, Required CPUs: {cpus_needed}")
                        
                        # 分配新的 CPU
                        if cpus_needed > current_cpus_count:
                            # 需要增加 CPU
                            cpu_pairs = [(i, i + 64) for i in range(0, 64, 2)] if NUMA_NODE == 0 \
                                       else [(i, i + 64) for i in range(1, 64, 2)]
                            all_cpus = []
                            for a, b in cpu_pairs:
                                all_cpus.extend([a, b])
                            
                            # 计算已使用的最大 CPU 索引
                            max_cpu_in_use = max([cpu for group in CGROUP_CONFIGS.values() 
                                                   for cpu in map(int, group['cpus'].split(','))], default=-1)
                            next_idx = all_cpus.index(max_cpu_in_use) + 1 if max_cpu_in_use >= 0 else 0
                            
                            additional_cpus = cpus_needed - current_cpus_count
                            new_cpus = current_cpus_list + all_cpus[next_idx:next_idx+additional_cpus]
                            new_cpus_str = ','.join(map(str, new_cpus))
                        else:
                            # 需要减少 CPU
                            new_cpus_str = ','.join(map(str, current_cpus_list[:cpus_needed]))
                        
                        # 更新 cgroup 配置
                        config['cpus'] = new_cpus_str
                        update_cgroup_cpus(config['path'], new_cpus_str)
                        print(f"    → Updated CPUs: {new_cpus_str}")
                    else:
                        print(f"  [Group {group_id}] Total clients: {total_clients_in_group}, "
                              f"CPUs: {current_cpus_count} (no change needed)")
            
            next_adjustment_time += ADJUSTMENT_INTERVAL
            print(f"Next adjustment at: {next_adjustment_time - time.time():.1f}s later\n")
        
        time.sleep(1)  # 每秒检查一次


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
    global CGROUP_CONFIGS, FUNC_TO_GROUP, FUNC_CLIENT_COUNT, stop_flag
    
    print(f"=== Closed-Loop Performance Test (Dynamic) ===")
    print(f"Test Duration: {TEST_DURATION}s")
    print(f"Adjustment Interval: {ADJUSTMENT_INTERVAL}s")
    print(f"Client Range: {MIN_CLIENTS_PER_FUNC}-{MAX_CLIENTS_PER_FUNC} per function")
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
    
    # 预热工作流缓存
    init_redis_client()
    workflow_cached_payloads = prepare_workflow_caches()

    # 仅基于分组映射的函数创建初始 clients
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

    # 初始化每个函数的 client 计数
    for func_name in mapped_funcs:
        FUNC_CLIENT_COUNT[func_name] = 2

    num_clients = len(client_configs)
    simple_count = len([c for c in client_configs if c[0] in SIMPLE_ACTIONS])
    workflow_count = num_clients - simple_count
    
    print(f"[INFO] Initial {num_clients} clients:")
    print(f"       - {simple_count} simple function clients")
    print(f"       - {workflow_count} workflow subfunction clients")
    
    # 创建并启动客户端线程
    print(f"\n[INFO] Starting {num_clients} client threads (closed-loop, fixed duration {TEST_DURATION}s)...")
    start_experiment = time.time()
    end_experiment_deadline = start_experiment + TEST_DURATION
    
    executor = ThreadPoolExecutor(max_workers=num_clients)
    client_futures = []
    
    for idx, (func_name, payload) in enumerate(client_configs):
        future = executor.submit(client_worker, idx, func_name, payload, end_experiment_deadline, workflow_cached_payloads)
        client_futures.append(future)
    
    # 启动动态调整线程
    adjustment_thread = threading.Thread(
        target=dynamic_adjustment_thread,
        args=(workflow_cached_payloads, end_experiment_deadline),
        daemon=True
    )
    adjustment_thread.start()
    
    # 等待所有 client 完成
    for future in as_completed(client_futures):
        future.result()
    
    executor.shutdown(wait=True)
    stop_flag = True
    adjustment_thread.join(timeout=5)
    
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
        print(f"  CV: {stat['cv']:.4f}")
        print(f"  Min: {stat['min']:.6f}s")
        print(f"  Max: {stat['max']:.6f}s")
        print(f"  P90: {stat['p90']:.6f}s")
        print(f"  P95: {stat['p95']:.6f}s")
    
    # 保存结果
    output = {
        "config": {
            "test_duration": TEST_DURATION,
            "initial_num_clients": num_clients,
            "adjustment_interval": ADJUSTMENT_INTERVAL,
            "client_range": [MIN_CLIENTS_PER_FUNC, MAX_CLIENTS_PER_FUNC],
            "numa_node": NUMA_NODE,
            "random_seed": RANDOM_SEED,
            "test_mode": "closed_loop_dynamic"
        },
        "summary": {
            "total_time": total_time,
            "throughput": total_completed / total_time,
            "completed_requests": total_completed
        },
        "statistics": stats
    }
    
    base_name = os.path.splitext(TASK_GROUPS_FILE)[0]
    os.makedirs('closed_loop_results', exist_ok=True)
    output_file = os.path.join('closed_loop_results', f"{base_name}_dynamic_results.json")
    with open(output_file, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"\n[INFO] Results saved to {output_file}")


if __name__ == '__main__':
    main()
