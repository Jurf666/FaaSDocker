# run_experiment_closed_loop.py
import time
import json
import os
import random
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

# 导入拆分后的模块
from config import *
from utils.connections import (
    init_redis_client, init_couchdb_client,
    init_controller_managers, wait_for_warmup
)
from utils.cgroup_manager import (
    generate_cgroups_from_task_groups, build_func_to_group_mapping
)
from utils.workflow_utils import prepare_workflow_caches, cleanup_workflow_data
from utils.request_handler import client_worker, get_perf_data
from utils.metrics_calculator import compute_stability, generate_experiment_summary

def main():
    print("=== Closed-Loop Performance Test ===")
    print(f"Duration: {TEST_DURATION}s | NUMA: {NUMA_NODE} | Seed: {RANDOM_SEED}")
    
    # 1. 初始化cgroup配置和函数映射
    cgroup_configs = generate_cgroups_from_task_groups(TASK_GROUPS_FILE, REFERENCE_GROUPS_FILE)
    func_to_group = build_func_to_group_mapping(cgroup_configs)
    
    # 2. 初始化Controller和容器预热
    init_controller_managers(cgroup_configs, func_to_group)
    wait_for_warmup(func_to_group)
    
    # 3. 初始化连接和工作流缓存
    redis_client = init_redis_client()
    couchdb_client = init_couchdb_client()
    workflow_caches = prepare_workflow_caches(redis_client)
    
    # 4. 生成客户端配置
    client_configs = []
    mapped_funcs = set(func_to_group.keys())
    for func_name in sorted(mapped_funcs):
        payload = SIMPLE_ACTIONS.get(func_name, workflow_caches.get(func_name, {}))
        for _ in range(CLIENTS_PER_FUNCTION):
            client_configs.append((func_name, payload.copy()))

    # p.s. client的补充逻辑
    """
    # 将每组 client 数量补齐到该分组 CPU 数的整数倍
    random.seed(RANDOM_SEED)
    for group_name, config in cgroup_configs.items():
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

    # 将每组 client 数量补满
    random.seed(RANDOM_SEED)
    for group_name, config in cgroup_configs.items():
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

    # 5. 启动实验
    print(f"\n[INFO] Launching {len(client_configs)} clients...")
    start_time = time.time()
    end_deadline = start_time + TEST_DURATION
    
    # 通知服务端开始监控
    try:
        requests.post(f"{CONTROLLER_URL}/start_monitor", json={"cgroup_configs": cgroup_configs}, timeout=5)
    except Exception as e:
        print(f"[WARN] Failed to start monitor: {e}")
    
    # 启动客户端线程
    executor = ThreadPoolExecutor(max_workers=len(client_configs))
    client_futures = []
    for idx, (func_name, payload) in enumerate(client_configs):
        future = executor.submit(client_worker, idx, func_name, payload, end_deadline)
        client_futures.append(future)
    for future in as_completed(client_futures):
        future.result()
    executor.shutdown(wait=True)
    
    # 通知服务端停止监控
    try:
        requests.post(f"{CONTROLLER_URL}/stop_monitor", timeout=5)
    except Exception as e:
        print(f"[WARN] Failed to stop monitor: {e}")
    
    # 6. 计算并保存结果
    total_time = time.time() - start_time
    perf_data = get_perf_data()
    summary = generate_experiment_summary(perf_data, total_time)
    stats = {func: compute_stability(times) for func, times in perf_data.items()}
    
    # 打印统计结果
    print("\n=== Performance Statistics ===")
    for func, stat in stats.items():
        print(f"\n[{func}]")
        print(f"  Count: {stat['count']} | Mean: {stat['mean']:.6f}s | CV: {stat['cv']:.4f}")
    
    # 保存结果
    output = {
        "config": {"test_duration": TEST_DURATION, "num_clients": len(client_configs), "numa_node": NUMA_NODE},
        "summary": summary,
        "statistics": stats
    }
    os.makedirs('closed_loop_results', exist_ok=True)
    output_file = f"closed_loop_results/{os.path.splitext(TASK_GROUPS_FILE)[0]}_results.json"
    with open(output_file, 'w') as f:
        json.dump(output, f, indent=2)
    print(f"\n[INFO] Results saved to {output_file}")
    
    # 7. 清理数据
    cleanup_workflow_data(redis_client, couchdb_client)

if __name__ == '__main__':
    main()