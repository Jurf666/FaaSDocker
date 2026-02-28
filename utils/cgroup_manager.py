# utils/cgroup_manager.py
import json
import os
import math
from collections import defaultdict
from config import NUMA_NODE, CLIENTS_PER_FUNCTION

def generate_cgroups_from_task_groups(current_file, reference_file):
    """
    构建分组到配置的映射
    1. 首先基于 reference_file (通常是实验组配置) 计算出实验所需的 CPU 核心全集。
    2. 如果 current_file == reference_file:
       表示当前跑的是实验组，按细粒度分组分配 CPU。
    3. 如果 current_file != reference_file:
       表示当前跑的是对照组，将第1步计算出的【所有CPU核心】统一分配给对照组，
       确保两组实验使用的总物理资源完全一致。
    """
    # --- 内部函数：计算某个文件所需的 CPU 分配方案 ---
    def _calculate_allocation(json_file, available_cpus):
        if not os.path.exists(json_file):
            print(f"[WARN] {json_file} not found")
            return {}, []
        
        with open(json_file, 'r') as f:
            groups_data = json.load(f)
        
        # 分组
        groups = defaultdict(list)
        for func, gid in groups_data.items():
            groups[gid].append(func)
        
        allocation_map = {} # {group_id: [cpu_list]}
        used_cpus_ordered = [] # 记录所有被分配出去的CPU
        
        cpu_ptr = 0 # 指针
        
        for gid in sorted(groups.keys()):
            funcs = groups[gid]
            total_clients = len(funcs) * CLIENTS_PER_FUNCTION
            
            # 计算核数逻辑 (与你原有逻辑保持一致)
            cpus_needed = math.ceil(total_clients / 5.0)
            if cpus_needed % 2 != 0:
                cpus_needed += 1
            
            # 从可用列表中取 CPU
            assigned = []
            while len(assigned) < cpus_needed and cpu_ptr < len(available_cpus):
                assigned.append(available_cpus[cpu_ptr])
                cpu_ptr += 1
            
            allocation_map[gid] = {
                "cpus": assigned,
                "funcs": funcs
            }
            used_cpus_ordered.extend(assigned)
        
        return allocation_map, used_cpus_ordered

    # 生成NUMA CPU列表
    cpu_pairs = [(i, i + 64) for i in range(NUMA_NODE, 64, 2)]
    all_phys_cpus = []
    for a, b in cpu_pairs:
        all_phys_cpus.extend([a, b])
    
    # 计算参考组配置
    ref_allocation, ref_total_cpus = _calculate_allocation(reference_file, all_phys_cpus)
    ref_cpu_str = ",".join(map(str, ref_total_cpus))
    print(f"[INFO] Reference CPUs ({len(ref_total_cpus)} cores): {ref_cpu_str}")
    
    # 生成当前组配置
    with open(current_file, 'r') as f:
        current_data = json.load(f)
    current_groups = defaultdict(list)
    for func, gid in current_data.items():
        current_groups[gid].append(func)
    
    final_configs = {}
    # 判断模式
    is_experiment_mode = (current_file == reference_file)
    
    if is_experiment_mode:
        print(f"[MODE] EXPERIMENT MODE. Using fine-grained allocation.")
        for gid, info in ref_allocation.items():
            gname = f"group_{gid}"
            final_configs[gname] = {
                'cpus': ",".join(map(str, info['cpus'])),
                'mems': str(NUMA_NODE),
                'functions': info['funcs']
            }
    else:
        print(f"[MODE] BASELINE MODE. Assigning ALL reference CPUs to current groups.")
        for gid in sorted(current_groups.keys()):
            gname = f"group_{gid}"
            final_configs[gname] = {
                'cpus': ref_cpu_str,
                'mems': str(NUMA_NODE),
                'functions': current_groups[gid]
            }
    
    # 打印配置摘要
    for gname, config in final_configs.items():
        print(f"   > {gname}: {len(config['functions'])} funcs, CPUs: {config['cpus']}")
    
    return final_configs

def build_func_to_group_mapping(cgroup_configs):
    """构建函数到分组的映射"""
    func_to_group = {}
    for group_name, config in cgroup_configs.items():
        if group_name.startswith('group_'):
            group_id = int(group_name.split('_')[1])
            for func in config['functions']:
                func_to_group[func] = group_id
    return func_to_group

def get_cgroup_for_function(func_name, cgroup_configs, func_to_group):
    """根据函数名获取对应的配置"""
    if func_name in func_to_group:
        group_name = f"group_{func_to_group[func_name]}"
        if group_name in cgroup_configs:
            return cgroup_configs[group_name]
    # 如果找不到, 返回default cgroup；若无default，则使用最后的备选方案
    return cgroup_configs.get('default', {'cpus': '0', 'mems': '0'})