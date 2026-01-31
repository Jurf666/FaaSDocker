"""
CgroupManager - 负责基于任务分组文件生成 cgroup 配置
"""
import os
import json
import math
from collections import defaultdict


class CgroupManager:
    """管理 cgroup 配置生成和查询"""
    
    def __init__(self, task_groups_file, numa_node=0):
        """
        初始化 CgroupManager
        
        Args:
            task_groups_file: 任务分组配置文件路径
            numa_node: NUMA 节点号 (0 或 1)
        """
        self.task_groups_file = task_groups_file
        self.numa_node = numa_node
        self.cgroup_configs = {}
        self.func_to_group = {}
        
    def generate_configs(self):
        """
        基于 task_groups.json 生成 cgroup 配置
        
        Returns:
            dict: cgroup 配置字典
        """
        if not os.path.exists(self.task_groups_file):
            print(f"[WARN] {self.task_groups_file} not found, using default cgroup")
            self.cgroup_configs = {
                'default': {
                    'cpus': '0',
                    'mems': str(self.numa_node)
                }
            }
            return self.cgroup_configs
        
        with open(self.task_groups_file, 'r') as f:
            task_groups = json.load(f)
        
        # 按分组号将函数分组
        groups = defaultdict(list)
        for func_name, group_id in task_groups.items():
            groups[group_id].append(func_name)
        
        # 生成 NUMA 节点对应的 CPU 成对列表
        cpu_pairs = self._generate_cpu_pairs()
        all_cpus = self._flatten_cpu_pairs(cpu_pairs)
        
        cpu_idx = 0
        configs = {}
        
        for group_id in sorted(groups.keys()):
            funcs_in_group = groups[group_id]
            total_clients = len(funcs_in_group) * 2  # 每个函数 2 个 client
            
            # 判断是否为 baseline 实验
            is_baseline = (len(groups) == 1) or ('baseline' in self.task_groups_file)
            
            if is_baseline:
                print(f"[INFO] Baseline Detected (Group {group_id}): "
                      f"Allocating ALL 54 CPUs to reduce contention.")
                cpus_needed = 54
            else:
                # 非 baseline: 计算所需 CPU 核数
                cpus_needed = math.ceil(total_clients / 5.0)
                if cpus_needed % 2 != 0:
                    cpus_needed += 1
            
            # 分配 CPU
            cpus_list = []
            while len(cpus_list) < cpus_needed and cpu_idx < len(all_cpus):
                cpus_list.append(all_cpus[cpu_idx])
                cpu_idx += 1
            
            if cpus_list:
                cpus_str = ','.join(map(str, cpus_list))
                group_name = f"group_{group_id}"
                configs[group_name] = {
                    'cpus': cpus_str,
                    'mems': str(self.numa_node),
                    'functions': funcs_in_group
                }
                print(f"[INFO] Group {group_id}: {len(funcs_in_group)} functions, "
                      f"{total_clients} total clients, {cpus_needed} CPUs needed: {cpus_str}")
        
        self.cgroup_configs = configs
        self._build_func_to_group_mapping()
        return self.cgroup_configs
    
    def _generate_cpu_pairs(self):
        """生成 NUMA 节点对应的 CPU 成对列表"""
        if self.numa_node == 0:
            return [(i, i + 64) for i in range(0, 64, 2)]
        else:
            return [(i, i + 64) for i in range(1, 64, 2)]
    
    def _flatten_cpu_pairs(self, cpu_pairs):
        """展开 CPU 对为扁平列表"""
        all_cpus = []
        for a, b in cpu_pairs:
            all_cpus.extend([a, b])
        return all_cpus
    
    def _build_func_to_group_mapping(self):
        """构建函数到分组的映射"""
        for group_name, group_config in self.cgroup_configs.items():
            if 'functions' in group_config:
                for func_name in group_config['functions']:
                    if group_name.startswith('group_'):
                        group_id = int(group_name.split('_')[1])
                        self.func_to_group[func_name] = group_id
    
    def get_cgroup_for_function(self, func_name):
        """
        根据函数名获取对应的 cgroup 配置
        
        Args:
            func_name: 函数名
            
        Returns:
            dict: cgroup 配置
        """
        if func_name in self.func_to_group:
            group_id = self.func_to_group[func_name]
            group_name = f"group_{group_id}"
            if group_name in self.cgroup_configs:
                return self.cgroup_configs[group_name]
        
        if 'default' in self.cgroup_configs:
            return self.cgroup_configs['default']
        
        return {'cpus': '0', 'mems': '0'}
    
    def get_configs(self):
        """获取所有 cgroup 配置"""
        return self.cgroup_configs
    
    def get_func_to_group_mapping(self):
        """获取函数到分组的映射"""
        return self.func_to_group
