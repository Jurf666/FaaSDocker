"""
闭环性能测试实验脚本 (重构版)
使用面向对象的方式组织代码，将不同功能模块分离到独立文件
"""
import os
import json
from collections import defaultdict

# 导入自定义模块
from cgroup_manager import CgroupManager
from workflow_warmer import WorkflowWarmer
from system_monitor import SystemMonitor
from data_cleaner import DataCleaner
from experiment_client import ExperimentClient

# -------------------------------------------------------------------------
# 全局配置参数
# -------------------------------------------------------------------------
CONTROLLER_HOST = os.environ.get('CONTROLLER_HOST', 'localhost')
CONTROLLER_PORT = os.environ.get('CONTROLLER_PORT', '5001')
CONTROLLER_URL = f"http://{CONTROLLER_HOST}:{CONTROLLER_PORT}"

TEST_DURATION = int(os.environ.get('TEST_DURATION', '300'))
RANDOM_SEED = int(os.environ.get('RANDOM_SEED', '42'))
NUMA_NODE = int(os.environ.get('NUMA_NODE', '0'))
TASK_GROUPS_FILE = os.environ.get('TASK_GROUPS_FILE', 'task_groups.json')
CLIENTS_PER_FUNCTION = int(os.environ.get('CLIENTS_PER_FUNCTION', '4'))

REDIS_HOST = os.environ.get('REDIS_HOST', '172.17.0.1')
REDIS_PORT = int(os.environ.get('REDIS_PORT', '6379'))
COUCHDB_URL = os.environ.get('COUCHDB_URL', 'http://openwhisk:openwhisk@172.17.0.1:5984/')

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


class ClosedLoopExperiment:
    """闭环性能测试实验主类"""
    
    def __init__(self):
        """初始化实验"""
        print(f"=== Closed-Loop Performance Test ===")
        print(f"Test Duration: {TEST_DURATION}s")
        print(f"NUMA Node: {NUMA_NODE}")
        print(f"Random Seed: {RANDOM_SEED}")
        print()
        
        # 初始化各个管理器
        self.cgroup_manager = CgroupManager(TASK_GROUPS_FILE, NUMA_NODE)
        self.workflow_warmer = WorkflowWarmer(REDIS_HOST, REDIS_PORT, CONTROLLER_URL)
        self.experiment_client = ExperimentClient(CONTROLLER_URL)
        self.data_cleaner = DataCleaner(REDIS_HOST, REDIS_PORT, COUCHDB_URL)
        self.system_monitor = None
        self.num_clients_created = 0  # 记录实际创建的 client 总数
        
    def setup(self):
        """设置实验环境"""
        # 1. 生成 cgroup 配置
        print(f"[INFO] Generating cgroup configurations from {TASK_GROUPS_FILE}...")
        cgroup_configs = self.cgroup_manager.generate_configs()
        func_to_group = self.cgroup_manager.get_func_to_group_mapping()
        
        # 2. 初始化 Controller 上的 Function Managers
        self.experiment_client.init_controller_managers(cgroup_configs, func_to_group)
        
        # 3. 预热工作流缓存
        workflow_cached_payloads = self.workflow_warmer.warmup_all_workflows(
            self.experiment_client.dispatch_simple
        )
        
        # 4. 准备监控器
        self.system_monitor = SystemMonitor(cgroup_configs, "system_metrics.csv")
        
        return func_to_group, workflow_cached_payloads
    
    def prepare_client_configs(self, func_to_group, workflow_cached_payloads):
        """
        准备客户端配置
        
        Args:
            func_to_group: 函数到分组的映射
            workflow_cached_payloads: 工作流缓存的 payloads
            
        Returns:
            list: 客户端配置列表 [(func_name, payload), ...]
        """
        client_configs = []
        mapped_funcs = set(func_to_group.keys())
        
        for func_name in sorted(mapped_funcs):
            if func_name in SIMPLE_ACTIONS:
                payload_src = SIMPLE_ACTIONS[func_name]
            elif func_name in workflow_cached_payloads:
                payload_src = workflow_cached_payloads[func_name]
            else:
                payload_src = {}
            
            # 每个函数创建固定数量 client
            for _ in range(CLIENTS_PER_FUNCTION):
                payload_copy = payload_src.copy() if isinstance(payload_src, dict) else {}
                client_configs.append((func_name, payload_copy))
        
        # 统计信息
        num_clients = len(client_configs)
        simple_count = len([c for c in client_configs if c[0] in SIMPLE_ACTIONS])
        workflow_count = num_clients - simple_count
        
        self.num_clients_created = num_clients  # 保存实际创建的 client 总数
        
        print(f"[INFO] Launching {num_clients} clients:")
        print(f"       - {simple_count} simple function clients")
        print(f"       - {workflow_count} workflow subfunction clients")
        
        return client_configs
    
    def run(self):
        """运行实验"""
        # 设置实验环境
        func_to_group, workflow_cached_payloads = self.setup()
        
        # 准备客户端配置
        client_configs = self.prepare_client_configs(func_to_group, workflow_cached_payloads)
        
        # 启动监控
        print(f"\n[INFO] Starting {len(client_configs)} client threads "
              f"(closed-loop, fixed duration {TEST_DURATION}s)...")
        self.system_monitor.start()
        
        # 运行实验
        summary = self.experiment_client.run_experiment(client_configs, TEST_DURATION)
        
        # 停止监控
        self.system_monitor.stop()
        
        # 计算统计数据
        stats = self.experiment_client.compute_statistics()
        
        return summary, stats
    
    def save_results(self, summary, stats):
        """
        保存实验结果
        
        Args:
            summary: 实验摘要
            stats: 统计数据
        """
        output = {
            "config": {
                "test_duration": TEST_DURATION,
                "num_clients": self.num_clients_created,  # 使用实际创建的 client 总数
                "numa_node": NUMA_NODE,
                "random_seed": RANDOM_SEED,
                "test_mode": "closed_loop"
            },
            "summary": summary,
            "statistics": stats
        }
        
        # 生成结果文件名
        base_name = os.path.splitext(TASK_GROUPS_FILE)[0]
        os.makedirs('closed_loop_results', exist_ok=True)
        output_file = os.path.join(
            'closed_loop_results',
            f"{base_name}_results_{CLIENTS_PER_FUNCTION}clients.json"
        )
        
        with open(output_file, 'w') as f:
            json.dump(output, f, indent=2)
        
        print(f"\n[INFO] Results saved to {output_file}")
    
    def cleanup(self):
        """清理实验产生的中间数据"""
        # 清理工作流数据
        self.data_cleaner.cleanup_all()
        # 关闭 HTTP 连接
        self.experiment_client.close()


def main():
    """主函数"""
    # 创建并运行实验
    experiment = ClosedLoopExperiment()
    
    try:
        # 运行实验
        summary, stats = experiment.run()
        
        # 保存结果
        experiment.save_results(summary, stats)
        
    finally:
        # 清理数据
        experiment.cleanup()


if __name__ == '__main__':
    main()
