"""
多配置对比实验脚本 - 用于分析 matmul 和 svd_compute 性能差异
运行四种配置：
1. baseline组：所有函数一个大组，可以跨核，配置了逻辑核
2. task_groups组：每个函数一个组，不可以跨核，配置了逻辑核
3. baseline2组：随机函数一个组，不可以跨核，配置逻辑核
4. baseline3组：随机函数一个组，不可以跨核，只配置物理核
"""
import os
import json
import time
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

# 实验配置列表
EXPERIMENT_CONFIGS = [
    {
        "name": "baseline",
        "file": "baseline_groups.json",
        "physical_cores_only": False,
        "description": "所有函数一个大组，可跨核，配置逻辑核"
    },
    {
        "name": "task_groups",
        "file": "task_groups.json",
        "physical_cores_only": False,
        "description": "每个函数一个组，不可跨核，配置逻辑核"
    },
    {
        "name": "baseline2",
        "file": "baseline2_groups.json",
        "physical_cores_only": False,
        "description": "随机函数一个组，不可跨核，配置逻辑核"
    },
    {
        "name": "baseline3",
        "file": "baseline3_groups.json",
        "physical_cores_only": True,
        "description": "随机函数一个组，不可跨核，只配置物理核"
    }
]


class ComparativeExperiment:
    """对比实验类"""
    
    def __init__(self):
        """初始化实验"""
        print("=" * 80)
        print(" 多配置对比实验 - 分析 matmul 和 svd_compute 性能差异")
        print("=" * 80)
        print(f"Test Duration: {TEST_DURATION}s")
        print(f"NUMA Node: {NUMA_NODE}")
        print(f"Random Seed: {RANDOM_SEED}")
        print()
        
        self.workflow_warmer = WorkflowWarmer(REDIS_HOST, REDIS_PORT, CONTROLLER_URL)
        self.data_cleaner = DataCleaner(REDIS_HOST, REDIS_PORT, COUCHDB_URL)
        self.all_results = {}
        
    def run_single_experiment(self, config):
        """
        运行单个配置的实验
        
        Args:
            config: 实验配置字典
            
        Returns:
            dict: 实验结果
        """
        print("\n" + "=" * 80)
        print(f" 实验: {config['name']}")
        print(f" 配置文件: {config['file']}")
        print(f" 描述: {config['description']}")
        print("=" * 80)
        
        # 1. 初始化 CgroupManager
        cgroup_manager = CgroupManager(
            config['file'], 
            NUMA_NODE, 
            physical_cores_only=config['physical_cores_only']
        )
        
        # 2. 生成 cgroup 配置
        print(f"\n[INFO] Generating cgroup configurations from {config['file']}...")
        cgroup_configs = cgroup_manager.generate_configs()
        func_to_group = cgroup_manager.get_func_to_group_mapping()
        
        # 3. 初始化 ExperimentClient 并清空 Controller 状态
        experiment_client = ExperimentClient(CONTROLLER_URL)
        
        # ⚠️ 关键：彻底重置 Controller 状态，包括：
        # - 清空函数管理器
        # - 清空任务状态字典
        # - 清理 Redis 中的临时数据
        try:
            print(f"\n[INFO] Resetting Controller state (comprehensive cleanup)...")
            resp = experiment_client.session.post(
                f"{CONTROLLER_URL}/reset_controller",
                timeout=10
            )
            if resp.status_code == 200:
                result = resp.json()
                print(f"[INFO] Controller reset complete:")
                print(f"       - Managers cleared: {result.get('managers_cleared', 0)}")
                print(f"       - Tasks cleared: {result.get('tasks_cleared', 0)}")
            else:
                print(f"[WARN] Failed to reset Controller: {resp.text}")
        except Exception as e:
            print(f"[WARN] Error resetting Controller: {e}")
        
        # 等待一秒，确保清理完成
        time.sleep(1)
        
        experiment_client.init_controller_managers(cgroup_configs, func_to_group)
        
        # 4. 预热工作流缓存（每次实验都需要预热，因为 Controller 被重新初始化）
        print(f"\n[INFO] Warming up workflow caches...")
        workflow_cached_payloads = self.workflow_warmer.warmup_all_workflows(
            experiment_client.dispatch_simple
        )
        
        # 5. 准备客户端配置
        client_configs = self._prepare_client_configs(func_to_group, workflow_cached_payloads)
        
        # 6. 准备监控器
        monitor_file = f"system_metrics_{config['name']}.csv"
        system_monitor = SystemMonitor(cgroup_configs, monitor_file)
        
        # 7. 运行实验
        print(f"\n[INFO] Starting {len(client_configs)} client threads...")
        system_monitor.start()
        
        start_time = time.time()
        summary = experiment_client.run_experiment(client_configs, TEST_DURATION)
        elapsed_time = time.time() - start_time
        
        system_monitor.stop()
        
        # 8. 计算统计数据
        stats = experiment_client.compute_statistics()
        
        # 9. 清理
        experiment_client.close()
        
        # 10. 保存结果
        result = {
            "config": {
                "name": config['name'],
                "file": config['file'],
                "description": config['description'],
                "test_duration": TEST_DURATION,
                "num_clients": len(client_configs),
                "numa_node": NUMA_NODE,
                "random_seed": RANDOM_SEED,
                "physical_cores_only": config['physical_cores_only'],
                "test_mode": "closed_loop"
            },
            "summary": summary,
            "statistics": stats,
            "elapsed_time": elapsed_time
        }
        
        # 保存单个实验结果
        os.makedirs('comparison_results', exist_ok=True)
        output_file = os.path.join('comparison_results', f"{config['name']}_results.json")
        with open(output_file, 'w') as f:
            json.dump(result, f, indent=2)
        
        print(f"\n[INFO] Results saved to {output_file}")
        
        # 清理数据，为下一个实验做准备
        print(f"\n[INFO] Cleaning up data...")
        self.data_cleaner.cleanup_all()
        
        return result
    
    def _prepare_client_configs(self, func_to_group, workflow_cached_payloads):
        """准备客户端配置"""
        client_configs = []
        mapped_funcs = set(func_to_group.keys())
        
        for func_name in sorted(mapped_funcs):
            if func_name in SIMPLE_ACTIONS:
                payload_src = SIMPLE_ACTIONS[func_name]
            elif func_name in workflow_cached_payloads:
                payload_src = workflow_cached_payloads[func_name]
            else:
                payload_src = {}
            
            # 每个函数创建 4 个 client
            for _ in range(4):
                payload_copy = payload_src.copy() if isinstance(payload_src, dict) else {}
                client_configs.append((func_name, payload_copy))
        
        return client_configs
    
    def run_all_experiments(self):
        """运行所有配置的实验"""
        print("\n开始运行所有对比实验...")
        
        for config in EXPERIMENT_CONFIGS:
            result = self.run_single_experiment(config)
            self.all_results[config['name']] = result
            
            # 每个实验之间休息30秒
            if config != EXPERIMENT_CONFIGS[-1]:
                print(f"\n[INFO] Waiting 30 seconds before next experiment...")
                time.sleep(30)
        
        print("\n" + "=" * 80)
        print(" 所有实验完成！")
        print("=" * 80)
    
    def generate_comparison_report(self):
        """生成对比报告，重点关注 matmul 和 svd_compute"""
        print("\n" + "=" * 80)
        print(" 性能对比报告")
        print("=" * 80)
        
        # 关注的函数
        target_funcs = ['matmul', 'svd_compute']
        
        report = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "test_configurations": {},
            "performance_comparison": {},
            "summary": {}
        }
        
        # 1. 收集所有配置信息
        for config_name, result in self.all_results.items():
            report["test_configurations"][config_name] = {
                "description": result["config"]["description"],
                "file": result["config"]["file"],
                "physical_cores_only": result["config"]["physical_cores_only"],
                "num_clients": result["config"]["num_clients"],
                "total_completed": result["summary"]["total_completed"],
                "throughput": result["summary"]["throughput"]
            }
        
        # 2. 对比目标函数的性能
        for func_name in target_funcs:
            report["performance_comparison"][func_name] = {}
            
            for config_name, result in self.all_results.items():
                if func_name in result["statistics"]:
                    stats = result["statistics"][func_name]
                    report["performance_comparison"][func_name][config_name] = {
                        "count": stats["count"],
                        "mean": stats["mean"],
                        "std": stats["std"],
                        "min": stats["min"],
                        "max": stats["max"],
                        "p90": stats["p90"],
                        "p95": stats["p95"]
                    }
        
        # 3. 生成摘要和分析
        print("\n重点函数性能对比:")
        print("-" * 80)
        
        for func_name in target_funcs:
            if func_name in report["performance_comparison"]:
                print(f"\n函数: {func_name}")
                print(f"{'配置':<15} {'完成数':<10} {'平均耗时(s)':<15} {'标准差':<12} {'P90(s)':<12}")
                print("-" * 80)
                
                func_stats = report["performance_comparison"][func_name]
                baseline_mean = func_stats.get('baseline', {}).get('mean', 0)
                
                for config_name in ['baseline', 'task_groups', 'baseline2', 'baseline3']:
                    if config_name in func_stats:
                        stats = func_stats[config_name]
                        mean = stats['mean']
                        
                        # 计算相对于 baseline 的差异
                        if baseline_mean > 0 and config_name != 'baseline':
                            diff_pct = ((mean - baseline_mean) / baseline_mean) * 100
                            diff_str = f"({diff_pct:+.1f}%)"
                        else:
                            diff_str = ""
                        
                        print(f"{config_name:<15} {stats['count']:<10} "
                              f"{mean:<15.4f} {stats['std']:<12.4f} "
                              f"{stats['p90']:<12.4f} {diff_str}")
        
        # 4. 保存完整报告
        report_file = os.path.join('comparison_results', 'performance_comparison_report.json')
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"\n[INFO] 完整对比报告已保存到: {report_file}")
        
        # 5. 生成可读的markdown报告
        self._generate_markdown_report(report)
        
        return report
    
    def _generate_markdown_report(self, report):
        """生成markdown格式的报告"""
        md_file = os.path.join('comparison_results', 'performance_comparison_report.md')
        
        with open(md_file, 'w', encoding='utf-8') as f:
            f.write("# 性能对比实验报告\n\n")
            f.write(f"**生成时间**: {report['timestamp']}\n\n")
            
            f.write("## 实验配置\n\n")
            f.write("| 配置 | 描述 | 物理核 | 客户端数 | 完成总数 | 吞吐量 |\n")
            f.write("|------|------|---------|----------|----------|--------|\n")
            
            for config_name, config_info in report["test_configurations"].items():
                f.write(f"| {config_name} | {config_info['description']} | "
                       f"{'是' if config_info['physical_cores_only'] else '否'} | "
                       f"{config_info['num_clients']} | "
                       f"{config_info['total_completed']} | "
                       f"{config_info['throughput']:.2f} |\n")
            
            f.write("\n## 关键函数性能对比\n\n")
            
            for func_name, func_data in report["performance_comparison"].items():
                f.write(f"### {func_name}\n\n")
                f.write("| 配置 | 完成数 | 平均耗时(s) | 标准差 | 最小值 | 最大值 | P90 | P95 |\n")
                f.write("|------|--------|-------------|--------|--------|--------|-----|-----|\n")
                
                for config_name in ['baseline', 'task_groups', 'baseline2', 'baseline3']:
                    if config_name in func_data:
                        stats = func_data[config_name]
                        f.write(f"| {config_name} | {stats['count']} | "
                               f"{stats['mean']:.4f} | {stats['std']:.4f} | "
                               f"{stats['min']:.4f} | {stats['max']:.4f} | "
                               f"{stats['p90']:.4f} | {stats['p95']:.4f} |\n")
                
                # 添加分析
                baseline_mean = func_data.get('baseline', {}).get('mean', 0)
                task_mean = func_data.get('task_groups', {}).get('mean', 0)
                
                if baseline_mean > 0 and task_mean > 0:
                    improvement = ((baseline_mean - task_mean) / baseline_mean) * 100
                    f.write(f"\n**分析**: task_groups相比baseline性能提升了 {improvement:.1f}%\n\n")
        
        print(f"[INFO] Markdown报告已保存到: {md_file}")


def main():
    """主函数"""
    experiment = ComparativeExperiment()
    
    try:
        # 运行所有实验
        experiment.run_all_experiments()
        
        # 生成对比报告
        experiment.generate_comparison_report()
        
    except KeyboardInterrupt:
        print("\n\n[WARN] 实验被用户中断")
    except Exception as e:
        print(f"\n[ERROR] 实验过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # 确保清理
        experiment.data_cleaner.cleanup_all()


if __name__ == '__main__':
    main()
