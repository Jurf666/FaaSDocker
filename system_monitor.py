"""
SystemMonitor - 负责系统监控，记录 CPU 使用率等指标
"""
import time
import csv
import threading
import psutil


class SystemMonitor:
    """系统监控管理器"""
    
    def __init__(self, cgroup_configs, output_file="system_metrics.csv"):
        """
        初始化 SystemMonitor
        
        Args:
            cgroup_configs: cgroup 配置字典
            output_file: 输出 CSV 文件路径
        """
        self.cgroup_configs = cgroup_configs
        self.output_file = output_file
        self.stop_event = threading.Event()
        self.monitor_thread = None
        self.ordered_cpu_list = []
        
        self._prepare_cpu_list()
    
    def _prepare_cpu_list(self):
        """准备有序的 CPU 列表（按组排序）"""
        sorted_groups = sorted(
            self.cgroup_configs.keys(), 
            key=lambda x: int(x.split('_')[1]) if '_' in x else x
        )
        
        for group_name in sorted_groups:
            config = self.cgroup_configs[group_name]
            if 'cpus' in config:
                cpu_ids = [int(x) for x in config['cpus'].split(',') if x.strip()]
                for cid in cpu_ids:
                    self.ordered_cpu_list.append((cid, group_name))
    
    def start(self):
        """启动监控线程"""
        print(f"[MONITOR] Starting monitor, saving to {self.output_file}...")
        self.stop_event.clear()
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
    
    def stop(self):
        """停止监控线程"""
        self.stop_event.set()
        if self.monitor_thread:
            self.monitor_thread.join()
        print("[MONITOR] Monitoring stopped.")
    
    def _monitor_loop(self):
        """监控主循环"""
        with open(self.output_file, 'w', newline='') as f:
            writer = csv.writer(f)
            
            # 构建表头
            headers = ["timestamp"]
            for cid, gname in self.ordered_cpu_list:
                headers.append(f"CPU_{cid}({gname})")
            writer.writerow(headers)
            
            last_time = time.time()
            
            # 循环采样
            while not self.stop_event.is_set():
                try:
                    # 采样间隔控制
                    current_time = time.time()
                    time_delta = current_time - last_time
                    if time_delta < 1.0:
                        time.sleep(1.0 - time_delta)
                        current_time = time.time()
                        time_delta = current_time - last_time
                    
                    row = [current_time]
                    
                    # 获取 CPU 数据
                    all_cpus = psutil.cpu_percent(interval=None, percpu=True)
                    for cid, _ in self.ordered_cpu_list:
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
