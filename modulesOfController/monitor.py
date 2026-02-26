import threading
import time
import csv
import psutil
import logging

logger = logging.getLogger("Monitor")

class SystemMonitor:
    def __init__(self):
        self.monitor_thread = None
        self.stop_event = threading.Event()

    def start(self, cgroup_configs, filename="server_metrics.csv"):
        # 停止旧线程
        self.stop()
        
         # 启动新线程
        self.stop_event.clear()
        self.monitor_thread = threading.Thread(
            target=self._monitor_logic,
            args=(cgroup_configs, filename)
        )
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        logger.info(f"Monitor started, writing to {filename}")

    def stop(self):
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.stop_event.set()
            self.monitor_thread.join()
            logger.info("Monitor stopped.")

    def _monitor_logic(self, cgroup_configs, filename):
        # 1. 解析 CPU 列表，保持与 Client 端一致的顺序
        ordered_cpu_list = []
        sorted_groups = sorted(cgroup_configs.keys(), key=lambda x: int(x.split('_')[1]) if '_' in x else x)
        
        for group_name in sorted_groups:
            config = cgroup_configs[group_name]
            # 网络传输过来可能是字符串，做一下防御性处理
            cpus_val = config.get('cpus', '')
            if isinstance(cpus_val, list):# 如果 JSON 自动解析成了 list
                cpu_ids = [int(x) for x in cpus_val]
            else:
                cpu_ids = [int(x) for x in str(cpus_val).split(',') if x.strip()]
            for cid in cpu_ids:
                ordered_cpu_list.append((cid, group_name))
        # 2. 写入 CSV
        try:
            with open(filename, 'w', newline='') as f:
                writer = csv.writer(f)
                headers = ["timestamp"]
                for cid, gname in ordered_cpu_list:
                    headers.append(f"CPU_{cid}({gname})") 
                writer.writerow(headers)
                
                last_time = time.time()
                while not self.stop_event.is_set():
                    current_time = time.time()
                    # 简单限流，1秒一次
                    if current_time - last_time < 1.0:
                        time.sleep(1.0 - (current_time - last_time))
                        current_time = time.time()
                    
                    row = [current_time]
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
            logger.error(f"[MONITOR] Error: {e}")