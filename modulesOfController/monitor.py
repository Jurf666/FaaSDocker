import threading
import time
import csv
import os
import psutil
import logging

logger = logging.getLogger("Monitor")


def _read_cpu_freq_mhz(cpu_id):
    for fname in ("cpuinfo_cur_freq", "scaling_cur_freq"):
        path = f"/sys/devices/system/cpu/cpu{cpu_id}/cpufreq/{fname}"
        try:
            with open(path, "r") as f:
                return int(f.read().strip()) / 1000.0  # kHz → MHz
        except Exception:
            continue
    return None


def _read_max_cpu_temp_celsius():
    max_temp = None
    thermal_dir = "/sys/class/thermal"
    if not os.path.isdir(thermal_dir):
        return None
    for zone in os.listdir(thermal_dir):
        if not zone.startswith("thermal_zone"):
            continue
        try:
            with open(os.path.join(thermal_dir, zone, "temp")) as f:
                val = int(f.read().strip()) / 1000.0
            if max_temp is None or val > max_temp:
                max_temp = val
        except Exception:
            continue
    return max_temp


class SystemMonitor:
    def __init__(self):
        self.monitor_thread = None
        self.stop_event = threading.Event()

    def start(self, cgroup_configs, filename="server_metrics.csv"):
        self.stop()
        self.stop_event.clear()
        self.monitor_thread = threading.Thread(
            target=self._monitor_logic,
            args=(cgroup_configs, filename),
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
        sorted_groups = sorted(
            cgroup_configs.keys(),
            key=lambda x: int(x.split("_")[1]) if "_" in x else x,
        )
        for group_name in sorted_groups:
            config = cgroup_configs[group_name]
            cpus_val = config.get("cpus", "")
            if isinstance(cpus_val, list):
                cpu_ids = [int(x) for x in cpus_val]
            else:
                cpu_ids = [int(x) for x in str(cpus_val).split(",") if x.strip()]
            for cid in cpu_ids:
                ordered_cpu_list.append((cid, group_name))

        # 2. 写入 CSV
        # 每核两列：利用率(%) + 频率(MHz)，末尾两列：最高温度 + 平均频率
        try:
            with open(filename, "w", newline="") as f:
                writer = csv.writer(f)

                headers = ["timestamp"]
                for cid, gname in ordered_cpu_list:
                    headers.append(f"CPU_{cid}_util%({gname})")
                    headers.append(f"CPU_{cid}_freq_MHz({gname})")
                headers.append("max_cpu_temp_C")
                headers.append("avg_freq_MHz")
                writer.writerow(headers)

                last_time = time.time()
                while not self.stop_event.is_set():
                    current_time = time.time()
                    if current_time - last_time < 1.0:
                        time.sleep(1.0 - (current_time - last_time))
                        current_time = time.time()

                    row = [current_time]
                    all_cpus_util = psutil.cpu_percent(interval=None, percpu=True)

                    freqs = []
                    for cid, _ in ordered_cpu_list:
                        util = all_cpus_util[cid] if cid < len(all_cpus_util) else -1
                        row.append(util)
                        freq = _read_cpu_freq_mhz(cid)
                        row.append(round(freq, 1) if freq is not None else -1)
                        if freq is not None:
                            freqs.append(freq)

                    temp = _read_max_cpu_temp_celsius()
                    row.append(round(temp, 1) if temp is not None else -1)
                    avg_freq = round(sum(freqs) / len(freqs), 1) if freqs else -1
                    row.append(avg_freq)

                    writer.writerow(row)
                    f.flush()
                    last_time = current_time

        except Exception as e:
            logger.error(f"[MONITOR] Error: {e}")