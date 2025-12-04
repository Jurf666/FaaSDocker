import os
import json
import numpy as np
from pathlib import Path

LOG_DIR = './storage/perf_logs'
OUTPUT_FILE = 'summary.json'

def load_and_aggregate():
    # 存储结构: { "action_name": [ {metrics1}, {metrics2}, ... ] }
    raw_data = {}
    
    files = list(Path(LOG_DIR).rglob("*_clean_*.json"))
    print(f"[-] Found {len(files)} log files.")

    for file_path in files:
        try:
            with open(file_path, 'r') as f:
                record = json.load(f)
            
            func_name = record.get('function', 'unknown')
            metrics = record.get('clean_metrics', {})
            
            # 过滤无效数据 (指令数太少说明没跑起来)
            if metrics.get('instructions', 0) < 10000:
                continue
                
            if func_name not in raw_data:
                raw_data[func_name] = []
            
            raw_data[func_name].append(metrics)
            
        except Exception as e:
            print(f"[!] Error processing {file_path}: {e}")

    # 计算平均值并提取核心特征
    summary = {}
    
    print("\n[-] Aggregating results...")
    for func, metrics_list in raw_data.items():
        count = len(metrics_list)
        if count == 0: continue
        
        # 1. 计算各项指标的平均值
        avg_metrics = {}
        # 取第一个 dict 的 keys
        keys = metrics_list[0].keys() 
        for k in keys:
            values = [m.get(k, 0) for m in metrics_list]
            avg_metrics[k] = np.mean(values)
            
        # 2. 基于平均值计算三大核心特征
        # IPC
        instr = avg_metrics.get('instructions', 1)
        cycles = avg_metrics.get('cycles', 1)
        ipc = instr / (cycles + 1e-5)
        
        # LLC MPKI (Misses Per Kilo Instructions)
        llc_miss = avg_metrics.get('LLC-load-misses', 0)
        llc_mpki = llc_miss / (instr / 1000.0 + 1e-5)
        
        # L1 MPKI
        l1_miss = avg_metrics.get('L1-dcache-load-misses', 0)
        l1_mpki = l1_miss / (instr / 1000.0 + 1e-5)
        
        summary[func] = {
            "sample_count": count,
            "ipc": float(f"{ipc:.4f}"),
            "llc_mpki": float(f"{llc_mpki:.4f}"),
            "l1_mpki": float(f"{l1_mpki:.4f}"),
            # 保留原始平均值备查
            "raw_avg": avg_metrics
        }
        
        print(f"  > {func:<20}: Samples={count}, IPC={ipc:.2f}, LLC_MPKI={llc_mpki:.2f}, L1_MPKI={l1_mpki:.2f}")

    # 保存
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(summary, f, indent=2)
    print(f"\n[+] Summary saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    load_and_aggregate()