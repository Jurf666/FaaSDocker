#!/bin/bash

# 完整对比脚本：运行所有4个配置并生成综合对比报告
# baseline: 1个大组，128条CPU，允许跨核，配置逻辑核
# task_groups: 27个组，不允许跨核，配置逻辑核  
# baseline2: 6个随机组，不允许跨核，配置逻辑核
# baseline3: 6个随机组，不允许跨核，只配置物理核

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "════════════════════════════════════════════════════════════════"
echo " Complete Comparative Performance Analysis"
echo " 4 Configurations comparison"
echo "════════════════════════════════════════════════════════════════"

mkdir -p comparison_results

configs=(
    "baseline|baseline_groups.json|1 big group, 128 CPUs, cross-core allowed, logical cores"
    "task_groups|task_groups.json|27 groups, cross-core not allowed, logical cores"
    "baseline2|baseline2_groups.json|6 random groups, cross-core not allowed, logical cores"
    "baseline3|baseline3_groups.json|6 random groups, cross-core not allowed, physical cores only"
)

results_dir="comparison_results"
declare -A all_results

for config in "${configs[@]}"; do
    IFS='|' read -r name file desc <<< "$config"
    
    echo ""
    echo "════════════════════════════════════════════════════════════════"
    echo "Running: $name"
    echo "Config: $file"
    echo "Description: $desc"
    echo "════════════════════════════════════════════════════════════════"
    
    # ⚠️ CRITICAL: Clean up Docker containers before each experiment
    #    /reset_controller only stops containers but doesn't remove them
    #    We need to actually DELETE them to avoid cgroup pollution
    echo "  [*] Cleaning up Docker containers..."
    docker rm -f $(docker ps -a -q --filter "name=.*-[a-f0-9]" 2>/dev/null) 2>/dev/null || true
    sleep 2
    
    # ⚠️ Then reset Controller state
    echo "  [*] Resetting Controller state..."
    python3 << 'RESETPY'
import requests
import time

try:
    resp = requests.post("http://127.0.0.1:5001/reset_controller", timeout=10)
    if resp.status_code == 200:
        data = resp.json()
        print(f"  [✓] Controller reset complete: "
              f"cleared {data.get('managers_cleared', 0)} managers, "
              f"{data.get('tasks_cleared', 0)} tasks")
    else:
        print(f"  [!] Reset failed: {resp.status_code}")
except Exception as e:
    print(f"  [!] Reset error: {e}")

time.sleep(2)  # Wait for cleanup to complete
RESETPY
    
    export TASK_GROUPS_FILE="$file"
    export TEST_DURATION=300
    
    python3 run_experiment_closed_loop_refactored.py
    
    # 复制结果
    src_file="closed_loop_results/${file%.*}_results_4clients.json"
    dst_file="$results_dir/result_${name}.json"
    
    if [ -f "$src_file" ]; then
        cp "$src_file" "$dst_file"
        echo "✓ Saved to $dst_file"
        all_results[$name]="$dst_file"
    else
        echo "⚠ Result file not found: $src_file"
    fi
    
    # 休息5秒
    if [ "$config" != "${configs[-1]}" ]; then
        echo "Waiting 5 seconds before next experiment..."
        sleep 5
    fi
done

echo ""
echo "════════════════════════════════════════════════════════════════"
echo " Generating comprehensive comparison report..."
echo "════════════════════════════════════════════════════════════════"

python3 << 'PYEOF'
import json
import os
from collections import defaultdict

# 加载所有结果
configs = ["baseline", "task_groups", "baseline2", "baseline3"]
results_dir = "comparison_results"
all_data = {}

for config_name in configs:
    fpath = os.path.join(results_dir, f"result_{config_name}.json")
    if os.path.exists(fpath):
        with open(fpath) as f:
            all_data[config_name] = json.load(f)
    else:
        print(f"❌ Missing: {fpath}")
        exit(1)

# 生成对比报告
report = {
    "timestamp": os.popen("date '+%Y-%m-%d %H:%M:%S'").read().strip(),
    "configurations": {},
    "function_comparisons": {},
    "summary_insights": []
}

# 配置信息
config_descriptions = {
    "baseline": "1 big group, 128 CPUs, cross-core allowed, logical cores",
    "task_groups": "27 groups, cross-core not allowed, logical cores",
    "baseline2": "6 random groups, cross-core not allowed, logical cores",
    "baseline3": "6 random groups, cross-core not allowed, physical cores only"
}

for config_name in configs:
    data = all_data[config_name]
    cfg = data.get("config", {})
    summary = data.get("summary", {})
    
    report["configurations"][config_name] = {
        "description": config_descriptions[config_name],
        "num_clients": cfg.get("num_clients", 0),
        "total_completed": summary.get("total_completed", 0),
        "throughput": summary.get("throughput", 0)
    }

# 关键函数对比
target_functions = ["matmul", "svd_compute", "linpack", "k-means"]

for func_name in target_functions:
    func_data = {}
    
    for config_name in configs:
        stats = all_data[config_name].get("statistics", {}).get(func_name, {})
        if stats:
            func_data[config_name] = {
                "count": stats.get("count", 0),
                "mean": stats.get("mean", 0),
                "std": stats.get("std", 0),
                "min": stats.get("min", 0),
                "max": stats.get("max", 0),
                "p90": stats.get("p90", 0),
                "p95": stats.get("p95", 0)
            }
    
    if func_data:
        report["function_comparisons"][func_name] = func_data

# 生成洞察
baseline_svd = all_data["baseline"].get("statistics", {}).get("svd_compute", {}).get("mean", 0)
task_svd = all_data["task_groups"].get("statistics", {}).get("svd_compute", {}).get("mean", 0)

if baseline_svd > 0 and task_svd > 0:
    improvement = ((baseline_svd - task_svd) / baseline_svd) * 100
    report["summary_insights"].append(
        f"Task isolation (task_groups) improves svd_compute by {improvement:.1f}% compared to baseline"
    )

# 保存 JSON 报告
report_file = os.path.join(results_dir, "comprehensive_comparison.json")
with open(report_file, 'w') as f:
    json.dump(report, f, indent=2)
print(f"✓ JSON report saved to: {report_file}")

# 生成可读的文本报告
text_report = os.path.join(results_dir, "comprehensive_comparison.txt")
with open(text_report, 'w') as f:
    f.write("=" * 110 + "\n")
    f.write(" COMPREHENSIVE PERFORMANCE COMPARISON REPORT\n")
    f.write("=" * 110 + "\n\n")
    
    f.write("CONFIGURATIONS:\n")
    f.write("-" * 110 + "\n")
    for config_name in configs:
        cfg = report["configurations"][config_name]
        f.write(f"\n{config_name}:\n")
        f.write(f"  Description: {cfg['description']}\n")
        f.write(f"  Clients: {cfg['num_clients']}\n")
        f.write(f"  Total Completed: {cfg['total_completed']}\n")
        f.write(f"  Throughput: {cfg['throughput']:.2f} ops/sec\n")
    
    f.write("\n" + "=" * 110 + "\n")
    f.write("FUNCTION PERFORMANCE COMPARISON:\n")
    f.write("=" * 110 + "\n")
    
    for func_name in target_functions:
        if func_name not in report["function_comparisons"]:
            continue
        
        f.write(f"\n{func_name.upper()}\n")
        f.write("-" * 110 + "\n")
        f.write(f"{'Config':<15} {'Count':<8} {'Mean(s)':<12} {'Std':<12} {'P90':<12} {'P95':<12} {'vs Baseline':<15}\n")
        f.write("-" * 110 + "\n")
        
        func_data = report["function_comparisons"][func_name]
        baseline_mean = func_data.get("baseline", {}).get("mean", 0)
        
        for config_name in configs:
            if config_name in func_data:
                stats = func_data[config_name]
                mean = stats["mean"]
                
                if baseline_mean > 0 and config_name != "baseline":
                    delta = ((mean - baseline_mean) / baseline_mean) * 100
                    vs_base = f"{delta:+.1f}%"
                else:
                    vs_base = "baseline"
                
                f.write(f"{config_name:<15} {stats['count']:<8} {mean:<12.4f} "
                       f"{stats['std']:<12.4f} {stats['p90']:<12.4f} {stats['p95']:<12.4f} {vs_base:<15}\n")
    
    f.write("\n" + "=" * 110 + "\n")
    f.write("KEY INSIGHTS:\n")
    f.write("-" * 110 + "\n")
    for insight in report["summary_insights"]:
        f.write(f"• {insight}\n")
    f.write("=" * 110 + "\n")

print(f"✓ Text report saved to: {text_report}")

# 打印到屏幕
with open(text_report) as f:
    print("\n" + f.read())

PYEOF

echo ""
echo "════════════════════════════════════════════════════════════════"
echo " ✓ All experiments complete!"
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "Result files in $results_dir:"
ls -lh comparison_results/result_*.json
echo ""
echo "Reports:"
ls -lh comparison_results/comprehensive_comparison.*
