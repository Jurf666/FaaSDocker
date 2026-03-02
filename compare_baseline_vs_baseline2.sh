#!/bin/bash

# 对比脚本：baseline_groups vs baseline2_groups
# baseline: 1个大组，128条CPU，允许跨核，配置逻辑核
# baseline2: 6个随机组，不允许跨核，配置逻辑核

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "================================================================"
echo "Comparing baseline_groups vs baseline2_groups"
echo "With Docker container cleanup between runs"
echo "================================================================"

mkdir -p comparison_results

# ==================== ROUND 1: baseline_groups ====================
echo ""
echo ">>> ROUND 1: baseline_groups configuration (1 big group, 128 CPUs)"
echo "========================================================================/"

# Clean up Docker containers first
echo "  [*] Cleaning up Docker containers..."
docker rm -f $(docker ps -a -q --filter "name=.*-[a-f0-9]" 2>/dev/null) 2>/dev/null || true
sleep 2

# Reset Controller state
echo "  [*] Resetting Controller state..."
python3 << 'RESETPY'
import requests, time
try:
    resp = requests.post("http://127.0.0.1:5001/reset_controller", timeout=10)
    if resp.status_code == 200:
        data = resp.json()
        print(f"  [✓] Controller reset: {data.get('managers_cleared', 0)} managers cleared")
    else:
        print(f"  [!] Reset failed: {resp.status_code}")
except Exception as e:
    print(f"  [!] Error: {e}")
time.sleep(2)
RESETPY

export TASK_GROUPS_FILE="baseline_groups.json"
export TEST_DURATION=300
python3 run_experiment_closed_loop_refactored.py

if [ -f "closed_loop_results/baseline_groups_results_4clients.json" ]; then
    cp closed_loop_results/baseline_groups_results_4clients.json comparison_results/baseline_groups_baseline2.json
    echo "✓ Saved"
fi

sleep 5
echo ""

# ==================== ROUND 2: baseline2_groups ====================
echo ">>> ROUND 2: baseline2_groups configuration (6 random groups, logical cores)"
echo "========================================================================/"

# Clean up Docker containers first
echo "  [*] Cleaning up Docker containers..."
docker rm -f $(docker ps -a -q --filter "name=.*-[a-f0-9]" 2>/dev/null) 2>/dev/null || true
sleep 2

# Reset Controller state
echo "  [*] Resetting Controller state..."
python3 << 'RESETPY'
import requests, time
try:
    resp = requests.post("http://127.0.0.1:5001/reset_controller", timeout=10)
    if resp.status_code == 200:
        data = resp.json()
        print(f"  [✓] Controller reset: {data.get('managers_cleared', 0)} managers cleared")
    else:
        print(f"  [!] Reset failed: {resp.status_code}")
except Exception as e:
    print(f"  [!] Error: {e}")
time.sleep(2)
RESETPY

export TASK_GROUPS_FILE="baseline2_groups.json"
export TEST_DURATION=300
python3 run_experiment_closed_loop_refactored.py

if [ -f "closed_loop_results/baseline2_groups_results_4clients.json" ]; then
    cp closed_loop_results/baseline2_groups_results_4clients.json comparison_results/baseline2_groups_baseline2.json
    echo "✓ Saved"
fi

echo ""
echo "✓ Comparison complete!"
