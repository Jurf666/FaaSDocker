"""
读取同目录下两个文件并分别导出竞争统计 CSV：
1) baseline_groups_same_core_overlaps.json
2) task_groups_same_core_overlaps.json

输出（同目录）：
1) baseline_groups_same_core_competition_summary.csv
2) task_groups_same_core_competition_summary.csv

统计口径：
- total_executions：函数总执行次数
- exclusive_executions：独占物理核执行次数（co_running_functions 为空）
- contend_with_xxx：与 xxx 在同核发生争用的调用次数（包含自身）
"""

import csv
import json
from collections import defaultdict
from pathlib import Path


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def build_stats(overlap_data):
    per_invocation = overlap_data.get("per_invocation", [])

    all_funcs = set()
    for row in per_invocation:
        fn = row.get("function_name")
        if fn:
            all_funcs.add(fn)
        for peer in row.get("co_running_functions", []):
            pfn = peer.get("function_name")
            if pfn:
                all_funcs.add(pfn)
    all_funcs = sorted(all_funcs)

    total = defaultdict(int)
    exclusive = defaultdict(int)
    contend = defaultdict(lambda: defaultdict(int))  # contend[a][b]

    for row in per_invocation:
        fn = row.get("function_name")
        if not fn:
            continue

        total[fn] += 1
        peers = row.get("co_running_functions", []) or []

        if len(peers) == 0:
            exclusive[fn] += 1
            continue

        # 同一次调用中，同一函数只计 1 次争用
        peer_names = {p.get("function_name") for p in peers if p.get("function_name")}
        for pfn in peer_names:
            contend[fn][pfn] += 1

    rows = []
    for fn in all_funcs:
        t = total[fn]
        ex = exclusive[fn]
        row = {
            "function_name": fn,
            "total_executions": t,
            "exclusive_executions": ex,
            "non_exclusive_executions": t - ex,
            "exclusive_ratio": (ex / t) if t > 0 else 0.0,
        }
        for peer in all_funcs:
            row[f"contend_with_{peer}"] = contend[fn][peer]
        rows.append(row)

    return all_funcs, rows


def write_csv(path: Path, all_funcs, rows):
    headers = [
        "function_name",
        "total_executions",
        "exclusive_executions",
        "non_exclusive_executions",
        "exclusive_ratio",
    ] + [f"contend_with_{f}" for f in all_funcs]

    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def export_one(input_path: Path, output_path: Path):
    if not input_path.exists():
        print(f"[WARN] file not found, skipped: {input_path.name}")
        return
    data = load_json(input_path)
    all_funcs, rows = build_stats(data)
    write_csv(output_path, all_funcs, rows)
    print(f"[OK] CSV saved: {output_path.name} | functions={len(all_funcs)} rows={len(rows)}")


def main():
    current_dir = Path(__file__).resolve().parent

    baseline_in = current_dir / "baseline_groups_same_core_overlaps.json"
    task_in = current_dir / "task_groups_same_core_overlaps.json"

    baseline_out = current_dir / "baseline_groups_same_core_competition_summary.csv"
    task_out = current_dir / "task_groups_same_core_competition_summary.csv"

    export_one(baseline_in, baseline_out)
    export_one(task_in, task_out)


if __name__ == "__main__":
    main()

