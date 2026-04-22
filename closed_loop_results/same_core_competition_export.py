"""
same_core_competition_export.py
================================
读取 baseline_groups_same_core_overlaps.json 和 task_groups_same_core_overlaps.json，
统计每个函数的同核竞争情况，导出两个 CSV 文件。

统计字段：
  - total_executions      : 总调用次数
  - exclusive_executions  : 无同核竞争的调用次数
  - exclusive_ratio       : exclusive_executions / total_executions
  - contend_with_xxx      : 与函数 xxx 同核竞争的调用次数（每个其他函数一列）

用法（在 closed_loop_results/ 目录下运行）：
  python same_core_competition_export.py
"""

import json
import os
from collections import defaultdict

import pandas as pd


def load_overlaps(filepath: str) -> list:
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("per_invocation", [])


def compute_competition(invocations: list) -> pd.DataFrame:
    # {func: {other_func: count}}
    contend_counts: dict = defaultdict(lambda: defaultdict(int))
    total_counts:   dict = defaultdict(int)
    exclusive_counts: dict = defaultdict(int)

    for record in invocations:
        func = record.get("function_name")
        if not func:
            continue
        overlaps = record.get("overlapping_functions", [])
        total_counts[func] += 1
        if not overlaps:
            exclusive_counts[func] += 1
        else:
            for other in overlaps:
                contend_counts[func][other] += 1

    all_funcs = sorted(total_counts.keys())
    rows = []
    for func in all_funcs:
        row = {
            "function":            func,
            "total_executions":    total_counts[func],
            "exclusive_executions": exclusive_counts[func],
            "exclusive_ratio":     exclusive_counts[func] / total_counts[func]
                                   if total_counts[func] > 0 else 0.0,
        }
        for other in all_funcs:
            if other == func:
                continue
            row[f"contend_with_{other}"] = contend_counts[func].get(other, 0)
        rows.append(row)

    return pd.DataFrame(rows)


def process(overlaps_file: str, out_csv: str) -> None:
    if not os.path.exists(overlaps_file):
        print(f"[WARN] 找不到文件，跳过: {overlaps_file}")
        return
    print(f"正在处理: {overlaps_file} ...")
    invocations = load_overlaps(overlaps_file)
    df = compute_competition(invocations)
    df.to_csv(out_csv, index=False, float_format="%.4f", encoding="utf-8-sig")
    print(f"成功生成文件: {out_csv}")


if __name__ == "__main__":
    process(
        "baseline_groups_same_core_overlaps.json",
        "baseline_groups_same_core_competition_summary.csv",
    )
    process(
        "task_groups_same_core_overlaps.json",
        "task_groups_same_core_competition_summary.csv",
    )
