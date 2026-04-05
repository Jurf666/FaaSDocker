"""
jsonAnalyzeFiltered.py
======================
与 jsonAnalyze.py 功能相同，区别：从 *_raw_samples.json 读取原始样本，
先裁剪至 [p1, p99] 区间再重新计算所有统计量，消除极端离群值的影响。

用法（在 closed_loop_results/ 目录下运行）：
  python jsonAnalyzeFiltered.py
  python jsonAnalyzeFiltered.py --base baseline_groups_raw_samples.json \
                                 --exp  task_groups_raw_samples.json \
                                 --out  base-exp-filtered.csv
"""

import argparse
import json
from typing import Any, Dict, List

import numpy as np
import pandas as pd

# 与 jsonAnalyze.py 保持一致的白名单
TASK_METRIC_WHITELIST = [
    "count",
    "mean",
    "cv",
    #"iqr",
    "p90",
    "p95",
    "p99",
    "effective_cpu_time_s_mean",
    "container_cpu_time_s_mean",
    "process_cpu_time_s_mean",
    "cycle_time_s_mean",
    "cgroup_nr_periods_mean",
    "cgroup_nr_throttled_mean",
    "cgroup_throttled_time_s_mean",
    "cgroup_throttle_ratio_mean",
]

# raw_samples.json 中各 metric 的键名 → 对应 results.json 中的统计字段前缀
METRIC_KEYS = {
    "exec_wall_time_s":      None,            # 主指标，统计量直接作为顶层字段
    "effective_cpu_time_s":  "effective_cpu_time_s",
    "container_cpu_time_s":  "container_cpu_time_s",
    "process_cpu_time_s":    "process_cpu_time_s",
    "cycle_time_s":          "cycle_time_s",
    "cgroup_nr_periods":     "cgroup_nr_periods",
    "cgroup_nr_throttled":   "cgroup_nr_throttled",
    "cgroup_throttled_time_s": "cgroup_throttled_time_s",
    "cgroup_throttle_ratio": "cgroup_throttle_ratio",
}


def _clip_p1_p99(values: List[float]) -> np.ndarray:
    """裁剪至 [p1, p99] 区间，返回 numpy 数组。"""
    arr = np.array(values, dtype=float)
    if len(arr) < 2:
        return arr
    lo = np.percentile(arr, 1)
    hi = np.percentile(arr, 99)
    return arr[(arr >= lo) & (arr <= hi)]


def _compute_stats(arr: np.ndarray) -> Dict[str, Any]:
    """对裁剪后的数组计算与 jsonAnalyze 一致的统计量。"""
    if len(arr) == 0:
        return {}
    mean = float(np.mean(arr))
    std  = float(np.std(arr))
    return {
        "count": len(arr),
        "mean":  mean,
        "std":   std,
        "cv":    float(std / mean) if mean != 0 else 0.0,
        "min":   float(np.min(arr)),
        "max":   float(np.max(arr)),
        "iqr":   float(np.percentile(arr, 75) - np.percentile(arr, 25)),
        "p90":   float(np.percentile(arr, 90)),
        "p95":   float(np.percentile(arr, 95)),
        "p99":   float(np.percentile(arr, 99)),
    }


def build_statistics(raw_samples: Dict[str, Any]) -> Dict[str, Any]:
    """
    将 raw_samples.json 的内容转换为与 results.json["statistics"] 相同结构的字典。
    主指标（exec_wall_time_s）的统计量作为顶层字段，其余 metric 以 {metric}_mean 形式附加。
    """
    statistics: Dict[str, Any] = {}

    for func, metrics in raw_samples.items():
        # 主指标：wall-clock 时间
        wall_values = metrics.get("exec_wall_time_s", [])
        wall_clipped = _clip_p1_p99(wall_values)
        stat = _compute_stats(wall_clipped)
        if not stat:
            continue

        # 附加各 metric 的 mean（与 results.json 的 metric_means 对齐）
        for metric_key, label in METRIC_KEYS.items():
            if label is None:
                continue  # exec_wall_time_s 已在顶层处理
            values = metrics.get(metric_key, [])
            if not values:
                stat[f"{label}_mean"] = 0.0
                continue
            clipped = _clip_p1_p99(values)
            stat[f"{label}_mean"] = float(np.mean(clipped)) if len(clipped) > 0 else 0.0

        statistics[func] = stat

    return statistics


def flatten_statistics(statistics: Dict[str, Any], source_label: str) -> List[Dict[str, Any]]:
    """将 statistics 字典展平为 DataFrame 行，格式与 jsonAnalyze.flatten_json_data 一致。"""
    rows = []
    for task_name, metrics in statistics.items():
        row = {"Task": task_name, "Source": source_label, "Type": "Task"}
        row.update(metrics)
        rows.append(row)
    return rows


def _calc_change_percent(base_series: pd.Series, exp_series: pd.Series) -> pd.Series:
    base_num = pd.to_numeric(base_series, errors="coerce")
    exp_num  = pd.to_numeric(exp_series,  errors="coerce")
    change   = (exp_num - base_num) / base_num * 100
    change   = change.mask((base_num == 0) & (exp_num != 0), pd.NA)
    change   = change.mask((base_num == 0) & (exp_num == 0), 0.0)
    return change


def _collect_available_metrics(df_merged: pd.DataFrame) -> set:
    ignore = {"Source_Base"}
    return {
        c[:-5]
        for c in df_merged.columns
        if c.endswith("_Base") and c not in ignore and f"{c[:-5]}_Exp" in df_merged.columns
    }


def _build_final_columns(df_merged: pd.DataFrame) -> List[str]:
    available = _collect_available_metrics(df_merged)
    selected  = [m for m in TASK_METRIC_WHITELIST if m in available]

    final_columns = ["Task"]
    for metric in selected:
        base_col   = f"{metric}_Base"
        exp_col    = f"{metric}_Exp"
        change_col = f"{metric}_Change%"

        base_num = pd.to_numeric(df_merged[base_col], errors="coerce")
        exp_num  = pd.to_numeric(df_merged[exp_col],  errors="coerce")
        if base_num.notna().sum() == 0 and exp_num.notna().sum() == 0:
            continue

        df_merged[change_col] = _calc_change_percent(df_merged[base_col], df_merged[exp_col])
        final_columns.extend([base_col, exp_col, change_col])

    return final_columns


def process_comparison(base_file: str, exp_file: str, output_file: str) -> None:
    print(f"正在处理 (p1~p99 过滤): {base_file} vs {exp_file} ...")

    try:
        with open(base_file, "r", encoding="utf-8") as f:
            base_raw = json.load(f)
        with open(exp_file, "r", encoding="utf-8") as f:
            exp_raw = json.load(f)
    except FileNotFoundError as e:
        print(f"错误: 找不到文件 - {e}")
        return

    base_stats = build_statistics(base_raw)
    exp_stats  = build_statistics(exp_raw)

    df_base = pd.DataFrame(flatten_statistics(base_stats, "Base"))
    df_exp  = pd.DataFrame(flatten_statistics(exp_stats,  "Exp"))

    df_merged = pd.merge(
        df_base, df_exp,
        on=["Task", "Type"],
        suffixes=("_Base", "_Exp"),
        how="outer",
    )
    df_merged = df_merged[df_merged["Type"] == "Task"].copy()

    final_columns = _build_final_columns(df_merged)
    df_final = df_merged[final_columns].sort_values("Task").reset_index(drop=True)

    df_final.to_csv(output_file, index=False, float_format="%.4f", encoding="utf-8-sig")
    print(f"成功生成文件: {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Compare two experiments using p1~p99 filtered raw samples."
    )
    parser.add_argument("--base", default="baseline_groups_raw_samples.json",
                        help="baseline raw samples json")
    parser.add_argument("--exp",  default="task_groups_raw_samples.json",
                        help="experiment raw samples json")
    parser.add_argument("--out",  default="base-exp-filtered.csv",
                        help="output csv path")
    args = parser.parse_args()

    process_comparison(base_file=args.base, exp_file=args.exp, output_file=args.out)
