import argparse
import json
from typing import Any, Dict, List

import pandas as pd


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


def _flatten_dict(obj: Dict[str, Any], out: Dict[str, Any], prefix: str = "") -> None:
    """把嵌套字典拍平成单层 key（使用点号拼接）。"""
    for key, value in obj.items():
        new_key = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            _flatten_dict(value, out, new_key)
        else:
            out[new_key] = value


def flatten_json_data(json_data: Dict[str, Any], source_label: str) -> List[Dict[str, Any]]:
    """把结果 JSON 展平为任务级行。"""
    rows: List[Dict[str, Any]] = []

    stats = json_data.get("statistics", {})
    for task_name, metrics in stats.items():
        row = {"Task": task_name, "Source": source_label, "Type": "Task"}
        if isinstance(metrics, dict):
            flat_metrics: Dict[str, Any] = {}
            _flatten_dict(metrics, flat_metrics)
            row.update(flat_metrics)
        rows.append(row)

    return rows


def _calc_change_percent(base_series: pd.Series, exp_series: pd.Series) -> pd.Series:
    """
    变化率公式：(Exp - Base) / Base * 100
    - Base=0 且 Exp=0 -> 0
    - Base=0 且 Exp!=0 -> NaN，避免无穷大污染
    """
    base_num = pd.to_numeric(base_series, errors="coerce")
    exp_num = pd.to_numeric(exp_series, errors="coerce")

    change = (exp_num - base_num) / base_num * 100

    both_zero_mask = (base_num == 0) & (exp_num == 0)
    base_zero_mask = (base_num == 0) & (exp_num != 0)

    change = change.mask(base_zero_mask, pd.NA)
    change = change.mask(both_zero_mask, 0.0)
    return change


def _collect_available_metrics(df_merged: pd.DataFrame) -> set[str]:
    ignore_base_cols = {"Source_Base"}
    return {
        c[:-5]
        for c in df_merged.columns
        if c.endswith("_Base") and c not in ignore_base_cols and f"{c[:-5]}_Exp" in df_merged.columns
    }


def _build_no_fail_columns(df_merged: pd.DataFrame) -> None:
    for suffix in ("Base", "Exp"):
        attempts_col = f"attempts_{suffix}"
        success_col = f"success_{suffix}"
        no_fail_col = f"noFail_{suffix}"

        if attempts_col not in df_merged.columns or success_col not in df_merged.columns:
            df_merged[no_fail_col] = pd.Series(pd.NA, index=df_merged.index, dtype="Int64")
            continue

        attempts = pd.to_numeric(df_merged[attempts_col], errors="coerce")
        success = pd.to_numeric(df_merged[success_col], errors="coerce")
        no_fail = pd.Series(pd.NA, index=df_merged.index, dtype="Int64")

        valid_mask = attempts.notna() & success.notna()
        no_fail.loc[valid_mask] = (attempts.loc[valid_mask] == success.loc[valid_mask]).astype(int)
        df_merged[no_fail_col] = no_fail


def _build_final_columns(df_merged: pd.DataFrame) -> List[str]:
    available_metrics = _collect_available_metrics(df_merged)
    selected_metrics = [
        metric
        for metric in TASK_METRIC_WHITELIST
        if metric in available_metrics
    ]

    final_columns = ["Task", "noFail_Base", "noFail_Exp"]
    for metric in selected_metrics:
        base_col = f"{metric}_Base"
        exp_col = f"{metric}_Exp"

        base_num = pd.to_numeric(df_merged[base_col], errors="coerce")
        exp_num = pd.to_numeric(df_merged[exp_col], errors="coerce")
        if base_num.notna().sum() == 0 and exp_num.notna().sum() == 0:
            continue

        change_col = f"{metric}_Change%"
        df_merged[change_col] = _calc_change_percent(df_merged[base_col], df_merged[exp_col])
        final_columns.extend([base_col, exp_col, change_col])

    return final_columns


def process_comparison(base_file: str, exp_file: str, output_file: str) -> None:
    print(f"正在处理: {base_file} vs {exp_file} ...")

    try:
        with open(base_file, "r", encoding="utf-8") as f:
            base_json = json.load(f)
        with open(exp_file, "r", encoding="utf-8") as f:
            exp_json = json.load(f)
    except FileNotFoundError as e:
        print(f"错误: 找不到文件 - {e}")
        return

    df_base = pd.DataFrame(flatten_json_data(base_json, "Base"))
    df_exp = pd.DataFrame(flatten_json_data(exp_json, "Exp"))

    df_merged = pd.merge(
        df_base,
        df_exp,
        on=["Task", "Type"],
        suffixes=("_Base", "_Exp"),
        how="outer",
    )

    df_merged = df_merged[df_merged["Type"] == "Task"].copy()
    _build_no_fail_columns(df_merged)

    final_columns = _build_final_columns(df_merged)
    df_final = df_merged[final_columns]
    df_final = df_final.sort_values(by=["Task"]).reset_index(drop=True)

    df_final.to_csv(output_file, index=False, float_format="%.4f", encoding="utf-8-sig")
    print(f"成功生成文件: {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compare two experiment JSON files and export CSV.")
    parser.add_argument("--base", default="baseline_groups_results.json", help="baseline result json")
    parser.add_argument("--exp", default="task_groups_results.json", help="experiment result json")
    parser.add_argument("--out", default="base-exp.csv", help="output csv path")
    args = parser.parse_args()

    process_comparison(base_file=args.base, exp_file=args.exp, output_file=args.out)
