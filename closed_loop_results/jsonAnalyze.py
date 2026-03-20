import argparse
import json
from typing import Any, Dict, List

import pandas as pd


def _flatten_dict(obj: Dict[str, Any], out: Dict[str, Any], prefix: str = "") -> None:
    """把嵌套字典展开为单层 key（使用点号拼接）。"""
    for key, value in obj.items():
        new_key = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            _flatten_dict(value, out, new_key)
        else:
            out[new_key] = value


def flatten_json_data(json_data: Dict[str, Any], source_label: str) -> List[Dict[str, Any]]:
    """
    把结果 JSON 展平为行：
    - statistics 下每个函数一行（Type=Task）
    - summary 一行（Task=Global_Summary, Type=Summary）
    """
    rows: List[Dict[str, Any]] = []

    stats = json_data.get("statistics", {})
    for task_name, metrics in stats.items():
        row = {"Task": task_name, "Source": source_label, "Type": "Task"}
        if isinstance(metrics, dict):
            flat_metrics: Dict[str, Any] = {}
            _flatten_dict(metrics, flat_metrics)
            row.update(flat_metrics)
        rows.append(row)

    summary = json_data.get("summary", {})
    if isinstance(summary, dict) and summary:
        row = {"Task": "Global_Summary", "Source": source_label, "Type": "Summary"}
        flat_summary: Dict[str, Any] = {}
        _flatten_dict(summary, flat_summary)
        row.update(flat_summary)
        rows.append(row)

    return rows


def _calc_change_percent(base_series: pd.Series, exp_series: pd.Series) -> pd.Series:
    """
    变化率公式：(Exp - Base) / Base * 100
    - Base=0 且 Exp=0 -> 0
    - Base=0 且 Exp!=0 -> NaN（避免无穷大污染）
    """
    base_num = pd.to_numeric(base_series, errors="coerce")
    exp_num = pd.to_numeric(exp_series, errors="coerce")

    change = (exp_num - base_num) / base_num * 100

    both_zero_mask = (base_num == 0) & (exp_num == 0)
    base_zero_mask = (base_num == 0) & (exp_num != 0)

    change = change.mask(base_zero_mask, pd.NA)
    change = change.mask(both_zero_mask, 0.0)
    return change


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

    # 用 Task + Type 合并，避免同名键冲突
    df_merged = pd.merge(
        df_base,
        df_exp,
        on=["Task", "Type"],
        suffixes=("_Base", "_Exp"),
        how="outer",
    )

    ignore_base_cols = {"Source_Base"}
    metric_cols = [
        c[:-5]
        for c in df_merged.columns
        if c.endswith("_Base") and c not in ignore_base_cols and f"{c[:-5]}_Exp" in df_merged.columns
    ]

    final_columns = ["Task", "Type"]
    for metric in metric_cols:
        base_col = f"{metric}_Base"
        exp_col = f"{metric}_Exp"

        # 仅处理至少一侧可转为数值的指标
        base_num = pd.to_numeric(df_merged[base_col], errors="coerce")
        exp_num = pd.to_numeric(df_merged[exp_col], errors="coerce")
        if base_num.notna().sum() == 0 and exp_num.notna().sum() == 0:
            continue

        change_col = f"{metric}_Change%"
        df_merged[change_col] = _calc_change_percent(df_merged[base_col], df_merged[exp_col])
        final_columns.extend([base_col, exp_col, change_col])

    df_final = df_merged[final_columns]

    # 汇总行放最上方，其他任务按名称排序
    df_summary = df_final[df_final["Task"] == "Global_Summary"]
    df_tasks = df_final[df_final["Task"] != "Global_Summary"].sort_values(by=["Type", "Task"])
    df_final = pd.concat([df_summary, df_tasks], ignore_index=True)

    # 用 utf-8-sig 方便 Excel 直接打开中文不乱码
    df_final.to_csv(output_file, index=False, float_format="%.4f", encoding="utf-8-sig")
    print(f"成功生成文件: {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compare two experiment JSON files and export CSV.")
    parser.add_argument("--base", default="base.json", help="baseline result json")
    parser.add_argument("--exp", default="exp.json", help="experiment result json")
    parser.add_argument("--out", default="base-exp.csv", help="output csv path")
    args = parser.parse_args()

    process_comparison(base_file=args.base, exp_file=args.exp, output_file=args.out)
