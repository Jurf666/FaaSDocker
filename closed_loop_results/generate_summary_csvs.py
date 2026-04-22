"""
generate_summary_csvs.py
========================
在所有 N 轮实验完成后，读取 multi_round/json/ 目录下每轮保存的
*_raw_samples.json，生成 11 个汇总 CSV 文件（过滤版本，p1~p99）：

  1. summary_CV_change_pct.csv       — CV 变化百分比
  2. summary_baseline_CV.csv         — baseline CV
  3. summary_exp_CV.csv              — experiment CV
  4. summary_baseline_Mean.csv       — baseline Mean (s)
  5. summary_exp_Mean.csv            — experiment Mean (s)
  6. summary_baseline_P90.csv        — baseline P90
  7. summary_exp_P90.csv             — experiment P90
  8. summary_baseline_P95.csv        — baseline P95
  9. summary_exp_P95.csv             — experiment P95
 10. summary_baseline_P99.csv        — baseline P99
 11. summary_exp_P99.csv             — experiment P99

每个 CSV 格式：
  Task | Round_1 | Round_2 | ... | Round_N | Average

用法：
  python generate_summary_csvs.py --json-dir ./closed_loop_results/multi_round/json \
                                   --rounds 5 \
                                   --out-dir ./closed_loop_results/multi_round/summary_csvs
"""

import argparse
import json
import os

import numpy as np
import pandas as pd


def _clip_p1_p99(values: list) -> np.ndarray:
    arr = np.array(values, dtype=float)
    if len(arr) < 2:
        return arr
    lo = np.percentile(arr, 1)
    hi = np.percentile(arr, 99)
    return arr[(arr >= lo) & (arr <= hi)]


def _compute_stats(values: list) -> dict:
    arr = _clip_p1_p99(values)
    if len(arr) == 0:
        return {}
    mean = float(np.mean(arr))
    std  = float(np.std(arr))
    return {
        "mean": mean,
        "cv":   float(std / mean) if mean != 0 else 0.0,
        "p90":  float(np.percentile(arr, 90)),
        "p95":  float(np.percentile(arr, 95)),
        "p99":  float(np.percentile(arr, 99)),
    }


def load_round_stats(json_dir: str, round_num: int) -> tuple:
    base_path = os.path.join(json_dir, f"round{round_num}_baseline_raw_samples.json")
    exp_path  = os.path.join(json_dir, f"round{round_num}_experiment_raw_samples.json")

    def _load(path):
        if not os.path.exists(path):
            print(f"[WARN] Missing: {path}")
            return {}
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        result = {}
        for func, metrics in raw.items():
            wall = metrics.get("exec_wall_time_s", [])
            if not wall:
                continue
            result[func] = _compute_stats(wall)
        return result

    return _load(base_path), _load(exp_path)


def build_summary_csvs(json_dir: str, rounds: int, out_dir: str) -> None:
    os.makedirs(out_dir, exist_ok=True)

    round_base = {}
    round_exp  = {}
    for r in range(1, rounds + 1):
        b, e = load_round_stats(json_dir, r)
        round_base[r] = b
        round_exp[r]  = e

    all_funcs = sorted({
        func
        for stats in list(round_base.values()) + list(round_exp.values())
        for func in stats
    })

    round_cols = [f"Round_{r}" for r in range(1, rounds + 1)]

    def _make_df(getter):
        rows = []
        for func in all_funcs:
            row = {"Task": func}
            vals = []
            for r in range(1, rounds + 1):
                v = getter(r, func)
                row[f"Round_{r}"] = round(v, 6) if v is not None else None
                if v is not None:
                    vals.append(v)
            row["Average"] = round(float(np.mean(vals)), 6) if vals else None
            rows.append(row)
        return pd.DataFrame(rows, columns=["Task"] + round_cols + ["Average"])

    def _save(df: pd.DataFrame, filename: str) -> None:
        path = os.path.join(out_dir, filename)
        df.to_csv(path, index=False, float_format="%.6f", encoding="utf-8-sig")
        print(f"[INFO] Saved: {path}")

    def _base_metric(metric):
        def _getter(r, func):
            return round_base[r].get(func, {}).get(metric)
        return _getter

    def _exp_metric(metric):
        def _getter(r, func):
            return round_exp[r].get(func, {}).get(metric)
        return _getter

    def _cv_change(r, func):
        base_cv = round_base[r].get(func, {}).get("cv")
        exp_cv  = round_exp[r].get(func, {}).get("cv")
        if base_cv is None or exp_cv is None or base_cv == 0:
            return None
        return (exp_cv - base_cv) / base_cv * 100.0

    _save(_make_df(_cv_change),            "summary_CV_change_pct.csv")
    _save(_make_df(_base_metric("cv")),    "summary_baseline_CV.csv")
    _save(_make_df(_exp_metric("cv")),     "summary_exp_CV.csv")
    _save(_make_df(_base_metric("mean")),  "summary_baseline_Mean.csv")
    _save(_make_df(_exp_metric("mean")),   "summary_exp_Mean.csv")
    _save(_make_df(_base_metric("p90")),   "summary_baseline_P90.csv")
    _save(_make_df(_exp_metric("p90")),    "summary_exp_P90.csv")
    _save(_make_df(_base_metric("p95")),   "summary_baseline_P95.csv")
    _save(_make_df(_exp_metric("p95")),    "summary_exp_P95.csv")
    _save(_make_df(_base_metric("p99")),   "summary_baseline_P99.csv")
    _save(_make_df(_exp_metric("p99")),    "summary_exp_P99.csv")

    print(f"\n[DONE] 11 summary CSVs saved to {out_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate per-metric summary CSVs across rounds.")
    parser.add_argument("--json-dir", default="closed_loop_results/multi_round/json",
                        help="Directory containing roundN_*_raw_samples.json files")
    parser.add_argument("--rounds",   type=int, required=True,
                        help="Total number of experiment rounds")
    parser.add_argument("--out-dir",  default="closed_loop_results/multi_round/summary_csvs",
                        help="Output directory for summary CSVs")
    args = parser.parse_args()
    build_summary_csvs(args.json_dir, args.rounds, args.out_dir)
