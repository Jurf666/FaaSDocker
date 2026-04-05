"""
visualize.py
============
从 closed_loop_results/{prefix}_same_core_overlaps.json 读取每次调用的
wall-clock 执行时间，为每个函数生成执行时间分布直方图。

用法：
  python visualize.py <prefix> [--funcs f1 f2 ...] [--bins N] [--out-dir DIR]

示例：
  python visualize.py baseline_groups
  python visualize.py task_groups --funcs matmul linpack --bins 40
"""

import argparse
import json
import math
import os
import sys
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")  # 无 GUI 环境也能生成图片
import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import gaussian_kde


# ── 各函数直方图的颜色循环 ──────────────────────────────────────────────────
BAR_COLORS = [
    "#4C72B0", "#DD8452", "#55A868", "#C44E52", "#8172B2",
    "#937860", "#DA8BC3", "#8C8C8C", "#CCB974", "#64B5CD",
]


def load_samples(overlaps_file: str) -> dict[str, list[float]]:
    """
    读取 overlaps JSON，按函数名聚合 duration_seconds。
    返回 {func_name: [duration, ...]}
    """
    with open(overlaps_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    per_func: dict[str, list[float]] = defaultdict(list)
    for record in data.get("per_invocation", []):
        func = record.get("function_name")
        dur = record.get("duration_seconds")
        if func and dur is not None:
            try:
                per_func[func].append(float(dur))
            except (TypeError, ValueError):
                pass

    return dict(per_func)


def plot_histogram(
    func_name: str,
    samples: list[float],
    n_bins: int,
    out_path: str,
    color: str = "#4C72B0",
    filtered: bool = False,
) -> None:
    """
    为单个函数绘制执行时间分布直方图（含 KDE 曲线和分位线）。
    filtered=True 时裁剪至 [p1, p99] 区间后再绘图。
    """
    arr = np.array(samples)
    n = len(arr)

    if filtered:
        lo = float(np.percentile(arr, 1))
        hi = float(np.percentile(arr, 99))
    else:
        lo = float(np.min(arr))
        hi = float(np.max(arr))

    if lo >= hi:
        print(f"[WARN] {func_name}: range is zero, skipping.")
        return

    # 统计量始终基于裁剪后的数据
    clipped = arr[(arr >= lo) & (arr <= hi)]
    p50 = float(np.percentile(clipped, 50))
    p95 = float(np.percentile(clipped, 95))
    p99 = float(np.percentile(clipped, 99))
    mean_val = float(np.mean(clipped))

    fig, ax = plt.subplots(figsize=(10, 5))

    # 直方图
    counts, bin_edges, _ = ax.hist(
        clipped,
        bins=n_bins,
        range=(lo, hi),
        color=color,
        alpha=0.7,
        edgecolor="white",
        linewidth=0.4,
        label=f"Histogram (n={n})",
    )

    # KDE 曲线（映射到与直方图相同的 y 轴刻度）
    if len(clipped) >= 5:
        try:
            kde = gaussian_kde(clipped, bw_method="scott")
            x_kde = np.linspace(lo, hi, 400)
            y_kde = kde(x_kde)
            # 将 KDE 密度缩放到直方图 count 轴
            bin_width = (hi - lo) / n_bins
            y_kde_scaled = y_kde * len(clipped) * bin_width
            ax.plot(x_kde, y_kde_scaled, color="black", linewidth=1.5, label="KDE")
        except Exception:
            pass  # 样本过少或带宽退化时跳过 KDE

    # 分位线
    vlines = [
        (mean_val, "red",    "--", f"mean={mean_val:.4f}s"),
        (p50,      "green",  "-",  f"p50={p50:.4f}s"),
        (p95,      "orange", "-",  f"p95={p95:.4f}s"),
        (p99,      "purple", "-",  f"p99={p99:.4f}s"),
    ]
    for x, c, ls, label in vlines:
        if lo <= x <= hi:
            ax.axvline(x, color=c, linestyle=ls, linewidth=1.2, label=label)

    ax.set_title(
        f"{func_name}  —  exec wall-clock time distribution  (n={n}"
        + (f", filtered p1~p99, n_clipped={len(clipped)}" if filtered else "")
        + ")",
        fontsize=13,
    )
    ax.set_xlabel("Execution time (s)", fontsize=11)
    ax.set_ylabel("Count", fontsize=11)
    ax.legend(fontsize=9, loc="upper right")
    ax.set_xlim(lo, hi)

    # 右上角注释：完整统计摘要（基于裁剪后数据）
    textstr = (
        f"{'[filtered p1~p99]' if filtered else '[all samples]'}\n"
        f"n={len(clipped)}\n"
        f"min={float(np.min(clipped)):.4f}s\n"
        f"max={float(np.max(clipped)):.4f}s\n"
        f"mean={mean_val:.4f}s\n"
        f"std={float(np.std(clipped)):.4f}s\n"
        f"p50={p50:.4f}s\n"
        f"p95={p95:.4f}s\n"
        f"p99={p99:.4f}s"
    )
    ax.text(
        0.98, 0.97, textstr,
        transform=ax.transAxes,
        fontsize=8,
        verticalalignment="top",
        horizontalalignment="right",
        bbox=dict(boxstyle="round,pad=0.4", facecolor="lightyellow", alpha=0.8),
    )

    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def plot_overview(
    func_samples: dict[str, list[float]],
    n_bins: int,
    out_path: str,
    filtered: bool = False,
) -> None:
    """
    总览图：将所有函数的分布并列在同一张大图中，便于横向对比。
    filtered=True 时各子图裁剪至 [p1, p99]。
    """
    funcs = sorted(func_samples.keys())
    n_funcs = len(funcs)
    if n_funcs == 0:
        return

    n_cols = min(3, n_funcs)
    n_rows = math.ceil(n_funcs / n_cols)

    fig, axes = plt.subplots(
        n_rows, n_cols,
        figsize=(6 * n_cols, 4 * n_rows),
        squeeze=False,
    )

    for idx, func in enumerate(funcs):
        row, col = divmod(idx, n_cols)
        ax = axes[row][col]
        arr = np.array(func_samples[func])
        n = len(arr)
        color = BAR_COLORS[idx % len(BAR_COLORS)]

        p1  = float(np.percentile(arr, 1))
        p99 = float(np.percentile(arr, 99))
        if filtered:
            lo, hi = p1, p99
        else:
            lo, hi = float(np.min(arr)), float(np.max(arr))

        clipped = arr[(arr >= lo) & (arr <= hi)]
        ax.hist(clipped, bins=n_bins, range=(lo, hi),
                color=color, alpha=0.75, edgecolor="white", linewidth=0.3)

        mean_val = float(np.mean(clipped))
        p95 = float(np.percentile(clipped, 95))
        for x, c, ls in [
            (mean_val, "red",    "--"),
            (p95,      "orange", "-"),
        ]:
            if lo <= x <= hi:
                ax.axvline(x, color=c, linestyle=ls, linewidth=1.0)

        ax.set_title(f"{func}  (n={len(clipped)}{'*' if filtered else ''})", fontsize=10)
        ax.set_xlabel("Time (s)", fontsize=8)
        ax.set_ylabel("Count", fontsize=8)
        ax.tick_params(labelsize=7)

    # 隐藏多余的子图格
    for idx in range(n_funcs, n_rows * n_cols):
        row, col = divmod(idx, n_cols)
        axes[row][col].set_visible(False)

    fig.suptitle(
        "Exec Wall-Clock Time Distribution — All Functions"
        + (" (filtered p1~p99, * = clipped count)" if filtered else ""),
        fontsize=14, y=1.01,
    )
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"[INFO] Overview saved to {out_path}")


def plot_comparison(
    base_samples: dict[str, list[float]],
    exp_samples: dict[str, list[float]],
    out_path: str,
    filtered: bool = False,
    base_label: str = "Baseline",
    exp_label: str = "Experiment",
) -> None:
    """
    对比汇总图：每个函数一个子图，同时展示 baseline 和实验组的 KDE 曲线。
    filtered=True 时各组数据先裁剪至 [p1, p99]。
    """
    funcs = sorted(set(base_samples.keys()) | set(exp_samples.keys()))
    n_funcs = len(funcs)
    if n_funcs == 0:
        return

    n_cols = min(3, n_funcs)
    n_rows = math.ceil(n_funcs / n_cols)

    fig, axes = plt.subplots(
        n_rows, n_cols,
        figsize=(7 * n_cols, 4.5 * n_rows),
        squeeze=False,
    )

    for idx, func in enumerate(funcs):
        row, col = divmod(idx, n_cols)
        ax = axes[row][col]

        for samples, color, label in [
            (base_samples.get(func, []), "#4C72B0", base_label),
            (exp_samples.get(func,  []), "#DD8452", exp_label),
        ]:
            if len(samples) < 5:
                continue
            arr = np.array(samples)

            if filtered:
                lo = float(np.percentile(arr, 1))
                hi = float(np.percentile(arr, 99))
                arr = arr[(arr >= lo) & (arr <= hi)]

            if len(arr) < 5:
                continue

            mean_val = float(np.mean(arr))
            std_val  = float(np.std(arr))
            cv_val   = std_val / mean_val if mean_val != 0 else 0.0

            # KDE 曲线
            try:
                kde = gaussian_kde(arr, bw_method="scott")
                x_kde = np.linspace(arr.min(), arr.max(), 500)
                y_kde = kde(x_kde)
                ax.plot(
                    x_kde, y_kde, color=color, linewidth=1.8,
                    label=f"{label}  n={len(arr)}\nmean={mean_val:.4f}s  cv={cv_val:.3f}",
                )
                ax.fill_between(x_kde, y_kde, alpha=0.15, color=color)
            except Exception:
                continue

            # mean 垂直线
            ax.axvline(mean_val, color=color, linestyle="--", linewidth=1.0)

        ax.set_title(func, fontsize=10)
        ax.set_xlabel("Time (s)", fontsize=8)
        ax.set_ylabel("Density", fontsize=8)
        ax.tick_params(labelsize=7)
        ax.legend(fontsize=7, loc="upper right")

    # 隐藏多余子图格
    for idx in range(n_funcs, n_rows * n_cols):
        row, col = divmod(idx, n_cols)
        axes[row][col].set_visible(False)

    title = (
        f"Exec Wall-Clock Time: {base_label} vs {exp_label}"
        + (" (filtered p1~p99)" if filtered else "")
    )
    fig.suptitle(title, fontsize=14, y=1.01)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"[INFO] Comparison plot saved to {out_path}")


def main() -> None:
    RESULTS_DIR  = "closed_loop_results"
    BASE_PREFIX  = "baseline_groups"
    EXP_PREFIX   = "task_groups"
    N_BINS       = 100
    MIN_SAMPLES  = 10

    def _load(prefix):
        path = os.path.join(RESULTS_DIR, f"{prefix}_same_core_overlaps.json")
        if not os.path.exists(path):
            print(f"[WARN] File not found, skipping: {path}")
            return {}
        print(f"[INFO] Loading {path} ...")
        samples = load_samples(path)
        return {f: v for f, v in samples.items() if len(v) >= MIN_SAMPLES}

    def _plot_single(prefix, func_samples):
        """为一组实验生成单组直方图（原始版 + 过滤版）。"""
        out_dir     = os.path.join(RESULTS_DIR, "plots", prefix)
        out_dir_f   = os.path.join(RESULTS_DIR, "plots", prefix + "_filtered")
        os.makedirs(out_dir,   exist_ok=True)
        os.makedirs(out_dir_f, exist_ok=True)
        print(f"[INFO] [{prefix}] output dir (all):      {out_dir}")
        print(f"[INFO] [{prefix}] output dir (filtered): {out_dir_f}")

        for idx, (func, samples) in enumerate(sorted(func_samples.items())):
            color = BAR_COLORS[idx % len(BAR_COLORS)]
            plot_histogram(func, samples, N_BINS,
                           os.path.join(out_dir,   f"{func}_wall_time.png"),
                           color=color, filtered=False)
            plot_histogram(func, samples, N_BINS,
                           os.path.join(out_dir_f, f"{func}_wall_time_filtered.png"),
                           color=color, filtered=True)
            print(f"[INFO]   {func}  n={len(samples)}")

        plot_overview(func_samples, N_BINS,
                      os.path.join(out_dir,   "_overview_wall_time.png"),          filtered=False)
        plot_overview(func_samples, N_BINS,
                      os.path.join(out_dir_f, "_overview_wall_time_filtered.png"), filtered=True)
        print(f"[INFO] [{prefix}] {len(func_samples)} function(s) done.")

    # 加载两组数据
    base_samples = _load(BASE_PREFIX)
    exp_samples  = _load(EXP_PREFIX)

    # 单组图
    if base_samples:
        _plot_single(BASE_PREFIX, base_samples)
    if exp_samples:
        _plot_single(EXP_PREFIX,  exp_samples)

    # 对比汇总图（两组都有数据时才生成）
    if base_samples and exp_samples:
        cmp_dir = os.path.join(RESULTS_DIR, "plots", "comparison")
        os.makedirs(cmp_dir, exist_ok=True)

        plot_comparison(base_samples, exp_samples,
                        os.path.join(cmp_dir, f"{BASE_PREFIX}_vs_{EXP_PREFIX}.png"),
                        filtered=False, base_label=BASE_PREFIX, exp_label=EXP_PREFIX)
        plot_comparison(base_samples, exp_samples,
                        os.path.join(cmp_dir, f"{BASE_PREFIX}_vs_{EXP_PREFIX}_filtered.png"),
                        filtered=True,  base_label=BASE_PREFIX, exp_label=EXP_PREFIX)
        print(f"[INFO] Comparison plots saved to {cmp_dir}")

    print(f"\n[DONE] All plots saved under {os.path.join(RESULTS_DIR, 'plots')}")


if __name__ == "__main__":
    main()
