"""
visualize.py
============
为每一轮实验生成 33 张图：
  - 27 张每函数双视图对比图（2模式 × 2版本，四格布局）
  - 4 张 overview（baseline_all, baseline_filtered, exp_all, exp_filtered）
  - 2 张 comparison（raw + filtered）

图片输出到 closed_loop_results/plots/ 目录。
"""

import json
import math
import os
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import gaussian_kde

plt.rcParams["font.sans-serif"] = ["WenQuanYi Zen Hei", "Noto Sans CJK SC", "SimHei", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False

BAR_COLORS = [
    "#4C72B0", "#DD8452", "#55A868", "#C44E52", "#8172B2",
    "#937860", "#DA8BC3", "#8C8C8C", "#CCB974", "#64B5CD",
]

RESULTS_DIR = "closed_loop_results"
BASE_PREFIX = "baseline_groups"
EXP_PREFIX  = "task_groups"
N_BINS      = 100
MIN_SAMPLES = 10


def load_samples(overlaps_file: str) -> dict:
    with open(overlaps_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    per_func = defaultdict(list)
    for record in data.get("per_invocation", []):
        func = record.get("function_name")
        dur  = record.get("duration_seconds")
        if func and dur is not None:
            try:
                per_func[func].append(float(dur))
            except (TypeError, ValueError):
                pass
    return dict(per_func)


def _clip(arr: np.ndarray, filtered: bool):
    if not filtered:
        return arr, float(arr.min()), float(arr.max())
    lo = float(np.percentile(arr, 1))
    hi = float(np.percentile(arr, 99))
    return arr[(arr >= lo) & (arr <= hi)], lo, hi


def _draw_hist(ax, arr: np.ndarray, filtered: bool, color: str, func_name: str):
    """Draw histogram + KDE + percentile lines on ax."""
    clipped, lo, hi = _clip(arr, filtered)
    if lo >= hi or len(clipped) < 2:
        ax.text(0.5, 0.5, "insufficient data", transform=ax.transAxes,
                ha="center", va="center", color="gray")
        return

    ax.hist(clipped, bins=N_BINS, range=(lo, hi),
            color=color, alpha=0.7, edgecolor="white", linewidth=0.3)

    if len(clipped) >= 5:
        try:
            kde = gaussian_kde(clipped, bw_method="scott")
            xk  = np.linspace(lo, hi, 400)
            yk  = kde(xk) * len(clipped) * (hi - lo) / N_BINS
            ax.plot(xk, yk, color="black", linewidth=1.2)
        except Exception:
            pass

    mean_v = float(np.mean(clipped))
    p95_v  = float(np.percentile(clipped, 95))
    p99_v  = float(np.percentile(clipped, 99))
    for x, c, ls in [(mean_v, "red", "--"), (p95_v, "orange", "-"), (p99_v, "purple", "-")]:
        if lo <= x <= hi:
            ax.axvline(x, color=c, linestyle=ls, linewidth=0.9)

    label_str = "filtered p1~p99" if filtered else "all samples"
    ax.set_title(
        f"{func_name}  [{label_str}]  n={len(clipped)}\n"
        f"mean={mean_v:.3f}s  p95={p95_v:.3f}s  p99={p99_v:.3f}s",
        fontsize=8,
    )
    ax.set_xlabel("Time (s)", fontsize=7)
    ax.set_ylabel("Count", fontsize=7)
    ax.tick_params(labelsize=6)
    ax.set_xlim(lo, hi)


def plot_dual_view(func_name: str, base_arr: np.ndarray, exp_arr: np.ndarray,
                   out_path: str, color_base: str, color_exp: str) -> None:
    """
    2×2 grid:
      TL: baseline all    TR: baseline filtered
      BL: exp all         BR: exp filtered
    """
    fig, axes = plt.subplots(2, 2, figsize=(14, 8))
    fig.suptitle(f"{func_name}  —  Exec Wall-Clock Time Distribution", fontsize=13)

    _draw_hist(axes[0][0], base_arr, False, color_base, "Baseline")
    _draw_hist(axes[0][1], base_arr, True,  color_base, "Baseline")
    _draw_hist(axes[1][0], exp_arr,  False, color_exp,  "Experiment")
    _draw_hist(axes[1][1], exp_arr,  True,  color_exp,  "Experiment")

    for ax, label in zip(axes.flat, ["Baseline (all)", "Baseline (filtered p1~p99)",
                                      "Experiment (all)", "Experiment (filtered p1~p99)"]):
        ax.set_title(f"{label}\n" + ax.get_title(), fontsize=8)

    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"[INFO] Dual-view saved: {out_path}")


def plot_overview(func_samples: dict, out_path: str, filtered: bool) -> None:
    funcs   = sorted(func_samples.keys())
    n_funcs = len(funcs)
    if n_funcs == 0:
        return
    n_cols = min(3, n_funcs)
    n_rows = math.ceil(n_funcs / n_cols)
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(6 * n_cols, 4 * n_rows), squeeze=False)
    for idx, func in enumerate(funcs):
        row, col = divmod(idx, n_cols)
        ax    = axes[row][col]
        arr   = np.array(func_samples[func])
        color = BAR_COLORS[idx % len(BAR_COLORS)]
        _draw_hist(ax, arr, filtered, color, func)
    for idx in range(n_funcs, n_rows * n_cols):
        row, col = divmod(idx, n_cols)
        axes[row][col].set_visible(False)
    title = "Overview — All Functions" + (" (filtered p1~p99)" if filtered else " (all samples)")
    fig.suptitle(title, fontsize=14)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"[INFO] Overview saved: {out_path}")


def plot_comparison(base_samples: dict, exp_samples: dict, out_path: str,
                    filtered: bool, base_label: str = "Baseline",
                    exp_label: str = "Experiment") -> None:
    funcs   = sorted(set(base_samples.keys()) | set(exp_samples.keys()))
    n_funcs = len(funcs)
    if n_funcs == 0:
        return
    n_cols = min(3, n_funcs)
    n_rows = math.ceil(n_funcs / n_cols)
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(7 * n_cols, 4.5 * n_rows), squeeze=False)
    for idx, func in enumerate(funcs):
        row, col = divmod(idx, n_cols)
        ax = axes[row][col]
        for samples, color, lbl in [
            (base_samples.get(func, []), "#4C72B0", base_label),
            (exp_samples.get(func,  []), "#DD8452", exp_label),
        ]:
            if len(samples) < 5:
                continue
            arr = np.array(samples, dtype=float)
            if filtered:
                lo = float(np.percentile(arr, 1))
                hi = float(np.percentile(arr, 99))
                arr = arr[(arr >= lo) & (arr <= hi)]
            if len(arr) < 5:
                continue
            mean_v = float(np.mean(arr))
            cv_v   = float(np.std(arr) / mean_v) if mean_v else 0.0
            try:
                kde = gaussian_kde(arr, bw_method="scott")
                xk  = np.linspace(arr.min(), arr.max(), 500)
                ax.plot(xk, kde(xk), color=color, linewidth=1.8,
                        label=f"{lbl}  n={len(arr)}\nmean={mean_v:.3f}s  cv={cv_v:.3f}")
                ax.fill_between(xk, kde(xk), alpha=0.15, color=color)
            except Exception:
                continue
            ax.axvline(mean_v, color=color, linestyle="--", linewidth=1.0)
        ax.set_title(func, fontsize=10)
        ax.set_xlabel("Time (s)", fontsize=8)
        ax.set_ylabel("Density", fontsize=8)
        ax.tick_params(labelsize=7)
        ax.legend(fontsize=7)
    for idx in range(n_funcs, n_rows * n_cols):
        row, col = divmod(idx, n_cols)
        axes[row][col].set_visible(False)
    title = (f"Comparison: {base_label} vs {exp_label}"
             + (" (filtered p1~p99)" if filtered else " (all samples)"))
    fig.suptitle(title, fontsize=14)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"[INFO] Comparison saved: {out_path}")


def main() -> None:
    def _load(prefix):
        path = os.path.join(RESULTS_DIR, f"{prefix}_same_core_overlaps.json")
        if not os.path.exists(path):
            print(f"[WARN] Not found: {path}")
            return {}
        samples = load_samples(path)
        return {f: v for f, v in samples.items() if len(v) >= MIN_SAMPLES}

    base_samples = _load(BASE_PREFIX)
    exp_samples  = _load(EXP_PREFIX)

    plots_dir = os.path.join(RESULTS_DIR, "plots")
    os.makedirs(plots_dir, exist_ok=True)

    # ── 27 dual-view per-function figures ──────────────────────────────────────
    all_funcs = sorted(set(base_samples.keys()) | set(exp_samples.keys()))
    for idx, func in enumerate(all_funcs):
        base_arr = np.array(base_samples.get(func, []), dtype=float)
        exp_arr  = np.array(exp_samples.get(func,  []), dtype=float)
        if len(base_arr) < MIN_SAMPLES and len(exp_arr) < MIN_SAMPLES:
            continue
        color_base = BAR_COLORS[idx % len(BAR_COLORS)]
        color_exp  = BAR_COLORS[(idx + 5) % len(BAR_COLORS)]
        out_path   = os.path.join(plots_dir, f"{func}_dual_view.png")
        # Ensure both arrays are non-empty for drawing (pad with empty if missing)
        if len(base_arr) < MIN_SAMPLES:
            base_arr = np.array([0.0])
        if len(exp_arr) < MIN_SAMPLES:
            exp_arr = np.array([0.0])
        plot_dual_view(func, base_arr, exp_arr, out_path, color_base, color_exp)

    # ── 4 overview figures ──────────────────────────────────────────────────────
    if base_samples:
        plot_overview(base_samples,
                      os.path.join(plots_dir, "overview_baseline_all.png"),       filtered=False)
        plot_overview(base_samples,
                      os.path.join(plots_dir, "overview_baseline_filtered.png"),  filtered=True)
    if exp_samples:
        plot_overview(exp_samples,
                      os.path.join(plots_dir, "overview_exp_all.png"),            filtered=False)
        plot_overview(exp_samples,
                      os.path.join(plots_dir, "overview_exp_filtered.png"),       filtered=True)

    # ── 2 comparison figures ────────────────────────────────────────────────────
    if base_samples and exp_samples:
        plot_comparison(base_samples, exp_samples,
                        os.path.join(plots_dir, "comparison_all.png"),
                        filtered=False)
        plot_comparison(base_samples, exp_samples,
                        os.path.join(plots_dir, "comparison_filtered.png"),
                        filtered=True)

    print(f"\n[DONE] All plots saved under {plots_dir}")


if __name__ == "__main__":
    main()
