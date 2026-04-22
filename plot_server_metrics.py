"""
plot_server_metrics.py
======================
读取 server_metrics.csv，生成以下图表：
  1. overview_util.png   — 利用率概览：均值带 + 热力图
  2. overview_freq.png   — 频率概览：平滑均值 + 原始散点 + 稳定性标注
  3. overview_temp.png   — CPU 温度变化曲线
  4. grid_util.png       — 每核利用率网格（平滑）
  5. grid_freq.png       — 每核频率网格（平滑）

用法：
  python3 plot_server_metrics.py [csv文件路径] [--out-prefix 输出前缀]
"""

import argparse
import csv
import math
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.colors import Normalize
from matplotlib.cm import ScalarMappable
plt.rcParams["font.sans-serif"] = ["WenQuanYi Zen Hei", "Noto Sans CJK SC", "SimHei", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False
import numpy as np


# ── 数据加载 ──────────────────────────────────────────────────────────────────

def load_csv(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        headers = next(reader)
        rows = []
        for row in reader:
            try:
                rows.append([float(x) if x.strip() not in ("", "-1") else float("nan")
                              for x in row])
            except Exception:
                continue
    if not rows:
        print(f"[ERROR] CSV 文件为空或无有效数据: {filepath}")
        sys.exit(1)
    data = np.array(rows)
    return headers, {h: data[:, i] for i, h in enumerate(headers)}


def make_relative_time(timestamps):
    return timestamps - timestamps[0]


# ── 平滑工具 ──────────────────────────────────────────────────────────────────

def smooth(arr, window=15):
    """简单移动平均，边缘用 valid 模式填充 NaN。"""
    if window <= 1 or len(arr) < window:
        return arr.copy()
    kernel = np.ones(window) / window
    smoothed = np.convolve(arr, kernel, mode="same")
    half = window // 2
    smoothed[:half] = np.nan
    smoothed[-half:] = np.nan
    return smoothed


# ── 图1：利用率概览 ───────────────────────────────────────────────────────────

def plot_overview_util(t, util_matrix, labels, out_path, title_suffix):
    """
    上半：均值 ± std 带状图（平滑）
    下半：热力图（每行一个 CPU，颜色=利用率）
    """
    mean_util = np.nanmean(util_matrix, axis=1)
    std_util  = np.nanstd(util_matrix, axis=1)
    sm = smooth(mean_util, 15)

    fig = plt.figure(figsize=(14, 9))
    gs = gridspec.GridSpec(2, 1, height_ratios=[1, 1.4], hspace=0.35)

    # 上：均值带
    ax0 = fig.add_subplot(gs[0])
    ax0.fill_between(t, np.clip(sm - std_util, 0, 100),
                     np.clip(sm + std_util, 0, 100),
                     alpha=0.25, color="#4C72B0", label="±1 std")
    ax0.plot(t, sm, color="#4C72B0", linewidth=1.8, label="Mean util (smoothed)")
    ax0.plot(t, mean_util, color="#4C72B0", linewidth=0.4, alpha=0.35)
    ax0.set_ylim(0, 105)
    ax0.set_ylabel("Utilization (%)", fontsize=10)
    ax0.set_xlabel("Time (s)", fontsize=10)
    ax0.set_title("CPU Utilization Overview" + title_suffix, fontsize=11)
    ax0.legend(fontsize=8, loc="upper right")
    ax0.grid(axis="y", linestyle=":", alpha=0.4)
    ax0.set_xlim(0, t[-1])

    # 下：热力图
    ax1 = fig.add_subplot(gs[1])
    im = ax1.imshow(util_matrix.T, aspect="auto", origin="lower",
                    extent=[0, t[-1], 0, util_matrix.shape[1]],
                    cmap="RdYlGn_r", vmin=0, vmax=100, interpolation="nearest")
    ax1.set_xlabel("Time (s)", fontsize=10)
    ax1.set_ylabel("CPU index", fontsize=10)
    ax1.set_title("Per-CPU Utilization Heatmap (%)", fontsize=11)
    plt.colorbar(im, ax=ax1, label="%", fraction=0.02, pad=0.01)

    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"[INFO] 利用率概览图已保存: {out_path}")


# ── 图2：频率概览 ─────────────────────────────────────────────────────────────

def plot_overview_freq(t, avg_freq, freq_matrix, out_path, title_suffix):
    """
    主图：avg_freq 平滑曲线 + 原始半透明散点
    下方：频率稳定性（滚动 std）
    """
    sm = smooth(avg_freq, 15)
    roll_std = np.array([
        np.nanstd(avg_freq[max(0, i-15):i+15]) for i in range(len(avg_freq))
    ])

    fig, (ax0, ax1) = plt.subplots(2, 1, figsize=(14, 7),
                                   gridspec_kw={"height_ratios": [2.5, 1]},
                                   sharex=True)
    fig.subplots_adjust(hspace=0.15)

    # 原始散点（半透明，降采样避免过密）
    step = max(1, len(t) // 800)
    ax0.scatter(t[::step], avg_freq[::step], s=4, color="#4C72B0",
                alpha=0.25, linewidths=0, label="Raw avg_freq")
    ax0.plot(t, sm, color="#C44E52", linewidth=2.0, label="Smoothed avg_freq", zorder=3)

    # 标注稳定区间（rolling std < 50 MHz 的连续段）
    stable_mask = roll_std < 50
    in_seg = False
    seg_start = 0
    for i, s in enumerate(stable_mask):
        if s and not in_seg:
            seg_start = t[i]; in_seg = True
        elif not s and in_seg:
            ax0.axvspan(seg_start, t[i-1], alpha=0.08, color="green")
            in_seg = False
    if in_seg:
        ax0.axvspan(seg_start, t[-1], alpha=0.08, color="green",
                    label="Stable zone (std<50 MHz)")

    ax0.set_ylabel("Frequency (MHz)", fontsize=10)
    ax0.set_title("CPU Frequency Overview" + title_suffix, fontsize=11)
    ax0.legend(fontsize=8, loc="lower right")
    ax0.grid(axis="y", linestyle=":", alpha=0.4)
    ax0.set_xlim(0, t[-1])

    # 下：滚动 std
    ax1.fill_between(t, 0, roll_std, color="#DD8452", alpha=0.6)
    ax1.axhline(50, color="green", linewidth=1, linestyle="--", label="50 MHz threshold")
    ax1.set_ylabel("Rolling std\n(MHz)", fontsize=9)
    ax1.set_xlabel("Time (s)", fontsize=10)
    ax1.legend(fontsize=8, loc="upper right")
    ax1.grid(axis="y", linestyle=":", alpha=0.4)

    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"[INFO] 频率概览图已保存: {out_path}")


# ── 图3：温度变化 ─────────────────────────────────────────────────────────────

def plot_overview_temp(t, temp, out_path, title_suffix):
    sm = smooth(temp, 15)

    fig, ax = plt.subplots(figsize=(14, 4))

    ax.plot(t, temp, color="#4C72B0", linewidth=0.5, alpha=0.4, label="Raw")
    ax.plot(t, sm,   color="#C44E52", linewidth=2.0, label="Smoothed")

    # 温度区间着色
    ax.axhspan(0,  65, alpha=0.06, color="green",  label="< 65°C (cool)")
    ax.axhspan(65, 75, alpha=0.06, color="orange", label="65–75°C (warm)")
    ax.axhspan(75, 120, alpha=0.06, color="red",   label="> 75°C (hot)")

    # 关键温度线
    for thresh, color, ls in [(65, "green", "--"), (75, "orange", "--"), (80, "red", ":")]:
        if np.nanmax(temp) >= thresh - 2:
            ax.axhline(thresh, color=color, linewidth=1, linestyle=ls, alpha=0.7)

    # 标注最高温度点
    peak_idx = np.nanargmax(temp)
    ax.annotate(f"Peak {temp[peak_idx]:.1f}°C",
                xy=(t[peak_idx], temp[peak_idx]),
                xytext=(t[peak_idx] + t[-1]*0.02, temp[peak_idx] + 1),
                fontsize=8, color="red",
                arrowprops=dict(arrowstyle="->", color="red", lw=1))

    ax.set_ylim(max(0, np.nanmin(temp) - 5), np.nanmax(temp) + 8)
    ax.set_ylabel("Temperature (°C)", fontsize=10)
    ax.set_xlabel("Time (s)", fontsize=10)
    ax.set_title("CPU Max Temperature" + title_suffix, fontsize=11)
    ax.legend(fontsize=8, loc="upper left")
    ax.grid(axis="y", linestyle=":", alpha=0.4)
    ax.set_xlim(0, t[-1])

    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"[INFO] 温度图已保存: {out_path}")


# ── 图4/5：per-core 网格（平滑） ──────────────────────────────────────────────

def _make_grid_fig(cols_data, t, title, ylabel, ylim=None, extra_line=None,
                   smooth_window=15):
    n = len(cols_data)
    if n == 0:
        return None
    n_cols = min(6, n)
    n_rows = math.ceil(n / n_cols)
    fig, axes = plt.subplots(n_rows, n_cols,
                             figsize=(4 * n_cols, 2.5 * n_rows),
                             sharex=True, sharey=True)
    axes_flat = np.array(axes).flatten() if n > 1 else [axes]

    for idx, (label, vals) in enumerate(cols_data):
        ax = axes_flat[idx]
        sm = smooth(vals, smooth_window)
        # 原始数据极淡
        ax.plot(t, vals, linewidth=0.4, color="#4C72B0", alpha=0.25)
        # 平滑线
        ax.plot(t, sm,   linewidth=1.4, color="#4C72B0", alpha=0.95)
        if extra_line is not None:
            ex_vals, ex_label = extra_line
            ex_sm = smooth(ex_vals, smooth_window)
            ax.plot(t, ex_sm, linewidth=1.5, color="black", alpha=0.85)
        # 简化标签（去掉 group 信息）
        short = label.split("(")[0].strip()
        ax.set_title(short, fontsize=7)
        ax.set_ylabel(ylabel, fontsize=6)
        ax.set_xlabel("Time (s)", fontsize=6)
        ax.tick_params(labelsize=5)
        ax.grid(axis="y", linestyle=":", alpha=0.35)
        if ylim is not None:
            ax.set_ylim(ylim)
        ax.set_xlim(0, t[-1])

    for idx in range(n, n_rows * n_cols):
        axes_flat[idx].set_visible(False)

    fig.suptitle(title, fontsize=11)
    fig.tight_layout()
    return fig


# ── 主入口 ────────────────────────────────────────────────────────────────────

def plot_metrics(filepath, out_prefix):
    headers, data = load_csv(filepath)

    timestamps = data.get("timestamp")
    if timestamps is None:
        print("[ERROR] CSV 中没有 timestamp 列")
        sys.exit(1)

    t = make_relative_time(timestamps)
    total_seconds = int(t[-1])
    base_name = os.path.basename(filepath)
    title_suffix = f"\n{base_name}  |  Duration: {total_seconds}s  |  Samples: {len(t)}"

    util_cols = [(h, data[h]) for h in headers if "_util%" in h]
    freq_cols = [(h, data[h]) for h in headers if "_freq_MHz" in h and "avg" not in h]
    avg_col   = "avg_freq_MHz"
    temp_col  = "max_cpu_temp_C"
    has_avg   = avg_col in data and not np.all(np.isnan(data[avg_col]))
    has_temp  = temp_col in data and not np.all(np.isnan(data[temp_col]))

    # 利用率矩阵 (T x N_cpu)
    if util_cols:
        util_matrix = np.column_stack([v for _, v in util_cols])

        # 图1：概览
        plot_overview_util(t, util_matrix,
                           [h for h, _ in util_cols],
                           f"{out_prefix}_overview_util.png",
                           title_suffix)

        # 图4：网格
        fig = _make_grid_fig(util_cols, t,
                             "CPU Utilization per Core (%)" + title_suffix,
                             "%", ylim=(0, 105))
        out = f"{out_prefix}_grid_util.png"
        fig.savefig(out, dpi=130, bbox_inches="tight")
        plt.close(fig)
        print(f"[INFO] 利用率网格图已保存: {out}")
    else:
        print("[WARN] 未找到 CPU 利用率列")

    if has_avg:
        avg_freq = data[avg_col]
        freq_matrix = np.column_stack([v for _, v in freq_cols]) if freq_cols else None

        # 图2：概览
        plot_overview_freq(t, avg_freq, freq_matrix,
                           f"{out_prefix}_overview_freq.png",
                           title_suffix)

        # 图5：网格
        if freq_cols:
            extra = (avg_freq, "avg_freq")
            fig = _make_grid_fig(freq_cols, t,
                                 "CPU Frequency per Core (MHz)" + title_suffix,
                                 "MHz", extra_line=extra)
            out = f"{out_prefix}_grid_freq.png"
            fig.savefig(out, dpi=130, bbox_inches="tight")
            plt.close(fig)
            print(f"[INFO] 频率网格图已保存: {out}")
    else:
        print("[WARN] 未找到 avg_freq_MHz 列，跳过频率概览图")

    if has_temp:
        plot_overview_temp(t, data[temp_col],
                           f"{out_prefix}_overview_temp.png",
                           title_suffix)
    else:
        print("[WARN] 未找到 max_cpu_temp_C 列，跳过温度图")


def main():
    parser = argparse.ArgumentParser(description="绘制 server_metrics.csv 数据质量图")
    parser.add_argument("csv", nargs="?", default="server_metrics.csv")
    parser.add_argument("--out-prefix", default=None,
                        help="输出图片前缀（默认与CSV同名去掉扩展名）")
    args = parser.parse_args()

    if not os.path.exists(args.csv):
        print(f"[ERROR] 找不到文件: {args.csv}")
        sys.exit(1)

    out_prefix = args.out_prefix or os.path.splitext(args.csv)[0]
    plot_metrics(args.csv, out_prefix)


if __name__ == "__main__":
    main()
