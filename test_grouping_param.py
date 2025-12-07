#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_grouping_param.py

控制变量扩展：在 baseline (ipc, llc_mpki, l1_mpki) 基础上, 测试候选特征的
所有子集 (或限制大小的子集)，使用 KMeans 比较每种组合的 silhouette。
优化: 对 silhouette、davies_bouldin、calinski_harabasz 三个指标都归一化到 0-1,
然后求和作为综合评分(composite_score), 这是因为silhouettes评分天然对n更小时的分组更友好,
为了减少这种干扰, 我们综合多种评分方式.

脚本使用方法: 直接调用python test_grouping_param.py, 要测试不同分组数下的效果就修改N_CLUSTERS参数.
"""
import os
import json
import math
import itertools
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from sklearn.preprocessing import RobustScaler
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score, davies_bouldin_score, calinski_harabasz_score

# ---------------- user params ----------------
INPUT_FILE = "summary.json"
OUT_DIR = "ablation_outputs"
N_CLUSTERS = 8
MIN_SAMPLES = 3
LOG1P = True
RANDOM_STATE = 42
KMEANS_N_INIT = 50

# Baseline features (always included)
BASELINE = ["ipc", "llc_mpki", "l1_mpki"]

# Candidate features to form subsets from
CANDIDATES = [
    "context_per_insn",
    "pf_per_insn",
    "cache_misses_per_insn",
    "branch_misses_per_insn"
]

# Limit maximum number of added features (set to len(CANDIDATES) for full power-set)
MAX_ADD = len(CANDIDATES)

# ---------------- helpers ----------------
def ensure_dir(d):
    os.makedirs(d, exist_ok=True)

def safe_float(x):
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None

def build_feature_rows(summary_json, min_samples=MIN_SAMPLES):
    rows = []
    skipped = []
    for func, info in summary_json.items():
        sc = info.get("sample_count", 0)
        try:
            sc_val = int(sc)
        except Exception:
            sc_val = 0
        if sc_val < min_samples:
            skipped.append((func, sc_val))
            continue

        raw = info.get("raw_avg", {}) or {}
        instr = safe_float(raw.get("instructions")) or 0.0
        cycles = safe_float(raw.get("cycles")) or 0.0
        task_clock = safe_float(raw.get("task-clock")) or 0.0
        context = safe_float(raw.get("context-switches")) or 0.0
        cache_misses = safe_float(raw.get("cache-misses")) or 0.0
        l1_misses = safe_float(raw.get("L1-dcache-load-misses")) or 0.0
        llc_misses = safe_float(raw.get("LLC-load-misses")) or 0.0
        page_faults = safe_float(raw.get("page-faults")) or 0.0
        branch_misses = safe_float(raw.get("branch-misses")) or 0.0

        # top-level derived values if present
        ipc_top = info.get("ipc") or info.get("IPC")
        llc_mpki_top = info.get("llc_mpki") or info.get("LLC-per-inst") or info.get("llc-per-inst")
        l1_mpki_top = info.get("l1_mpki") or info.get("L1-per-inst") or info.get("l1-per-inst")

        # compute safe derived features (prefer top-level if provided)
        if ipc_top is not None:
            ipc = safe_float(ipc_top)
        else:
            ipc = (instr / (cycles + 1e-12)) if cycles > 0 else 0.0

        if llc_mpki_top is not None:
            llc_mpki = safe_float(llc_mpki_top)
        else:
            llc_mpki = (llc_misses / (instr / 1000.0 + 1e-12)) if instr > 0 else 0.0

        if l1_mpki_top is not None:
            l1_mpki = safe_float(l1_mpki_top)
        else:
            l1_mpki = (l1_misses / (instr / 1000.0 + 1e-12)) if instr > 0 else 0.0

        # extra derived features
        cpi = (cycles / (instr + 1e-12)) if instr > 0 else 0.0
        task_clock_per_insn = (task_clock / (instr + 1e-12)) if instr > 0 else 0.0
        context_per_insn = (context / (instr + 1e-12)) if instr > 0 else 0.0
        pf_per_insn = (page_faults / (instr + 1e-12)) if instr > 0 else 0.0
        cache_misses_per_insn = (cache_misses / (instr + 1e-12)) if instr > 0 else 0.0
        branch_misses_per_insn = (branch_misses / (instr + 1e-12)) if instr > 0 else 0.0

        rows.append({
            "function": func,
            "sample_count": sc_val,
            "ipc": float(ipc or 0.0),
            "llc_mpki": float(llc_mpki or 0.0),
            "l1_mpki": float(l1_mpki or 0.0),
            "cpi": float(cpi),
            "task_clock_per_insn": float(task_clock_per_insn),
            "context_per_insn": float(context_per_insn),
            "pf_per_insn": float(pf_per_insn),
            "cache_misses_per_insn": float(cache_misses_per_insn),
            "branch_misses_per_insn": float(branch_misses_per_insn)
        })
    return pd.DataFrame(rows).set_index("function"), skipped

def preprocess_X(df_features, feature_cols, do_log=LOG1P):
    X = df_features[feature_cols].astype(float).replace([np.inf, -np.inf], np.nan).fillna(0.0).values
    if do_log:
        X = np.log1p(X)
    scaler = RobustScaler()
    X_scaled = scaler.fit_transform(X)
    return X_scaled

def plot_and_save_pca(X_scaled, labels, names, out_png, title):
    pca = PCA(n_components=2)
    coords = pca.fit_transform(X_scaled)
    plt.figure(figsize=(10,7))
    sc = plt.scatter(coords[:,0], coords[:,1], c=labels, cmap='tab10', s=70, alpha=0.8)
    for i, nm in enumerate(names):
        plt.text(coords[i,0]+0.01, coords[i,1]+0.01, nm, fontsize=8)
    plt.title(title)
    plt.xlabel("PC1"); plt.ylabel("PC2")
    plt.colorbar(sc)
    plt.grid(alpha=0.3)
    plt.tight_layout()
    #plt.savefig(out_png, dpi=200)
    plt.close()
    return pca.explained_variance_ratio_

# ---------------- main experiment ----------------
def main():
    ensure_dir(OUT_DIR)
    if not Path(INPUT_FILE).exists():
        print(f"[ERROR] Input file {INPUT_FILE} not found.")
        return

    with open(INPUT_FILE, 'r', encoding='utf-8') as fh:
        summary = json.load(fh)

    df, skipped = build_feature_rows(summary)
    if skipped:
        print(f"[i] Skipped {len(skipped)} functions due to sample_count < {MIN_SAMPLES} (examples: {skipped[:3]})")
    if df.empty:
        print("[ERROR] No valid functions after filtering.")
        return

    names = df.index.tolist()

    # build all subsets of candidates up to MAX_ADD
    all_subsets = []
    for r in range(0, min(len(CANDIDATES), MAX_ADD) + 1):
        for comb in itertools.combinations(CANDIDATES, r):
            all_subsets.append(list(comb))

    print(f"[i] Will run {len(all_subsets)} combinations (including baseline-only).")

    results = []

    # --- First pass: compute raw metrics per combo ---
    for subset in all_subsets:
        # create feature set = baseline + subset
        featset = list(BASELINE) + list(subset)
        # create readable tag
        if subset:
            tag = "plus_" + "_".join(subset)
        else:
            tag = "baseline"
        print(f"\n[RUN] tag={tag}, features={featset}")

        try:
            Xs = preprocess_X(df, featset, do_log=LOG1P)
            km = KMeans(n_clusters=N_CLUSTERS, n_init=KMEANS_N_INIT, random_state=RANDOM_STATE)
            labels = km.fit_predict(Xs)

            sil = None
            dbi = None
            ch = None
            try:
                if len(set(labels)) >= 2:
                    sil = float(silhouette_score(Xs, labels))
            except Exception:
                sil = None

            try:
                # davies_bouldin_score requires at least 2 clusters
                if len(set(labels)) >= 2:
                    dbi = float(davies_bouldin_score(Xs, labels))
            except Exception:
                dbi = None

            try:
                # calinski_harabasz_score requires n_samples > n_clusters and at least 2 labels
                if len(set(labels)) >= 2 and Xs.shape[0] > N_CLUSTERS:
                    ch = float(calinski_harabasz_score(Xs, labels))
            except Exception:
                ch = None

            mapping = {n: int(l) for n,l in zip(names, labels)}
            safe_tag = tag.replace(" ", "_").replace("/", "_")
            fname = os.path.join(OUT_DIR, f"kmeans_n{N_CLUSTERS}_{safe_tag}.json")
            '''
            with open(fname, 'w', encoding='utf-8') as fh:
                json.dump(mapping, fh, indent=2, ensure_ascii=False)
            '''

            png = os.path.join(OUT_DIR, f"kmeans_n{N_CLUSTERS}_{safe_tag}.png")
            evr = plot_and_save_pca(Xs, labels, names, png, f"KMeans n{N_CLUSTERS} - {tag} (sil={sil})")
            print(f"  -> silhouette={sil}, dbi={dbi}, ch={ch}, pca_ev_ratio={evr}, mapping={fname}, pca_png={png}")

            results.append({
                "tag": tag,
                "added": subset,
                "features": featset,
                "silhouette": sil,
                "dbi": dbi,
                "ch": ch,
                "pca_ev_ratio": evr.tolist(),
                "mapping_file": os.path.abspath(fname),
                "pca_png": os.path.abspath(png)
            })
        except Exception as e:
            print(f"  [ERROR] combo {tag} failed: {e}")
            results.append({
                "tag": tag,
                "added": subset,
                "features": featset,
                "error": str(e)
            })

    # --- Second pass: normalize the three metrics to 0-1 and compute composite ---
    # collect arrays (nan for missing)
    silhouettes = np.array([r.get("silhouette", np.nan) for r in results], dtype=float)
    dbis = np.array([r.get("dbi", np.nan) for r in results], dtype=float)
    chs = np.array([r.get("ch", np.nan) for r in results], dtype=float)

    # Normalize silhouette: from [-1,1] -> [0,1] (works when nan present)
    sil_norm = np.full_like(silhouettes, np.nan, dtype=float)
    valid_sil = np.isfinite(silhouettes)
    if valid_sil.any():
        sil_norm[valid_sil] = (silhouettes[valid_sil] + 1.0) / 2.0

    # Normalize DBI: lower is better -> invert (1/(dbi+eps)), then min-max to [0,1]
    eps = 1e-12
    dbi_norm = np.full_like(dbis, np.nan, dtype=float)
    valid_dbi = np.isfinite(dbis)
    if valid_dbi.any():
        dbi_inv = np.full_like(dbis, np.nan, dtype=float)
        dbi_inv[valid_dbi] = 1.0 / (dbis[valid_dbi] + eps)
        finite = np.isfinite(dbi_inv)
        if finite.any():
            mn = np.nanmin(dbi_inv[finite])
            mx = np.nanmax(dbi_inv[finite])
            if math.isclose(mx, mn):
                dbi_norm[finite] = 1.0  # all equal -> set to 1
            else:
                dbi_norm[finite] = (dbi_inv[finite] - mn) / (mx - mn)

    # Normalize CH: larger is better -> divide by max (min implicitly 0)
    ch_norm = np.full_like(chs, np.nan, dtype=float)
    valid_ch = np.isfinite(chs)
    if valid_ch.any():
        mx = np.nanmax(chs[valid_ch])
        if math.isclose(mx, 0.0):
            ch_norm[valid_ch] = 0.0
        else:
            ch_norm[valid_ch] = chs[valid_ch] / (mx + eps)

    # Put normalized values back into results and compute weighted score
    for i, r in enumerate(results):
        r["silhouette_norm"] = None if not np.isfinite(sil_norm[i]) else float(sil_norm[i])
        r["dbi_norm"] = None if not np.isfinite(dbi_norm[i]) else float(dbi_norm[i])
        r["ch_norm"] = None if not np.isfinite(ch_norm[i]) else float(ch_norm[i])

        # weights
        w_sil = 0.5
        w_dbi = 0.3
        w_ch  = 0.2

        # weighted composite score (only count metrics that exist)
        weighted_sum = 0.0
        weight_total = 0.0

        for val, w in (
            (r["silhouette_norm"], w_sil),
            (r["dbi_norm"], w_dbi),
            (r["ch_norm"], w_ch),
        ):
            if val is not None:
                weighted_sum += val * w
                weight_total += w

        # normalize by total weight of available metrics
        r["composite_score"] = float(weighted_sum)
        r["final_score"] = float(weighted_sum / weight_total) if weight_total > 0 else None


    # save summary & csv
    df_res = pd.DataFrame([{
        "tag": r.get("tag"),
        "added": ",".join(r.get("added", [])),
        "features": ",".join(r.get("features", [])),
        "silhouette": r.get("silhouette"),
        "silhouette_norm": r.get("silhouette_norm"),
        "dbi": r.get("dbi"),
        "dbi_norm": r.get("dbi_norm"),
        "ch": r.get("ch"),
        "ch_norm": r.get("ch_norm"),
        "composite_score": r.get("composite_score"),
        "final_score": r.get("final_score"),
        "pca_ev1": (r.get("pca_ev_ratio")[0] if r.get("pca_ev_ratio") else None),
        "pca_ev2": (r.get("pca_ev_ratio")[1] if r.get("pca_ev_ratio") else None),
        "mapping_file": r.get("mapping_file"),
        "pca_png": r.get("pca_png"),
        "error": r.get("error")
    } for r in results])
    csv_out = os.path.join(OUT_DIR, "kmeans_feature_subset_results.csv")
    df_res.to_csv(csv_out, index=False)
    with open(os.path.join(OUT_DIR, "kmeans_feature_subset_summary.json"), 'w', encoding='utf-8') as fh:
        json.dump(results, fh, indent=2, ensure_ascii=False)

    print(f"\nAll done. Results written to {csv_out} and summary JSON in {OUT_DIR}")

    # === 最佳 Top-5 结果统计并输出 ===
    print("\n==============================")
    print(f" Top 5 Feature Combinations (by composite_score) with n = {N_CLUSTERS}")
    print("==============================")

    # filter valid results (composite_score > 0 or not None)
    valid_results = [r for r in results if r.get("composite_score") is not None]
    valid_results.sort(key=lambda x: x["composite_score"], reverse=True)

    top_k = min(5, len(valid_results))
    best_five = valid_results[:top_k]

    for i, r in enumerate(best_five, 1):
        print(f"\n[{i}] composite = {r['composite_score']:.4f}, final(avg) = {r['final_score']:.4f}")
        print(f"    Features  : {r['features']}")
        print(f"    Norms     : sil={r.get('silhouette_norm')}, dbi={r.get('dbi_norm')}, ch={r.get('ch_norm')}")
        print(f"    Raw       : sil={r.get('silhouette')}, dbi={r.get('dbi')}, ch={r.get('ch')}")
        #print(f"    files     : {r.get('mapping_file')}, {r.get('pca_png')}")

if __name__ == "__main__":
    main()
