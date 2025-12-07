#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
perf_task_grouping_multi.py

在保留原脚本主逻辑的前提下，增加多种聚类方法并输出对比结果：
 - kmeans (原脚本)
 - gmm
 - hierarchical (agglomerative)
 - spectral
 - cosine_agglomerative (基于余弦相似度的层次聚类)

输入: summary.json (同原脚本)
输出:
 - task_groups.json (保持原脚本默认输出，为 KMeans 的 mapping)
 - task_groups_<method>.json (每个方法单独的 mapping)
 - pca_<method>.png (每个方法的 PCA 可视化)
 - clustering_summary.json (各方法 silhouette/dbi/ch/composite 分数汇总)
"""
import os
import json
import math
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans, AgglomerativeClustering, SpectralClustering
from sklearn.mixture import GaussianMixture
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score, davies_bouldin_score, calinski_harabasz_score
from sklearn.metrics.pairwise import cosine_similarity

# ---------------- user params (you can change) ----------------
INPUT_FILE = 'summary.json'
OUTPUT_CONFIG = 'task_groups.json'        # original output (kept for compatibility)
N_CLUSTERS = 6
METHODS_TO_RUN = [                       # methods to run (order matters)
    "threshold_score",
    "kmeans",
    "gmm",
    "hierarchical",
    "spectral",
    "cosine_agglomerative"
]
N_INIT = 50                              # for kmeans/GMM n_init attempts
RANDOM_STATE = 42

# weights for composite scoring (silhouette, dbi_norm, ch_norm)
WEIGHT_SIL = 0.5
WEIGHT_DBI = 0.3
WEIGHT_CH  = 0.2

# ---------------- helper functions ----------------
def ensure_dir(d):
    if not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def save_mapping(mapping, outpath):
    with open(outpath, 'w', encoding='utf-8') as fh:
        json.dump(mapping, fh, indent=2, ensure_ascii=False)

def plot_pca_and_save(X_scaled, labels, names, out_png, title):
    pca = PCA(n_components=2)
    coords = pca.fit_transform(X_scaled)
    plt.figure(figsize=(10, 7))
    scatter = plt.scatter(coords[:, 0], coords[:, 1], c=labels, cmap='viridis', s=100, alpha=0.7)
    for i, nm in enumerate(names):
        plt.text(coords[i, 0], coords[i, 1], nm, fontsize=8)
    plt.title(title)
    plt.xlabel("PC1")
    plt.ylabel("PC2")
    # legend may be large for many groups; keep it but it's optional
    try:
        plt.legend(*scatter.legend_elements(), title="Group", loc='best', fontsize=8)
    except Exception:
        pass
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_png, dpi=200)
    plt.close()
    # return explained variance for debugging if needed
    return pca.explained_variance_ratio_

def compute_silhouette_safe(X_scaled, labels):
    try:
        unique = set(labels)
        if len(unique) < 2:
            return None
        return float(silhouette_score(X_scaled, labels))
    except Exception:
        return None

def compute_dbi_safe(X_scaled, labels):
    try:
        unique = set(labels)
        if len(unique) < 2:
            return None
        return float(davies_bouldin_score(X_scaled, labels))
    except Exception:
        return None

def compute_ch_safe(X_scaled, labels, n_clusters):
    try:
        unique = set(labels)
        # calinski_harabasz requires n_samples > n_clusters and at least 2 labels
        if len(unique) < 2 or X_scaled.shape[0] <= n_clusters:
            return None
        return float(calinski_harabasz_score(X_scaled, labels))
    except Exception:
        return None


# ---------------- main routine ----------------
def perform_grouping():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found. Run aggregate_metrics.py first.")
        return

    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # convert to DataFrame (compatible with original keys ipc, llc_mpki, l1_mpki)
    rows = []
    for func, info in data.items():
        # attempt to read provided top-level metrics
        ipc = None
        llc = None
        l1 = None

        # top-level convenience keys
        if isinstance(info.get('ipc'), (int, float)):
            ipc = info.get('ipc')
        elif isinstance(info.get('IPC'), (int, float)):
            ipc = info.get('IPC')

        if isinstance(info.get('llc_mpki'), (int, float)):
            llc = info.get('llc_mpki')
        elif isinstance(info.get('LLC-per-inst'), (int, float)):
            llc = info.get('LLC-per-inst')

        if isinstance(info.get('l1_mpki'), (int, float)):
            l1 = info.get('l1_mpki')
        elif isinstance(info.get('L1-per-inst'), (int, float)):
            l1 = info.get('L1-per-inst')

        # attempt to find raw averages for derived features
        raw = info.get("raw_avg") or info.get("raw") or {}
        instr = raw.get("instructions") or info.get("instructions") or 0.0
        # some files may use 'context-switches' or 'context' as key
        context = raw.get("context-switches") or raw.get("context") or info.get("context-switches") or info.get("context") or 0.0

        # fallback default numeric conversions
        try:
            instr = float(instr)
        except Exception:
            instr = 0.0
        try:
            context = float(context)
        except Exception:
            context = 0.0

        # compute context_per_insn safely
        context_per_insn = (context / (instr + 1e-12)) if instr > 0 else 0.0

        # final safe defaults for ipc/llc/l1
        ipc_val = float(ipc) if ipc is not None else 0.0
        llc_val = float(llc) if llc is not None else 0.0
        l1_val = float(l1) if l1 is not None else 0.0

        rows.append({
            "function": func,
            "ipc": ipc_val,
            "llc_mpki": llc_val,
            "l1_mpki": l1_val,
            "context_per_insn": float(context_per_insn)
        })

    df = pd.DataFrame(rows)
    print(f"[-] Loaded {len(df)} tasks for grouping.")

    if df.empty:
        print("[!] No tasks found, abort.")
        return

    # now we use 4 features including the new context_per_insn
    feature_cols = ['ipc', 'llc_mpki', 'l1_mpki', 'context_per_insn']
    X = df[feature_cols].values

    # standardize
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    names = df['function'].tolist()

    # create output dir
    out_dir = "clustering_outputs"
    ensure_dir(out_dir)

    # We'll collect per-method raw metrics first, then normalize across methods and compute composite
    method_records = []  # list of dicts {method, labels, fname, png, sil, dbi, ch}

    # run methods
    for method in METHODS_TO_RUN:
        try:
            method_lower = method.lower()
            print(f"\n[i] Running method: {method}")

            labels = np.zeros(len(names), dtype=int)
            sil = None
            dbi = None
            ch = None
            fname = None
            png = None

            if method_lower == 'kmeans':
                km = KMeans(n_clusters=N_CLUSTERS, random_state=RANDOM_STATE, n_init=N_INIT)
                labels = km.fit_predict(X_scaled)
                sil = compute_silhouette_safe(X_scaled, labels)
                dbi = compute_dbi_safe(X_scaled, labels)
                ch = compute_ch_safe(X_scaled, labels, N_CLUSTERS)
                mapping = {n: int(l) for n, l in zip(names, labels)}
                fname = os.path.join(out_dir, f"{method_lower}_n{N_CLUSTERS}.json")
                save_mapping(mapping, fname)
                png = os.path.join(out_dir, f"{method_lower}_n{N_CLUSTERS}.png")
                evr = plot_pca_and_save(X_scaled, labels, names, png, f"kmeans (Silhouette: {sil})")

            elif method_lower == 'gmm':
                try:
                    gm = GaussianMixture(n_components=N_CLUSTERS, covariance_type='full', random_state=RANDOM_STATE, n_init=5)
                    labels = gm.fit_predict(X_scaled)
                    sil = compute_silhouette_safe(X_scaled, labels)
                    dbi = compute_dbi_safe(X_scaled, labels)
                    ch = compute_ch_safe(X_scaled, labels, N_CLUSTERS)
                except Exception as e:
                    print(f"[WARN] GMM failed: {e}")
                    labels = np.zeros(len(names), dtype=int)
                    sil = None
                    dbi = None
                    ch = None
                mapping = {n: int(l) for n, l in zip(names, labels)}
                fname = os.path.join(out_dir, f"{method_lower}_n{N_CLUSTERS}.json")
                save_mapping(mapping, fname)
                png = os.path.join(out_dir, f"{method_lower}_n{N_CLUSTERS}.png")
                evr = plot_pca_and_save(X_scaled, labels, names, png, f"GMM (Silhouette: {sil})")

            elif method_lower == 'hierarchical':
                try:
                    hier = AgglomerativeClustering(n_clusters=N_CLUSTERS, linkage='ward')
                    labels = hier.fit_predict(X_scaled)
                    sil = compute_silhouette_safe(X_scaled, labels)
                    dbi = compute_dbi_safe(X_scaled, labels)
                    ch = compute_ch_safe(X_scaled, labels, N_CLUSTERS)
                except Exception as e:
                    print(f"[WARN] Hierarchical failed: {e}")
                    labels = np.zeros(len(names), dtype=int)
                    sil = None
                    dbi = None
                    ch = None
                mapping = {n: int(l) for n, l in zip(names, labels)}
                fname = os.path.join(out_dir, f"{method_lower}_n{N_CLUSTERS}.json")
                save_mapping(mapping, fname)
                png = os.path.join(out_dir, f"{method_lower}_n{N_CLUSTERS}.png")
                evr = plot_pca_and_save(X_scaled, labels, names, png, f"Hierarchical (Silhouette: {sil})")

            elif method_lower == 'spectral':
                try:
                    spec = SpectralClustering(n_clusters=N_CLUSTERS, random_state=RANDOM_STATE, affinity='rbf', assign_labels='kmeans')
                    labels = spec.fit_predict(X_scaled)
                    sil = compute_silhouette_safe(X_scaled, labels)
                    dbi = compute_dbi_safe(X_scaled, labels)
                    ch = compute_ch_safe(X_scaled, labels, N_CLUSTERS)
                except Exception as e:
                    print(f"[WARN] Spectral failed: {e}")
                    labels = np.zeros(len(names), dtype=int)
                    sil = None
                    dbi = None
                    ch = None
                mapping = {n: int(l) for n, l in zip(names, labels)}
                fname = os.path.join(out_dir, f"{method_lower}_n{N_CLUSTERS}.json")
                save_mapping(mapping, fname)
                png = os.path.join(out_dir, f"{method_lower}_n{N_CLUSTERS}.png")
                evr = plot_pca_and_save(X_scaled, labels, names, png, f"Spectral (Silhouette: {sil})")

            elif method_lower == 'cosine_agglomerative':
                try:
                    sim = cosine_similarity(X_scaled)
                    dist = 1.0 - sim
                    cos_model = AgglomerativeClustering(n_clusters=N_CLUSTERS, metric='precomputed', linkage='average')
                    labels = cos_model.fit_predict(dist)
                    sil = compute_silhouette_safe(X_scaled, labels)
                    dbi = compute_dbi_safe(X_scaled, labels)
                    ch = compute_ch_safe(X_scaled, labels, N_CLUSTERS)
                except Exception as e:
                    print(f"[WARN] Cosine-Agglomerative failed: {e}")
                    labels = np.zeros(len(names), dtype=int)
                    sil = None
                    dbi = None
                    ch = None
                mapping = {n: int(l) for n, l in zip(names, labels)}
                fname = os.path.join(out_dir, f"{method_lower}_n{N_CLUSTERS}.json")
                save_mapping(mapping, fname)
                png = os.path.join(out_dir, f"{method_lower}_n{N_CLUSTERS}.png")
                evr = plot_pca_and_save(X_scaled, labels, names, png, f"Cosine-Agglomerative (Silhouette: {sil})")

            else:
                print(f"[WARN] Unknown method {method}, skipping.")
                continue

            # record raw metrics & filepaths; we'll normalize them across methods later
            method_records.append({
                "method": method,
                "labels": [int(x) for x in labels],
                "silhouette": sil,
                "dbi": dbi,
                "ch": ch,
                "output_mapping": os.path.abspath(fname) if fname else None,
                "pca_png": os.path.abspath(png) if png else None
            })

            print(f"[OK] {method} done. silhouette={sil}, dbi={dbi}, ch={ch}")

        except Exception as e:
            print(f"[ERROR] method {method} crashed: {e}")
            method_records.append({
                "method": method,
                "error": str(e)
            })

    # ---------------- normalize metrics across methods and compute composite ----------------
    # collect arrays (nan for missing)
    silhouettes = np.array([r.get("silhouette", np.nan) for r in method_records], dtype=float)
    dbis = np.array([r.get("dbi", np.nan) for r in method_records], dtype=float)
    chs = np.array([r.get("ch", np.nan) for r in method_records], dtype=float)

    # Normalize silhouette: from [-1,1] -> [0,1]
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
                dbi_norm[finite] = 1.0
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

    # compute weighted composite for each method (weights apply only to available metrics)
    clustering_summary = {}
    for i, r in enumerate(method_records):
        sil_n = None if not np.isfinite(sil_norm[i]) else float(sil_norm[i])
        dbi_n = None if not np.isfinite(dbi_norm[i]) else float(dbi_norm[i])
        ch_n = None if not np.isfinite(ch_norm[i]) else float(ch_norm[i])

        # weighted composite with available weights
        weighted_sum = 0.0
        weight_total = 0.0
        for val, w in ((sil_n, WEIGHT_SIL), (dbi_n, WEIGHT_DBI), (ch_n, WEIGHT_CH)):
            if val is not None:
                weighted_sum += val * w
                weight_total += w

        final_score = float(weighted_sum / weight_total) if weight_total > 0 else None
        composite_raw = float(weighted_sum)  # sum of weighted values (not normalized by total weight)

        clustering_summary[r.get("method")] = {
            "silhouette": r.get("silhouette"),
            "silhouette_norm": sil_n,
            "dbi": r.get("dbi"),
            "dbi_norm": dbi_n,
            "ch": r.get("ch"),
            "ch_norm": ch_n,
            "composite_score": composite_raw,
            "final_score": final_score,
            "output_mapping": r.get("output_mapping"),
            "pca_png": r.get("pca_png"),
            "error": r.get("error")
        }

    # Save original KMeans mapping to OUTPUT_CONFIG for compatibility (if exists)
    km_map_path = os.path.join(out_dir, "kmeans_n" + str(N_CLUSTERS) + ".json")
    if "kmeans" in clustering_summary and os.path.exists(km_map_path):
        try:
            with open(km_map_path, 'r', encoding='utf-8') as fh:
                km_map = json.load(fh)
            with open(OUTPUT_CONFIG, 'w', encoding='utf-8') as fh:
                json.dump(km_map, fh, indent=2, ensure_ascii=False)
            print(f"[i] Wrote KMeans mapping also to {OUTPUT_CONFIG} (compatibility).")
        except Exception as e:
            print(f"[WARN] could not write compatibility file {OUTPUT_CONFIG}: {e}")

    # save clustering summary
    summary_path = os.path.join(out_dir, "clustering_summary.json")
    with open(summary_path, 'w', encoding='utf-8') as fh:
        json.dump(clustering_summary, fh, indent=2, ensure_ascii=False)
    print(f"\nAll clustering methods finished. Summary written to {summary_path}")
    print("Per-method composite scores (final_score):")
    for k, v in clustering_summary.items():
        print(f" - {k}: final_score = {v.get('final_score')} (silhouette={v.get('silhouette')}, dbi={v.get('dbi')}, ch={v.get('ch')})")

if __name__ == "__main__":
    perform_grouping()
