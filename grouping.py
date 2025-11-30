import os
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA

# 尝试导入 adjustText 优化标签显示
try:
    from adjustText import adjust_text
    HAS_ADJUST_TEXT = True
except ImportError:
    HAS_ADJUST_TEXT = False
    print("[!] Warning: 'adjustText' not found. Install it for better plots: pip install adjustText")

# --- 配置区域 ---
LOG_DIR = './storage/perf_logs'
OUTPUT_CONFIG = 'task_groups.json'
N_CLUSTERS = 3  # 建议保持 3: 计算型、内存型、混合/IO型
MAX_WORKERS = 8

def read_single_file(file_path):
    """读取单个日志文件并提取原始指标"""
    try:
        with open(file_path, 'r') as f:
            record = json.load(f)
        
        metrics = record.get('clean_metrics', {})
        if not metrics: return None
        
        # 过滤掉指令数过少(运行时间极短)的样本，避免噪音干扰特征
        if metrics.get('instructions', 0) < 10000: return None
        
        return {
            'function': record.get('function', 'unknown'),
            'instructions': metrics.get('instructions', 0),
            'cycles': metrics.get('cycles', 0),
            'task_clock': metrics.get('task-clock', 0.1), # 用于加权
            'llc_misses': metrics.get('LLC-load-misses', 0) or metrics.get('cache-misses', 0),
            'l1_misses': metrics.get('L1-dcache-load-misses', 0),
        }
    except Exception:
        return None

def load_clean_data(log_dir):
    """多线程加载所有去噪后的 JSON 数据"""
    # 适配文件名包含 _clean_ 的情况
    files = list(Path(log_dir).rglob("*_clean_*.json"))
    print(f"[-] Scanning {len(files)} files...")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        data = [r for r in ex.map(read_single_file, files) if r]
    
    return pd.DataFrame(data) if data else pd.DataFrame()

def feature_engineering(df):
    """
    特征工程：只保留与'超线程资源竞争'最直接相关的核心指标
    """
    # 1. 样本权重 (运行时间越长，特征越可信，权重越大)
    df['sample_weight'] = np.log1p(df['task_clock'])
    
    # 2. 核心特征计算
    # IPC: 衡量 ALU/FPU 计算单元的占用率
    df['ipc'] = df['instructions'] / (df['cycles'] + 1e-5)
    
    # LLC MPKI: 衡量内存带宽 (Memory Bandwidth) 的占用率 -> 最关键的干扰源
    df['llc_mpki'] = df['llc_misses'] / (df['instructions'] / 1e3 + 1e-5)
    
    # L1 MPKI: 衡量 L1/L2 私有缓存的占用率 -> 缓存驱逐 (Thrashing) 风险
    df['l1_mpki'] = df['l1_misses'] / (df['instructions'] / 1e3 + 1e-5)

    # --- 优化点：只使用这 3 个维度进行聚类 ---
    # 移除分支预测、上下文切换等次要指标，防止维度稀疏导致的聚类模糊
    feature_cols = ['ipc', 'llc_mpki', 'l1_mpki']
    
    # 清洗数据：去除无穷大和空值
    df_clean = df.replace([np.inf, -np.inf], np.nan).dropna(subset=feature_cols)
    return df_clean, feature_cols

def analyze_and_group(df, feature_cols):
    """K-Means 聚类"""
    X = df[feature_cols].values
    
    # 标准化：将不同量级的特征 (0-3 vs 0-100) 拉伸到同一尺度
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # 加权聚类
    kmeans = KMeans(n_clusters=N_CLUSTERS, random_state=42, n_init=20)
    df['group_id'] = kmeans.fit_predict(X_scaled, sample_weight=df['sample_weight'])
    
    # 生成最终配置：取众数 (Mode) 确定每个函数的最终归属
    final_mapping = {}
    for func, group in df.groupby('function'):
        mode_val = group['group_id'].mode()
        if not mode_val.empty:
            gid = int(mode_val[0])
            final_mapping[func] = gid
    
    return df, final_mapping, X_scaled, kmeans

def print_report(df, cols):
    """打印物理含义分析报告"""
    print("\n=== Optimized Analysis Report (Core Contention Features) ===")
    g_avg = df[cols].mean()
    
    for gid, group in df.groupby('group_id'):
        means = group[cols].mean()
        count = len(group['function'].unique())
        
        # 自动打标签
        tags = []
        if means['ipc'] > 1.2: 
            tags.append("Compute-Bound (高计算)")
        elif means['ipc'] < 0.6:
            tags.append("Stall-Bound (高停顿)")
            
        if means['llc_mpki'] > 5.0: # 阈值可根据实际情况微调
            tags.append("Memory-Bandwidth-Heavy (吃带宽)")
        
        if not tags: tags.append("Balanced/Mixed (混合型)")
        
        label = " + ".join(tags)
        print(f"\n[Group {gid}] -> {label} ({count} tasks)")
        print(f"  Avg Features: IPC={means['ipc']:.2f}, LLC_MPKI={means['llc_mpki']:.2f}, L1_MPKI={means['l1_mpki']:.2f}")
        print(f"  Tasks: {list(group['function'].unique())}")

def visualize_radar(df, cols):
    """绘制雷达图：展示每组的资源偏好"""
    scaler = MinMaxScaler()
    df_norm = pd.DataFrame(scaler.fit_transform(df[cols]), columns=cols)
    centers = df_norm.groupby(df['group_id']).mean()
    
    labels = np.array(cols)
    angles = np.linspace(0, 2*np.pi, len(labels), endpoint=False).tolist() + [0]
    
    plt.figure(figsize=(8, 8))
    ax = plt.subplot(111, polar=True)
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c'] # 蓝、橙、绿
    
    for gid, row in centers.iterrows():
        vals = row.tolist() + row.tolist()[:1]
        ax.plot(angles, vals, linewidth=2, label=f"Group {gid}", color=colors[int(gid)%3])
        ax.fill(angles, vals, alpha=0.15, color=colors[int(gid)%3])
        
    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(labels, fontsize=12)
    plt.title("Resource Intensity Profile (Normalized)", y=1.05)
    plt.legend(loc='upper right', bbox_to_anchor=(1.1, 1.1))
    plt.savefig('grouping_radar_optimized.png', dpi=150)
    print("[+] Radar chart saved.")

def visualize_scatter(df, X_scaled, kmeans):
    """绘制散点图：展示聚类分布"""
    pca = PCA(n_components=2)
    X_pca = pca.fit_transform(X_scaled)
    
    plt.figure(figsize=(12, 8))
    scatter = plt.scatter(X_pca[:, 0], X_pca[:, 1], c=df['group_id'], cmap='viridis', alpha=0.6, s=80)
    
    # 绘制中心点
    centers = pca.transform(kmeans.cluster_centers_)
    plt.scatter(centers[:, 0], centers[:, 1], c='red', s=200, marker='X', alpha=1.0, label='Centroids')
    
    # 智能标签
    texts = []
    for func, sub in df.groupby('function'):
        center_x = np.mean(X_pca[sub.index, 0])
        center_y = np.mean(X_pca[sub.index, 1])
        t = plt.text(center_x, center_y, func, fontsize=9, fontweight='bold',
                     bbox=dict(facecolor='white', alpha=0.7, edgecolor='none', pad=1))
        texts.append(t)

    if HAS_ADJUST_TEXT:
        adjust_text(texts, arrowprops=dict(arrowstyle='-', color='gray', lw=0.5))
    
    plt.title("Task Grouping Distribution (Core Features Only)")
    plt.xlabel("Principal Component 1")
    plt.ylabel("Principal Component 2")
    plt.legend(*scatter.legend_elements(), title="Group ID")
    plt.grid(True, alpha=0.3)
    
    plt.savefig('grouping_scatter_optimized.png', dpi=150)
    print("[+] Scatter plot saved.")

if __name__ == '__main__':
    if not os.path.exists(LOG_DIR):
        print(f"Error: Directory {LOG_DIR} not found.")
        exit(1)
        
    df = load_clean_data(LOG_DIR)
    
    if not df.empty:
        # 1. 特征工程 (仅核心3项)
        df, cols = feature_engineering(df)
        
        # 2. 聚类
        df, mapping, X_scaled, model = analyze_and_group(df, cols)
        
        # 3. 报告与输出
        print_report(df, cols)
        with open(OUTPUT_CONFIG, 'w') as f:
            json.dump(mapping, f, indent=2)
            
        # 4. 可视化
        visualize_radar(df, cols)
        visualize_scatter(df, X_scaled, model)
    else:
        print("No valid data found after filtering.")