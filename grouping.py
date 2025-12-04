import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score # 核心评分工具

INPUT_FILE = 'summary.json'
OUTPUT_CONFIG = 'task_groups.json'
N_CLUSTERS = 3

def perform_grouping():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found. Run aggregate_metrics.py first.")
        return

    with open(INPUT_FILE, 'r') as f:
        data = json.load(f)
    
    # 转换为 DataFrame
    rows = []
    for func, info in data.items():
        rows.append({
            "function": func,
            "ipc": info['ipc'],
            "llc_mpki": info['llc_mpki'],
            "l1_mpki": info['l1_mpki']
        })
    
    df = pd.DataFrame(rows)
    print(f"[-] Loaded {len(df)} tasks for grouping.")
    
    # 1. 准备特征数据
    feature_cols = ['ipc', 'llc_mpki', 'l1_mpki']
    X = df[feature_cols].values
    
    # 2. 标准化
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # 3. K-Means 聚类
    kmeans = KMeans(n_clusters=N_CLUSTERS, random_state=42, n_init=50) # n_init加大以寻找最优解
    labels = kmeans.fit_predict(X_scaled)
    df['group_id'] = labels
    
    # 4. 计算得分 (Silhouette Score)
    # 衡量指标：
    # - 接近 1: 分组完美
    # - 接近 0: 边界模糊
    # - 负数: 分组错误
    score = silhouette_score(X_scaled, labels)
    print(f"\n[★] Clustering Performance (Silhouette Score): {score:.4f}")
    if score > 0.5:
        print("    -> Excellent separation!")
    elif score > 0.25:
        print("    -> Good separation.")
    else:
        print("    -> Weak separation (Features might be mixed).")

    # 5. 生成报告 & 物理含义推断
    print("\n=== Group Profiles ===")
    mapping = {}
    
    for gid in range(N_CLUSTERS):
        group = df[df['group_id'] == gid]
        avg = group[feature_cols].mean()
        tasks = group['function'].tolist()
        
        # 简单的自动标签
        tag = "Mixed"
        if avg['ipc'] > 1.5 and avg['llc_mpki'] < 1.0:
            tag = "Compute-Bound (High IPC)"
        elif avg['llc_mpki'] > 5.0:
            tag = "Memory-Bound (High LLC Miss)"
        elif avg['ipc'] < 0.8:
            tag = "IO/Stall-Bound (Low IPC)"
            
        print(f"\n[Group {gid}] Label: {tag}")
        print(f"  - Tasks: {tasks}")
        print(f"  - Centers: IPC={avg['ipc']:.2f}, LLC={avg['llc_mpki']:.2f}, L1={avg['l1_mpki']:.2f}")
        
        for t in tasks:
            mapping[t] = int(gid)

    # 6. 保存结果
    with open(OUTPUT_CONFIG, 'w') as f:
        json.dump(mapping, f, indent=2)
    print(f"\n[+] Grouping saved to {OUTPUT_CONFIG}")
    
    # 7. 可视化 (PCA 2D)
    pca = PCA(n_components=2)
    X_pca = pca.fit_transform(X_scaled)
    
    plt.figure(figsize=(10, 7))
    scatter = plt.scatter(X_pca[:, 0], X_pca[:, 1], c=labels, cmap='viridis', s=100, alpha=0.7)
    
    # 标注文字
    for i, row in df.iterrows():
        plt.text(X_pca[i, 0], X_pca[i, 1], row['function'], fontsize=9)
        
    plt.title(f"Task Grouping (Silhouette Score: {score:.2f})")
    plt.xlabel("PC1")
    plt.ylabel("PC2")
    plt.legend(*scatter.legend_elements(), title="Group")
    plt.grid(True, alpha=0.3)
    plt.savefig('final_grouping_result.png')
    print("[+] Plot saved to final_grouping_result.png")

if __name__ == "__main__":
    import os
    perform_grouping()