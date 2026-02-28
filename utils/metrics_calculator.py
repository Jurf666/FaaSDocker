# utils/metrics_calculator.py
import numpy as np

def compute_stability(times):
    """计算性能统计指标（纯函数）"""
    if not times:
        return {}
    arr = np.array(times)
    return {
        "count": len(times),
        "mean": float(np.mean(arr)),
        "variance": float(np.var(arr)),
        "std": float(np.std(arr)),
        "cv": float(np.std(arr) / np.mean(arr)) if np.mean(arr) != 0 else 0.0,
        "min": float(np.min(arr)),
        "max": float(np.max(arr)),
        "iqr": float(np.percentile(arr, 75) - np.percentile(arr, 25)),
        "p90": float(np.percentile(arr, 90)),
        "p95": float(np.percentile(arr, 95))
    }

def generate_experiment_summary(perf_data, total_time):
    """生成实验汇总数据"""
    total_completed = sum(len(times) for times in perf_data.values())
    return {
        "total_time": total_time,
        "throughput": total_completed / total_time,
        "completed_requests": total_completed
    }