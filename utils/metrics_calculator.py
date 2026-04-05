import numpy as np


def compute_stability(times):
    """计算一组数值样本的统计指标。"""
    if not times:
        return {}

    arr = np.array(times)
    mean_value = float(np.mean(arr))
    std_value = float(np.std(arr))

    return {
        "count": len(times),
        "mean": mean_value,
        "variance": float(np.var(arr)),
        "std": std_value,
        "cv": float(std_value / mean_value) if mean_value != 0 else 0.0,
        "min": float(np.min(arr)),
        "max": float(np.max(arr)),
        "iqr": float(np.percentile(arr, 75) - np.percentile(arr, 25)),
        "p90": float(np.percentile(arr, 90)),
        "p95": float(np.percentile(arr, 95)),
        "p99": float(np.percentile(arr, 99)),
    }


def generate_experiment_summary(perf_data, total_time):
    """生成实验汇总数据。"""
    total_completed = sum(len(times) for times in perf_data.values())
    return {
        "total_time": total_time,
        "throughput": total_completed / total_time,
        "completed_requests": total_completed,
    }
