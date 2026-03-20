"""
core_overlap_analyzer.py
========================

这个文件的目标：
----------------
给定一次压测期间收集到的 execution_samples（每次函数调用一条），
计算“同一个物理核上，某次调用执行期间与哪些函数并发重叠”。

为什么需要这个分析：
--------------------
你的 baseline 里会做请求级绑核（同一请求绑定到某个物理核的 SMT 线程对），
因此我们可以把每次调用看成一个时间区间 [start, end]，
再按“物理核”分桶，做区间重叠计算，就能得到：
1) 某次调用启动瞬间，核上已经在跑哪些函数（active_at_start）
2) 整个调用区间内，与哪些函数有重叠，重叠时长多少（overlap）

精度策略：
---------
- 优先使用 ns（纳秒）字段做分析（适合毫秒级函数）
- 若 ns 字段缺失，则回退到秒字段（向后兼容）
"""

from collections import Counter, defaultdict


# 常量：用于 ns / s / ms 之间换算
NS_PER_S = 1_000_000_000
NS_PER_MS = 1_000_000


def _to_float(value):
    """
    尝试把输入转成 float。
    失败时返回 None，而不是抛异常。
    """
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value):
    """
    尝试把输入转成 int。
    失败时返回 None。
    """
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _seconds_to_ns(value):
    """
    将“秒”字段（可能是字符串或浮点）转为 ns 整数。
    例如：0.005 -> 5_000_000
    """
    v = _to_float(value)
    if v is None:
        return None
    return int(round(v * NS_PER_S))


def _normalize_sample(sample):
    """
    把原始 sample 标准化为统一结构；不满足分析条件时返回 None。

    标准化做了什么：
    1) 解析 physical_cores，要求能定位到“唯一物理核”
       - baseline 请求级绑核场景下，通常就是 1 个物理核
       - 若不是 1 个（0 或 >1），本条样本跳过
    2) 统一时间口径为 ns
       - 优先 start_ns/end_ns/duration_ns
       - 缺失时回退 start_time/end_time/duration（秒）
    3) 若边界不完整，但 duration 存在，则尽量修复
       - 有 end + duration -> 推 start
       - 有 start + duration -> 推 end
    4) 最终必须满足 end_ns > start_ns
    """
    cores = sample.get("physical_cores") or []
    if isinstance(cores, (int, float, str)):
        cores = [cores]

    normalized_cores = []
    for c in cores:
        try:
            normalized_cores.append(int(c))
        except (TypeError, ValueError):
            continue

    # baseline 请求级绑核预期：一条请求只映射到一个物理核
    if len(normalized_cores) != 1:
        return None

    # 优先读 ns 字段
    start_ns = _to_int(sample.get("start_ns"))
    end_ns = _to_int(sample.get("end_ns"))
    duration_ns = _to_int(sample.get("duration_ns"))

    # 缺失时回退到秒字段并转换为 ns
    if start_ns is None:
        start_ns = _seconds_to_ns(sample.get("start_time"))
    if end_ns is None:
        end_ns = _seconds_to_ns(sample.get("end_time"))
    if duration_ns is None:
        duration_ns = _seconds_to_ns(sample.get("duration"))

    # 边界不完整时，用 duration 尽量补齐
    if (start_ns is None or end_ns is None or end_ns <= start_ns) and duration_ns and duration_ns > 0:
        if end_ns is not None:
            start_ns = end_ns - duration_ns
        elif start_ns is not None:
            end_ns = start_ns + duration_ns

    # 仍无法形成有效区间，则跳过样本
    if start_ns is None or end_ns is None or end_ns <= start_ns:
        return None

    # 统一以区间边界重算 duration，避免口径漂移
    duration_ns = end_ns - start_ns

    # 这里构造的是“分析中间态”，后续 sweep-line 会在此基础上填充重叠信息
    return {
        "client_request_id": sample.get("client_request_id"),
        "server_request_id": sample.get("server_request_id"),
        "function_name": sample.get("function_name"),
        "container_id": sample.get("container_id"),
        "cpuset": sample.get("cpuset"),
        "physical_core": normalized_cores[0],
        "start_ns": start_ns,
        "end_ns": end_ns,
        "duration_ns": duration_ns,
        # 启动瞬间正在同核执行的函数计数，如 {"image": 2, "network": 1}
        "active_at_start": Counter(),
        # 与其他函数累计重叠时长（ns），如 {"image": 5000000, "disk": 1200000}
        "overlap_by_function_ns": defaultdict(int),
        # 与本次调用发生过重叠的“请求集合”（用于统计 peer_request_count）
        "peer_requests": set(),
    }


def analyze_same_core_overlaps(execution_samples):
    """
    主入口：分析同物理核并发重叠。

    输入：
      execution_samples: list[dict]
        每个元素代表一次调用，至少应包含函数名、物理核、时间边界。

    输出：
      dict:
        - function_level_summary: 按函数聚合
        - core_level_summary: 按物理核聚合
        - per_invocation: 每次调用的详细重叠结果

    算法思路（核心）：
    -----------------
    1) 先标准化样本（_normalize_sample）
    2) 按 physical_core 分组
    3) 每个核内按 start_ns 排序后做 sweep-line：
       - active 列表保存“当前仍可能与后续区间重叠”的调用
       - 对每个新到的 rec：
         a) 先清理 active 中已结束的区间
         b) active 中剩余区间就是“rec 启动瞬间正在运行”的请求
         c) rec 与 active 做两两区间交集，累计 overlap_ns
    """
    normalized = []
    skipped = 0
    for sample in execution_samples:
        item = _normalize_sample(sample)
        if item is None:
            skipped += 1
            continue
        normalized.append(item)

    # 先按物理核分桶；不同核之间天然不会“同核并发”
    by_core = defaultdict(list)
    for item in normalized:
        by_core[item["physical_core"]].append(item)

    # ---- sweep-line（逐核）----
    for _, records in by_core.items():
        # 先按开始时间排序；开始时间相同再按结束时间
        records.sort(key=lambda x: (x["start_ns"], x["end_ns"]))
        active = []

        for rec in records:
            # 1) 移除已经结束的区间（end <= rec.start 不再可能重叠）
            active = [a for a in active if a["end_ns"] > rec["start_ns"]]

            # 2) active 里还在的区间，就是 rec 开始时“已经在跑”的请求
            for a in active:
                rec["active_at_start"][a["function_name"]] += 1

            # 3) rec 与 active 做区间交集，统计重叠时长
            for a in active:
                overlap_start = max(rec["start_ns"], a["start_ns"])
                overlap_end = min(rec["end_ns"], a["end_ns"])
                if overlap_end <= overlap_start:
                    continue

                overlap_ns = overlap_end - overlap_start
                rec["overlap_by_function_ns"][a["function_name"]] += overlap_ns
                a["overlap_by_function_ns"][rec["function_name"]] += overlap_ns

                # 记录“发生过重叠”的请求对
                rec["peer_requests"].add(a["server_request_id"] or a["client_request_id"])
                a["peer_requests"].add(rec["server_request_id"] or rec["client_request_id"])

            # 4) 当前 rec 入 active，供后续区间比较
            active.append(rec)

    # ---- 汇总输出结构 ----
    detail_rows = []
    function_summary_ns = defaultdict(lambda: defaultdict(int))
    core_summary = defaultdict(lambda: {"invocations": 0, "with_overlap": 0})

    for rec in normalized:
        duration_ns = rec["duration_ns"] if rec["duration_ns"] > 0 else 1

        # 当前调用与其他函数重叠的列表，按重叠时长降序
        overlap_items = sorted(
            rec["overlap_by_function_ns"].items(),
            key=lambda kv: kv[1],
            reverse=True,
        )

        co_running = []
        for fname, overlap_ns in overlap_items:
            co_running.append(
                {
                    "function_name": fname,
                    "overlap_ns": overlap_ns,
                    "overlap_ms": round(overlap_ns / NS_PER_MS, 6),
                    "overlap_ratio": round(overlap_ns / duration_ns, 6),
                }
            )
            function_summary_ns[rec["function_name"]][fname] += overlap_ns

        has_overlap = len(co_running) > 0
        core_summary[rec["physical_core"]]["invocations"] += 1
        if has_overlap:
            core_summary[rec["physical_core"]]["with_overlap"] += 1

        # 每次调用的详细记录（最关键的可解释数据）
        detail_rows.append(
            {
                "server_request_id": rec["server_request_id"],
                "client_request_id": rec["client_request_id"],
                "function_name": rec["function_name"],
                "physical_core": rec["physical_core"],
                "cpuset": rec["cpuset"],
                "container_id": rec["container_id"],
                "start_ns": rec["start_ns"],
                "end_ns": rec["end_ns"],
                "duration_ns": rec["duration_ns"],
                "duration_ms": round(rec["duration_ns"] / NS_PER_MS, 6),
                "duration_seconds": rec["duration_ns"] / NS_PER_S,
                "active_at_start": dict(rec["active_at_start"]),
                "co_running_functions": co_running,
                "peer_request_count": len(rec["peer_requests"]),
            }
        )

    # ---- 按函数聚合 ----
    function_level = {}
    grouped_by_func = defaultdict(list)
    for row in detail_rows:
        grouped_by_func[row["function_name"]].append(row)

    for func, rows in grouped_by_func.items():
        total = len(rows)
        with_overlap = sum(1 for r in rows if r["co_running_functions"])
        avg_peer_count = (sum(r["peer_request_count"] for r in rows) / total) if total else 0.0

        overlap_rank = sorted(
            function_summary_ns[func].items(),
            key=lambda kv: kv[1],
            reverse=True,
        )

        function_level[func] = {
            "invocations": total,
            "invocations_with_overlap": with_overlap,
            "overlap_hit_rate": (with_overlap / total) if total else 0.0,
            "avg_peer_request_count": avg_peer_count,
            "top_co_running_functions": [
                {
                    "function_name": f,
                    "total_overlap_ns": v,
                    "total_overlap_ms": round(v / NS_PER_MS, 6),
                }
                for f, v in overlap_rank[:10]
            ],
        }

    # ---- 按物理核聚合 ----
    core_level = {}
    for core, info in core_summary.items():
        inv = info["invocations"]
        ov = info["with_overlap"]
        core_level[str(core)] = {
            "invocations": inv,
            "invocations_with_overlap": ov,
            "overlap_hit_rate": (ov / inv) if inv else 0.0,
        }

    # 输出前排序：按时间先后，方便人工排查
    detail_rows.sort(key=lambda x: (x["start_ns"], x["function_name"]))

    return {
        "time_precision": "nanoseconds_preferred_with_seconds_fallback",
        "total_samples": len(execution_samples),
        "analyzable_samples": len(normalized),
        "skipped_samples": skipped,
        "function_level_summary": function_level,
        "core_level_summary": core_level,
        "per_invocation": detail_rows,
    }
