import json
import os
import shutil
import sys
import time
import traceback

from flask import Flask, jsonify, request
from gevent.pywsgi import WSGIServer

exec_path = "/proxy/exec/actions"
default_file = "main.py"

CGROUP_CPU_STAT_PATHS = (
    "/sys/fs/cgroup/cpu.stat",
    "/sys/fs/cgroup/cpu/cpu.stat",
)
CGROUP_CPU_USAGE_PATHS = (
    "/sys/fs/cgroup/cpuacct.usage",
    "/sys/fs/cgroup/cpu/cpuacct.usage",
)


def _read_cgroup_cpu_stat():
    """Read cpu.stat from either cgroup v2 or cgroup v1 layout."""
    for stat_path in CGROUP_CPU_STAT_PATHS:
        if not os.path.exists(stat_path):
            continue

        try:
            stats = {}
            with open(stat_path, "r", encoding="utf-8") as f:
                for line in f:
                    parts = line.strip().split()
                    if len(parts) != 2:
                        continue
                    key, value = parts
                    try:
                        stats[key] = int(value)
                    except ValueError:
                        continue
            return stats, stat_path
        except Exception:
            continue

    return None, None


def _extract_throttled_time_ns(stats):
    if not isinstance(stats, dict):
        return None
    if "throttled_time" in stats:
        return int(stats["throttled_time"])
    if "throttled_nsec" in stats:
        return int(stats["throttled_nsec"])
    if "throttled_usec" in stats:
        return int(stats["throttled_usec"]) * 1000
    return None


def _build_cgroup_throttle_delta(before_stats, after_stats):
    if not isinstance(before_stats, dict) or not isinstance(after_stats, dict):
        return {}

    nr_periods_before = before_stats.get("nr_periods")
    nr_periods_after = after_stats.get("nr_periods")
    nr_throttled_before = before_stats.get("nr_throttled")
    nr_throttled_after = after_stats.get("nr_throttled")
    throttled_time_before_ns = _extract_throttled_time_ns(before_stats)
    throttled_time_after_ns = _extract_throttled_time_ns(after_stats)

    nr_periods_delta = (
        nr_periods_after - nr_periods_before
        if nr_periods_before is not None and nr_periods_after is not None
        else None
    )
    nr_throttled_delta = (
        nr_throttled_after - nr_throttled_before
        if nr_throttled_before is not None and nr_throttled_after is not None
        else None
    )
    throttled_time_ns_delta = (
        throttled_time_after_ns - throttled_time_before_ns
        if throttled_time_before_ns is not None and throttled_time_after_ns is not None
        else None
    )

    throttle_ratio = None
    if nr_periods_delta not in (None, 0) and nr_throttled_delta is not None:
        throttle_ratio = nr_throttled_delta / nr_periods_delta

    return {
        "cgroup_nr_periods_delta": nr_periods_delta,
        "cgroup_nr_throttled_delta": nr_throttled_delta,
        "cgroup_throttled_time_ns_delta": throttled_time_ns_delta,
        "cgroup_throttled_time_seconds_delta": (
            throttled_time_ns_delta / 1_000_000_000.0
            if throttled_time_ns_delta is not None
            else None
        ),
        "cgroup_throttle_ratio_delta": throttle_ratio,
    }


def _extract_usage_ns_from_cpu_stat(stats):
    if not isinstance(stats, dict):
        return None
    if "usage_usec" in stats:
        return int(stats["usage_usec"]) * 1000
    if "usage_nsec" in stats:
        return int(stats["usage_nsec"])
    return None


def _read_cgroup_cpu_usage_ns():
    """
    Read cumulative CPU usage of the current container cgroup.
    Priority:
      1) cgroup v2 cpu.stat -> usage_usec / usage_nsec
      2) cgroup v1 cpuacct.usage -> nanoseconds
    Returns: (usage_ns_or_none, source_path_or_none)
    """
    cpu_stat, cpu_stat_path = _read_cgroup_cpu_stat()
    usage_ns = _extract_usage_ns_from_cpu_stat(cpu_stat)
    if usage_ns is not None:
        return usage_ns, cpu_stat_path

    for usage_path in CGROUP_CPU_USAGE_PATHS:
        if not os.path.exists(usage_path):
            continue
        try:
            with open(usage_path, "r", encoding="utf-8") as f:
                raw = f.read().strip()
            return int(raw), usage_path
        except Exception:
            continue

    return None, None


class ActionRunner:
    def __init__(self):
        self.action_name = None
        self.compiled_code = None
        self.file_path = None

    def init(self, inp):
        """初始化：读取并编译 action 代码。"""
        self.action_name = inp["action"]
        self.file_path = os.path.join(exec_path, self.action_name, default_file)

        if not os.path.exists(self.file_path):
            print(f"[Proxy] Error: File not found {self.file_path}")
            return False

        with open(self.file_path, "r") as f:
            source = f.read()

        self.compiled_code = compile(source, self.file_path, mode="exec")
        print(f"[Proxy] Action '{self.action_name}' initialized.")
        return True

    def run(self, inp):
        """
        执行 action（simple/workflow 通用）。

        统计口径（重要）：
        - 仅统计 runner.run() 内部的 exec + main 区间
        - 不统计 /run 路由外层包络时间

        说明：
        - 字段名沿用 func_main_* 以兼容上层代码；
        - 但当前语义是 exec+main 的整体区间。
        """
        is_workflow_mode = "input_mapping" in inp
        current_store = None
        env_workdir = f"/tmp/{self.action_name}"

        action_context = {
            "__name__": "__main__",
            "__file__": self.file_path if self.file_path else "unknown.py",
            "os": os,
            "json": json,
            "print": print,
        }

        if is_workflow_mode:
            sys.path.append("/proxy")
            from store import Store

            request_id = inp.get("request_id", "unknown")
            input_mapping = inp.get("input_mapping", {})

            current_store = Store(request_id, input_mapping)
            env_workdir = f"/tmp/{request_id}/{self.action_name}"

            action_context["store"] = current_store
            action_context["ENV_WORKDIR"] = env_workdir

            if os.path.exists(env_workdir):
                shutil.rmtree(env_workdir)
            os.makedirs(env_workdir, exist_ok=True)
        else:
            action_context["store"] = None

        # ===== [修改标记-ns 精度统计] =====
        # 起点放在 exec 前：计入 exec + main
        func_main_start_ns = time.monotonic_ns()
        func_cpu_start_ns = time.process_time_ns()
        cgroup_cpu_stat_before, cgroup_cpu_stat_path = _read_cgroup_cpu_stat()
        container_cpu_usage_before_ns, container_cpu_usage_source = _read_cgroup_cpu_usage_ns()
        try:
            if not self.compiled_code:
                raise Exception("Code not initialized. Controller failed to call /init?")
            exec(self.compiled_code, action_context)
        except Exception as e:
            print(f"[Proxy] Execution Error: {e}")
            return {"error": str(e), "traceback": traceback.format_exc()}

        func_result = None
        if "main" in action_context and callable(action_context["main"]):
            try:
                func_result = action_context["main"](inp)
            except Exception as e:
                print(f"[Proxy] Main function error: {e}")
                return {"error": str(e), "traceback": traceback.format_exc()}
        func_main_end_ns = time.monotonic_ns()
        func_cpu_end_ns = time.process_time_ns()
        cgroup_cpu_stat_after, _ = _read_cgroup_cpu_stat()
        container_cpu_usage_after_ns, _ = _read_cgroup_cpu_usage_ns()

        func_duration_ns = func_main_end_ns - func_main_start_ns
        func_duration = func_duration_ns / 1_000_000_000.0
        process_cpu_time_ns = func_cpu_end_ns - func_cpu_start_ns
        process_cpu_time = process_cpu_time_ns / 1_000_000_000.0
        container_cpu_usage_ns = (
            container_cpu_usage_after_ns - container_cpu_usage_before_ns
            if container_cpu_usage_before_ns is not None and container_cpu_usage_after_ns is not None
            else None
        )
        container_cpu_usage_time = (
            container_cpu_usage_ns / 1_000_000_000.0
            if container_cpu_usage_ns is not None
            else None
        )
        cgroup_throttle_delta = _build_cgroup_throttle_delta(
            cgroup_cpu_stat_before,
            cgroup_cpu_stat_after,
        )

        response = {
            "func_result": func_result,
            "func_duration": func_duration,
            "func_main_start_ns": func_main_start_ns,
            "func_main_end_ns": func_main_end_ns,
            "func_duration_ns": func_duration_ns,
            # backward-compatible process-level CPU time fields
            "func_cpu_time": process_cpu_time,
            "func_cpu_time_ns": process_cpu_time_ns,
            "process_cpu_time": process_cpu_time,
            "process_cpu_time_ns": process_cpu_time_ns,
            # container/cgroup-level CPU time fields
            "container_cpu_time": container_cpu_usage_time,
            "container_cpu_time_ns": container_cpu_usage_ns,
            "cgroup_cpu_stat_path": cgroup_cpu_stat_path,
            "cgroup_cpu_usage_source": container_cpu_usage_source,
        }
        response.update(cgroup_throttle_delta)

        if is_workflow_mode and current_store:
            response["output_keys"] = current_store.output_keys
        else:
            response["output_keys"] = {}

        container_cpu_log = (
            f"{container_cpu_usage_time:.6f}s"
            if container_cpu_usage_time is not None
            else "N/A"
        )
        print(
            f"[Proxy] {self.action_name} func_duration: {func_duration:.6f}s "
            f"({func_duration_ns}ns), process_cpu: {process_cpu_time:.6f}s, "
            f"container_cpu: {container_cpu_log}"
        )
        return response


proxy = Flask(__name__)
proxy.status = "new"
runner = ActionRunner()


@proxy.route("/status", methods=["GET"])
def status():
    res = {"status": proxy.status, "workdir": os.getcwd()}
    if runner.action_name:
        res["action"] = runner.action_name
    return jsonify(res)


@proxy.route("/init", methods=["POST"])
def init():
    try:
        proxy.status = "init"
        inp = request.get_json(force=True, silent=True)
        if runner.init(inp):
            proxy.status = "ok"
            return ("OK", 200)
        return ("Init Failed", 500)
    except Exception as e:
        return (str(e), 500)


@proxy.route("/run", methods=["POST"])
def run():
    """
    /run 仅负责触发 runner.run() 并回传其结果。
    不再额外统计整个 /run 包络耗时，避免口径混淆。
    """
    proxy.status = "run"
    try:
        inp = request.get_json(force=True, silent=True) or {}
        result = runner.run(inp)
        proxy.status = "ok"
        return jsonify({"result": result})
    except Exception as e:
        proxy.status = "error"
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    server = WSGIServer(("0.0.0.0", 5000), proxy)
    server.serve_forever()
