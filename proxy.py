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

        func_duration_ns = func_main_end_ns - func_main_start_ns
        func_duration = func_duration_ns / 1_000_000_000.0

        response = {
            "func_result": func_result,
            "func_duration": func_duration,
            "func_main_start_ns": func_main_start_ns,
            "func_main_end_ns": func_main_end_ns,
            "func_duration_ns": func_duration_ns,
        }

        if is_workflow_mode and current_store:
            response["output_keys"] = current_store.output_keys
        else:
            response["output_keys"] = {}

        print(f"[Proxy] {self.action_name} func_duration: {func_duration:.6f}s ({func_duration_ns}ns)")
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
