import os
import sys
import time
import json
import shutil
import traceback
from flask import Flask, request, jsonify
from gevent.pywsgi import WSGIServer

exec_path = '/proxy/exec/actions'
default_file = 'main.py'

class ActionRunner:
    def __init__(self):
        self.action_name = None
        self.compiled_code = None
        # --- 必须确保这里定义了 file_path ---
        self.file_path = None 

    def init(self, inp):
        """初始化：编译代码"""
        self.action_name = inp['action']
        self.file_path = os.path.join(exec_path, self.action_name, default_file)
        
        if not os.path.exists(self.file_path):
            print(f"[Proxy] Error: File not found {self.file_path}")
            return False

        with open(self.file_path, 'r') as f:
            source = f.read()
        
        self.compiled_code = compile(source, self.file_path, mode='exec')
        print(f"[Proxy] Action '{self.action_name}' initialized.")
        return True

    def run(self, inp):
        """运行：混合模式"""
        # 1. 判断模式
        is_workflow_mode = 'input_mapping' in inp
        
        current_store = None
        env_workdir = f"/tmp/{self.action_name}"

        # 2. 准备上下文
        action_context = {
            "__name__": "__main__",
            # --- 防错处理：如果 file_path 还是 None，给一个默认值 ---
            "__file__": self.file_path if self.file_path else "unknown.py",
            "os": os,
            "json": json,
            "print": print
        }

        # --- 分支处理 ---
        if is_workflow_mode:
            # 动态导入 Store
            sys.path.append('/proxy')
            from store import Store
            
            request_id = inp.get('request_id', 'unknown')
            input_mapping = inp.get('input_mapping', {})
            
            current_store = Store(request_id, input_mapping)
            env_workdir = f"/tmp/{request_id}/{self.action_name}"
            
            action_context["store"] = current_store
            action_context["ENV_WORKDIR"] = env_workdir
            
            if os.path.exists(env_workdir):
                shutil.rmtree(env_workdir)
            os.makedirs(env_workdir, exist_ok=True)
        else:
            # 简单模式
            action_context["store"] = None

        # 3&4. 执行代码并运行 main（统一计时：exec + main 的总时间）
        try:
            if not self.compiled_code:
                # 如果没有编译代码（说明 init 没跑或失败了），尝试现场补救（仅限简单情况）或报错
                raise Exception("Code not initialized. Controller failed to call /init?")
            
            exec(self.compiled_code, action_context)
        except Exception as e:
            print(f"[Proxy] Execution Error: {e}")
            return {"error": str(e), "traceback": traceback.format_exc()}
        
        func_start = time.time()
        
        # 运行 main 函数
        func_result = None
        if 'main' in action_context and callable(action_context['main']):
            try:
                func_result = action_context['main'](inp)
            except Exception as e:
                print(f"[Proxy] Main function error: {e}")
                return {"error": str(e), "traceback": traceback.format_exc()}
        
        func_end = time.time()
        func_duration = func_end - func_start

        # 5. 构造返回
        response = {
            "func_result": func_result,
            #"workdir_used": env_workdir,
            "func_duration": func_duration  # 纯函数执行时间（秒）
        }
        
        if is_workflow_mode and current_store:
            response["output_keys"] = current_store.output_keys
        else:
            response["output_keys"] = {}

        print(f"[Proxy] {self.action_name} func_duration: {func_duration:.6f}s (result keys: {list(response.keys())})")
        return response

proxy = Flask(__name__)
proxy.status = 'new'
runner = ActionRunner()

@proxy.route('/status', methods=['GET'])
def status():
    res = {'status': proxy.status, 'workdir': os.getcwd()}
    if runner.action_name:
        res['action'] = runner.action_name
    return jsonify(res)

@proxy.route('/init', methods=['POST'])
def init():
    try:
        proxy.status = 'init'
        inp = request.get_json(force=True, silent=True)
        if runner.init(inp):
            proxy.status = 'ok'
            return ('OK', 200)
        else:
            return ('Init Failed', 500)
    except Exception as e:
        return (str(e), 500)

@proxy.route('/run', methods=['POST'])
def run():
    proxy.status = 'run'
    try:
        inp = request.get_json(force=True, silent=True) or {}
        
        start = time.time()
        result = runner.run(inp)
        end = time.time()
        
        data = {
            "start_time": start,
            "end_time": end,
            "duration": end - start,
            "result": result
        }
        proxy.status = 'ok'
        return jsonify(data)
    except Exception as e:
        proxy.status = 'error'
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    server = WSGIServer(('0.0.0.0', 5000), proxy)
    server.serve_forever()