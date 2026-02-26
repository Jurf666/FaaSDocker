import atexit
import logging
import uuid
from flask import Flask, request, jsonify

# 引入我们拆分的模块
from modulesOfController.data_store import DataStore
from modulesOfController.dispatcher import Dispatcher
from modulesOfController.monitor import SystemMonitor
from modulesOfController.workflow_engine import WorkflowEngine

# --- 日志配置 ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

app = Flask(__name__)

# --- 组件初始化 ---
data_store = DataStore()
dispatcher = Dispatcher()
monitor = SystemMonitor()
wf_engine = WorkflowEngine(dispatcher, data_store)

# --- 退出清理 ---
atexit.register(dispatcher.clean_up)
atexit.register(monitor.stop)

# -------------------------------------------------------------------------
# HTTP 接口
# -------------------------------------------------------------------------

@app.route('/create_manager', methods=['POST'])
def create_manager():
    body = request.get_json(silent=True) or {}
    function_name = body.get("function_name")
    cpuset_cpus = body.get("cpuset_cpus")
    min_idle_containers = body.get("min_idle_containers")
    if not function_name: 
        return jsonify({"error": "function_name required"}), 400
    
    already_exists = function_name in dispatcher.function_managers
    if already_exists:
        return jsonify({"status": "exists"}), 200
    else:
        dispatcher.get_or_create_manager(function_name, cpuset_cpus=cpuset_cpus ,min_idle_containers = min_idle_containers)
        return jsonify({"status": "created"}), 201

@app.route('/dispatch_workflow', methods=['POST'])
def dispatch_workflow():
    body = request.get_json(silent=True) or {}
    name = body.get("workflow_name")
    payload = body.get("payload", {})
    
    task_id = f"task-{uuid.uuid4().hex[:8]}"
    # 启动线程
    wf_engine.submit_task(name, payload, task_id)
    
    return jsonify({
        "status": "accepted", 
        "task_id": task_id,
        "message": "Task running in background"
    }), 202

@app.route('/check_task/<task_id>', methods=['GET'])
def check_task(task_id):
    """
    查询状态接口
    用户拿着上面的 ID 来轮询：“我的任务跑完了吗？”
    返回状态：running / completed / failed。
    """
    info = data_store.get_task_info(task_id)
    return jsonify({"task_id": task_id, **info}), 200

@app.route('/start_monitor', methods=['POST'])
def start_monitor():
    body = request.get_json(silent=True) or {}
    configs = body.get("cgroup_configs", {})
    monitor.start(configs)
    return jsonify({"status": "Monitor started"}), 200

@app.route('/stop_monitor', methods=['POST'])
def stop_monitor():
    monitor.stop()
    return jsonify({"status": "Monitor stopped"}), 200

@app.route('/manager_status/<function_name>', methods=['GET'])
def manager_status(function_name):
    with dispatcher.manager_lock:
        if function_name not in dispatcher.function_managers:
            return jsonify({"error": "unknown function"}), 404
        m = dispatcher.function_managers[function_name]
        
    with m.lock:
        total = len(m.containers)
        idle = sum(1 for d in m.containers.values() if d["status"] == "idle")
        busy = sum(1 for d in m.containers.values() if d["status"] == "busy")
        ports = [{"id": cid[:12], "host_port": d.get("host_port")} for cid, d in m.containers.items()]
        
    return jsonify({"function": function_name, "total": total, "idle": idle, "busy": busy, "containers": ports})

# --- 通用 Dispatch 接口 ---
@app.route('/dispatch/<function_name>', methods=['POST'])
def dispatch_single(function_name):
    payload = request.get_json(silent=True) or {}
    is_workflow = payload.pop('is_workflow', None)
    try:
        out = dispatcher.dispatch_sync(function_name, payload, is_workflow)
        return jsonify({"status": "success", "output": out}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5002, threaded=True)