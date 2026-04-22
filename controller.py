import atexit
import logging
import uuid
import time
import threading
from flask import Flask, request, jsonify
from gevent import monkey
monkey.patch_all()
from gevent.pywsgi import WSGIServer

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

# ===== [Warmup异步化 + 同函数在途去重 改造开始] =====
# 设计目标：
# 1) ensure_warmup 接口不再同步阻塞等待补容器完成，而是“快速接单”后立即返回（202）；
# 2) 同一个 function_name 在已有 warmup 任务运行时，不重复创建任务（去重）；
# 3) 如果重复请求带来更高 target，则只更新在途任务的目标，复用同一个 job。
#
# 说明：
# - warmup_jobs: 保存所有 warmup 作业的状态快照，便于后续排查日志；
# - warmup_inflight: 记录每个函数当前在途（queued/running）的作业 id，实现“同函数去重”；
# - warmup_lock: 保护上述两个共享结构，避免多线程竞争导致状态错乱。
warmup_jobs = {}
warmup_inflight = {}
warmup_lock = threading.Lock()


def _run_warmup_job(job_id):
    """
    异步 warmup 执行器（后台线程）。

    核心策略：
    - 每轮读取一次当前 target_total，然后调用 manager.ensure_min_total_containers(target)；
    - 若执行期间被新的请求把 target 提高了（例如从 8 提到 10），则继续下一轮补齐；
    - 若 target 未变化，说明本轮已追平当前目标，作业标记 completed 并退出。
    """
    while True:
        # ---- 第一步：读取任务快照并标记 running ----
        with warmup_lock:
            job = warmup_jobs.get(job_id)
            if not job:
                return
            function_name = job["function_name"]
            target_total = int(job.get("target_total", 0))
            job["status"] = "running"
            job["updated_at"] = time.time()

        # ---- 第二步：获取 manager，并执行一次“补齐到 target” ----
        with dispatcher.manager_lock:
            manager = dispatcher.function_managers.get(function_name)

        if manager is None:
            with warmup_lock:
                job = warmup_jobs.get(job_id)
                if job:
                    job["status"] = "failed"
                    job["error"] = f"unknown function: {function_name}"
                    job["updated_at"] = time.time()
                    if warmup_inflight.get(function_name) == job_id:
                        warmup_inflight.pop(function_name, None)
            return

        try:
            # 这里依旧复用原有补容器逻辑（无需改动 manager 内部实现）。
            result = manager.ensure_min_total_containers(target_total)
        except Exception as e:
            with warmup_lock:
                job = warmup_jobs.get(job_id)
                if job:
                    job["status"] = "failed"
                    job["error"] = str(e)
                    job["updated_at"] = time.time()
                    if warmup_inflight.get(function_name) == job_id:
                        warmup_inflight.pop(function_name, None)
            return

        # ---- 第三步：写回结果 ----
        with warmup_lock:
            job = warmup_jobs.get(job_id)
            if not job:
                return

            job["result"] = result
            job["updated_at"] = time.time()
            job["status"] = "completed"
            if warmup_inflight.get(function_name) == job_id:
                warmup_inflight.pop(function_name, None)
            return
# ===== [Warmup异步化 + 同函数在途去重 改造结束] =====

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
    if not function_name: 
        return jsonify({"error": "function_name required"}), 400
    
    already_exists = function_name in dispatcher.function_managers
    if already_exists:
        return jsonify({"status": "exists"}), 200
    else:
        dispatcher.get_or_create_manager(function_name, cpuset_cpus=cpuset_cpus)
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
        total = sum(1 for d in m.containers.values() if d["container_obj"].status == 'running')
        idle = sum(1 for d in m.containers.values() if d["status"] == "idle" and d["container_obj"].status == 'running')
        busy = sum(1 for d in m.containers.values() if d["status"] == "busy")
        ports = [{"id": cid[:12], "host_port": d.get("host_port")} for cid, d in m.containers.items()]
        
    return jsonify({"function": function_name, "total": total, "idle": idle, "busy": busy, "containers": ports})

# ===== [Warmup增强开始] =====
@app.route('/ensure_warmup/<function_name>', methods=['POST'])
def ensure_warmup(function_name):
    """
    异步触发指定函数的 warmup（补容器）逻辑。

    改造点：
    1) 接口立即返回 202，不再同步阻塞等待补容器完成（避免客户端 Read timeout）；
    2) 同函数在途去重：若已有 queued/running 任务，则复用该任务；
    3) 去重时允许“只升不降”地提高 target_total_containers。
    """
    body = request.get_json(silent=True) or {}
    target_total = body.get("target_total_containers")
    try:
        target_total = int(target_total)
    except (TypeError, ValueError):
        target_total = 0
    if target_total < 0:
        target_total = 0

    with dispatcher.manager_lock:
        if function_name not in dispatcher.function_managers:
            return jsonify({"error": "unknown function"}), 404

    now = time.time()
    with warmup_lock:
        inflight_job_id = warmup_inflight.get(function_name)
        if inflight_job_id:
            # ---- 命中“同函数在途任务”时：不新建任务，直接去重复用 ----
            existing_job = warmup_jobs.get(inflight_job_id)
            if existing_job and existing_job.get("status") in ("queued", "running"):
                existing_job["updated_at"] = now
                return jsonify({
                    "accepted": True,
                    "deduplicated": True,
                    "job_id": inflight_job_id,
                    "function": function_name,
                    "status": existing_job.get("status"),
                    "target_total": existing_job.get("target_total"),
                }), 202

        # ---- 未命中在途任务：新建异步 warmup 作业 ----
        job_id = f"warmup-{uuid.uuid4().hex[:8]}"
        warmup_jobs[job_id] = {
            "job_id": job_id,
            "function_name": function_name,
            "status": "queued",
            "target_total": target_total,
            "result": None,
            "error": None,
            "created_at": now,
            "updated_at": now,
        }
        warmup_inflight[function_name] = job_id

    # 在锁外启动后台线程，避免持锁启动线程造成额外阻塞。
    threading.Thread(target=_run_warmup_job, args=(job_id,), daemon=True).start()
    return jsonify({
        "accepted": True,
        "deduplicated": False,
        "job_id": job_id,
        "function": function_name,
        "status": "queued",
        "target_total": target_total,
    }), 202
# ===== [Warmup增强结束] =====


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

# ── 多轮自动化实验同步接口 ────────────────────────────────────────────────────
# 状态机：
#   "not_ready"   → controller 启动后默认状态，客户端轮询等待
#   "ready"       → 服务器调用 /set_ready 后设置，客户端收到后开始 run.py
#   "client_done" → 客户端完成后调用 /client_done，服务器脚本收到后继续清理下一轮
_experiment_state = {"status": "not_ready", "round": 0, "mode": ""}
_experiment_state_lock = threading.Lock()
_client_done_event = threading.Event()


@app.route('/set_ready', methods=['POST'])
def set_ready():
    """服务器端脚本调用：标记当前轮次已就绪，通知客户端可以开始。"""
    body = request.get_json(silent=True) or {}
    with _experiment_state_lock:
        _experiment_state["status"] = "ready"
        _experiment_state["round"] = body.get("round", 0)
        _experiment_state["mode"] = body.get("mode", "")
        _client_done_event.clear()
    return jsonify({"status": "ready", **_experiment_state}), 200


@app.route('/experiment_ready', methods=['GET'])
def experiment_ready():
    """客户端轮询：查询服务器是否就绪。就绪返回200+ready，否则返回200+not_ready。"""
    with _experiment_state_lock:
        state = dict(_experiment_state)
    return jsonify(state), 200


@app.route('/client_done', methods=['POST'])
def client_done():
    """客户端完成后调用：通知服务器本轮客户端已跑完。"""
    body = request.get_json(silent=True) or {}
    with _experiment_state_lock:
        _experiment_state["status"] = "client_done"
    _client_done_event.set()
    return jsonify({"status": "client_done", "round": body.get("round", 0)}), 200


@app.route('/wait_client_done', methods=['GET'])
def wait_client_done():
    """服务器端脚本轮询：查询客户端是否已完成本轮。"""
    done = _client_done_event.is_set()
    with _experiment_state_lock:
        state = dict(_experiment_state)
    return jsonify({"client_done": done, **state}), 200


@app.route('/freq_stable', methods=['GET'])
def freq_stable():
    """查询 server CPU 频率是否已稳定（供 client 在实验开始前轮询）。
    稳定条件：所有在线 CPU 的当前频率与最大频率之比 >= 0.95，
    且连续两次采样（间隔 1s）的频率变化 < 100 MHz。
    """
    import glob
    import os

    def read_khz(path):
        try:
            return int(open(path).read().strip())
        except Exception:
            return None

    cur_files = sorted(glob.glob("/sys/devices/system/cpu/cpu*/cpufreq/scaling_cur_freq"))
    max_files = sorted(glob.glob("/sys/devices/system/cpu/cpu*/cpufreq/scaling_max_freq"))

    if not cur_files:
        return jsonify({"stable": True, "reason": "no cpufreq sysfs"}), 200

    sample1 = [read_khz(f) for f in cur_files]
    time.sleep(1)
    sample2 = [read_khz(f) for f in cur_files]
    max_freqs = [read_khz(f) for f in max_files]

    valid = [(s1, s2, mx) for s1, s2, mx in zip(sample1, sample2, max_freqs)
             if s1 and s2 and mx]
    if not valid:
        return jsonify({"stable": True, "reason": "unreadable"}), 200

    avg_cur = sum(s2 for _, s2, _ in valid) / len(valid)
    avg_max = sum(mx for _, _, mx in valid) / len(valid)
    max_delta_khz = max(abs(s2 - s1) for s1, s2, _ in valid)
    ratio = avg_cur / avg_max if avg_max else 1.0

    stable = ratio >= 0.95 and max_delta_khz < 100_000  # 100 MHz in kHz
    return jsonify({
        "stable": stable,
        "avg_cur_mhz": round(avg_cur / 1000, 1),
        "avg_max_mhz": round(avg_max / 1000, 1),
        "ratio": round(ratio, 4),
        "max_delta_mhz": round(max_delta_khz / 1000, 1),
    }), 200
# ── 多轮自动化实验同步接口 结束 ───────────────────────────────────────────────


if __name__ == '__main__':
    server = WSGIServer(('0.0.0.0', 5002), app)
    print("Controller listening on port 5002 (gevent)...")
    server.serve_forever()
