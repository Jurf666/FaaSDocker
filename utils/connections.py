# utils/connections.py
import redis
import couchdb
import requests
from config import REDIS_HOST, REDIS_PORT, COUCHDB_URL, CONTROLLER_URL,TARGET_IDLE_CONTAINERS

def init_redis_client():
    """初始化Redis连接"""
    try:
        client = redis.StrictRedis(
            host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True
        )
        client.ping()
        print(f"[INFO] Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
        return client
    except Exception as e:
        print(f"[WARN] Redis not available, workflow cache warmup skipped: {e}")
        return None

def init_couchdb_client():
    """初始化CouchDB连接"""
    try:
        client = couchdb.Server(COUCHDB_URL)
        print(f"[INFO] Connected to CouchDB at {COUCHDB_URL}")
        return client
    except Exception as e:
        print(f"[WARN] CouchDB not available, cleanup may be incomplete: {e}")
        return None

def init_controller_managers(cgroup_configs, func_to_group):
    """
    遍历所有函数，根据计算出的分组信息，调用 Controller 的 API 进行初始化。
    这样 Controller 在创建容器时就会直接加上 --cpuset-cpus 参数。
    """
    print("[INFO] Initializing Function Managers on Controller...")
    # 获取所有涉及的函数
    all_funcs = set(func_to_group.keys())
    for func_name in all_funcs:
        group_id = func_to_group[func_name]
        group_name = f"group_{group_id}"
        cpuset = cgroup_configs[group_name]['cpus'] if group_name in cgroup_configs else None
        try:
            resp = requests.post(
                f"{CONTROLLER_URL}/create_manager",
                json={"function_name": func_name, "cpuset_cpus": cpuset ,"min_idle_containers": TARGET_IDLE_CONTAINERS},
                timeout=5
            )
            status = "OK" if resp.status_code in [200, 201] else f"Failed ({resp.text})"
            print(f"   > Init {func_name}: cpuset={cpuset} ({status})")
        except Exception as e:
            print(f"   > Init {func_name}: Error {e}")

def wait_for_warmup(func_to_group):
    """等轮询 Controller，直到所有涉及的函数的 idle 容器数量达到目标值"""
    print(f"\n[INFO] Waiting for {TARGET_IDLE_CONTAINERS} idle containers per function...")
    # 获取所有需要监控的函数名
    all_funcs = list(func_to_group.keys())
    while True:
        all_ready = True
        pending = []
        for func_name in all_funcs:
            try:
                # 调用 Controller 提供的查询接口
                resp = requests.get(f"{CONTROLLER_URL}/manager_status/{func_name}", timeout=2)
                if resp.status_code == 200:
                    data = resp.json()
                    idle_count = data.get("idle", 0)
                    # 检查是否达标
                    if idle_count < TARGET_IDLE_CONTAINERS:
                        all_ready = False
                        pending.append(f"{func_name}({idle_count}/{TARGET_IDLE_CONTAINERS})")
                else:
                    # 如果查询失败，保守起见认为没准备好
                    all_ready = False
            except Exception:
                all_ready = False
        if all_ready:
            print("[INFO] All function containers are warmed up and ready!")
            break
        print(f"   ... Waiting for: {', '.join(pending[:5])} ...")
        import time
        time.sleep(2)