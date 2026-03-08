# utils/connections.py
import redis
import couchdb
import requests
from config import REDIS_HOST, REDIS_PORT, COUCHDB_URL, CONTROLLER_URL,TARGET_CONTAINERS

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
                json={"function_name": func_name, "cpuset_cpus": cpuset},
                timeout=5
            )
            status = "OK" if resp.status_code in [200, 201] else f"Failed ({resp.text})"
            print(f"   > Init {func_name}: cpuset={cpuset} ({status})")
        except Exception as e:
            print(f"   > Init {func_name}: Error {e}")

def wait_for_warmup(func_to_group):
    """等待所有函数容器达到目标规模：先保证 total，再保证 idle。"""
    # ===== [Warmup增强开始] =====
    # 详细注释：
    # 1) 先检查 total（总容器数）是否达到目标；
    # 2) total 不足时，主动调用 Controller 的 ensure_warmup 接口触发补容器；
    # 3) total 达标后，再检查 idle，确保容器已经可用于请求。
    print(f"\n[INFO] Waiting for {TARGET_CONTAINERS} containers per function...")
    all_funcs = list(func_to_group.keys())

    while True:
        all_ready = True
        pending = []
        for func_name in all_funcs:
            try:
                resp = requests.get(f"{CONTROLLER_URL}/manager_status/{func_name}", timeout=2)
                if resp.status_code != 200:
                    all_ready = False
                    pending.append(f"{func_name}(status={resp.status_code})")
                    continue

                data = resp.json()
                total_count = data.get("total", 0)

                # 总容器数不足时，立刻触发服务端补齐。
                if total_count < TARGET_CONTAINERS:
                    all_ready = False
                    pending.append(f"{func_name}(total={total_count}/{TARGET_CONTAINERS})")
                    try:
                        # ===== [适配 Warmup 异步化 + 去重 改造开始] =====
                        # 关键说明：
                        # 1) 服务端 ensure_warmup 现在会“快速返回 202”，后台异步补容器；
                        # 2) 202 是“请求已接收/已去重复用在途任务”的正常状态，不应再按失败处理；
                        # 3) 客户端继续依赖 manager_status(total/idle) 轮询判断是否真正预热完成。
                        ensure_resp = requests.post(
                            f"{CONTROLLER_URL}/ensure_warmup/{func_name}",
                            json={"target_total_containers": TARGET_CONTAINERS},
                            timeout=8
                        )
                        if ensure_resp.status_code not in [200, 202]:
                            print(f"[WARN] ensure_warmup {func_name} failed: {ensure_resp.status_code}")
                        # ===== [适配 Warmup 异步化 + 去重 改造结束] =====
                    except Exception as e:
                        print(f"[WARN] ensure_warmup {func_name} exception: {e}")
                    continue

            except Exception as e:
                all_ready = False
                pending.append(f"{func_name}(status=exception)")
                print(f"[WARN] manager_status {func_name} exception: {e}")

        if all_ready:
            print("[INFO] All function containers are warmed up and ready!")
            break

        print(f"   ... Waiting for: {', '.join(pending[:5])} ...")
        import time
        time.sleep(2)
    # ===== [Warmup增强结束] =====
