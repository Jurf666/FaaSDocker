import docker
import time
import threading
import os
import requests

class FunctionManager:
    def __init__(self, function_name, image_name, container_port,  host_port_start=8000, idle_timeout=300, min_idle_containers=4,cpuset_cpus=None):
        self.function_name = function_name
        self.image_name = image_name
        self.container_port = container_port
        self.host_port_start = host_port_start
        self.idle_timeout = idle_timeout
        self.min_idle_containers = min_idle_containers
        self.cpuset_cpus = cpuset_cpus
        self.docker_client = docker.from_env()
        self.containers = {}  # 核心数据结构：字典。Key是容器ID，Value是容器的状态信息
        # {container_id: {"container_obj": ..., "status": "idle/busy", "last_active": timestamp, "host_port": ...}}
        self.lock = threading.Lock()# 线程锁，防止多个线程同时修改 self.containers 导致数据错乱
        self._cleaner_stop_event = threading.Event()# 用于优雅地停止后台清理线程

        self.cleaner_thread = threading.Thread(target=self._run_cleaner, daemon=True)# 创建后台清理线程
        self.cleaner_thread.start()# 启动后台清理线程
    
        
        print(f"FunctionManager for {self.function_name} initialized.")
        
    """
    健康检查方法，用于确保容器启动后，里面的 Web 服务真的可以接客了。
        timeout: 总超时时间(秒)
        check_interval: 每次轮询前 sleep 的时间(秒), 此处设置为10ms
    """
    def _wait_for_container_service(self, host_port, timeout=30, check_interval=0.01):
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                # 向容器映射到宿主机的端口发送请求
                response = requests.get(f"http://127.0.0.1:{host_port}/status", timeout=check_interval)
                if response.status_code == 200:
                    try:
                        data = response.json()
                    except Exception:
                        data = {}
                    if data.get("status") in ["new", "ok", "ready"]:
                        print(f"Container service on port {host_port} is ready.")
                        return True
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                pass# 如果连接不上，说明服务还没起起来，忽略错误继续重试
            time.sleep(check_interval)# 稍微睡一会再试
        print(f"Container service on port {host_port} did not become ready within {timeout} seconds.")
        return False

    """
    创建新容器方法
    """
    def _create_new_container(self):
        # 生成一个随机名字，防止重名
        container_name = f"{self.function_name}-{os.urandom(4).hex()}"
        try:
            print(f"Creating new container '{container_name}' ...")

            # --- 准备 docker run 的参数 ---
            run_kwargs = {
                "detach": True,# 后台运行
                "ports": {f"{self.container_port}/tcp": None}, # 重要：Value为None表示让Docker随机分配宿主机端口
                "name": container_name,
                "nano_cpus": 200000000,# 限制容器只能使用 0.2 个 CPU 核的算力
            }
            if self.cpuset_cpus:
                run_kwargs['cpuset_cpus'] = self.cpuset_cpus  # <--- 关键修改：原生绑核
            # --- 调用 Docker API 启动容器 ---
            container = self.docker_client.containers.run( #
                self.image_name,
                **run_kwargs# 展开参数字典
            )
            print(f"Created container id={container.id:12}")
        except docker.errors.ImageNotFound:
            print(f"Error: Image '{self.image_name}' not found.")
            return None
        except Exception as e:
            print(f"Error creating container '{container_name}': {e}")
            return None

        # 等待 Docker 完成端口映射
        '''
        Docker 启动容器是异步的。 
        当 client.containers.run() 返回时，容器可能只是状态变成了 "Created"，但网络还没完全分配好，或者端口还没绑定成功。
        如果我们立刻去查，可能查不到数据。 
        所以需要 for _ in range(60) 配合 time.sleep(0.5)，给自己最多 30 秒的时间，等着 Docker 把网接好。
        '''
        host_port = None
        for _ in range(60):  # 循环检查，最多等待约 30s（60*0.5）
            try:
                container.reload() # 刷新容器属性:强制去 Docker 守护进程那里重新拉取一次最新的容器信息（刷新缓存）。
                # 从 NetworkSettings 中提取映射出的宿主机端口
                mapping = container.attrs.get("NetworkSettings", {}).get("Ports", {}).get(f"{self.container_port}/tcp")
                if mapping and mapping[0].get("HostPort"):
                    host_port = int(mapping[0]["HostPort"])
                    break
            except Exception as e:
                print("container inspect exception:", e)
            time.sleep(0.5)
        #失败情况①：一直没分配好宿主机端口
        if not host_port:
            print(f"Service mapping not available for container {container.id[:12]}; attrs={container.attrs}")
            try:
                print("Container logs (tail 50):")
                print(container.logs(tail=50).decode(errors='ignore'))
            except Exception:
                pass
            try:#删除容器
                container.stop(timeout=1)
                container.remove(force=True)
            except Exception as e:
                print("cleanup error:", e)
            return None

        #失败情况②：健康检查没通过
        if not self._wait_for_container_service(host_port, timeout=30, check_interval=0.1):
            print(f"Service for newly created container {container.id[:12]} on port {host_port} not ready, removing it.")
            try:
                print("Container logs (tail 80):")
                print(container.logs(tail=80).decode(errors='ignore'))
            except Exception:
                pass
            try:#删除容器
                container.stop(timeout=1)
                container.remove(force=True)
            except Exception as e:
                print("Error cleaning up failed new container:", e)
            return None
        #成功情况
        with self.lock:# 加锁写入 self.containers
            self.containers[container.id] = {
                "container_obj": container,
                "status": "idle",
                "last_active": time.time(),# 刚创建好，默认为空闲
                "host_port": host_port
            }
        print(f"Container '{container_name}' created id={container.id[:12]} host_port={host_port}. Service ready.")
        return container.id

    """
    获取可用容器：当有请求来时，调用此方法
    """
    def get_container_for_request(self):
        with self.lock:
            # 1. 优先寻找现有的空闲容器 (复用)
            for container_id, data in self.containers.items():
                if data["status"] == "idle" and data["container_obj"].status == 'running':
                    data["status"] = "busy"# 标记为忙碌
                    data["last_active"] = time.time()
                    print(f"Assigned existing idle container {container_id[:12]} for {self.function_name}.")
                    return data["host_port"], container_id

        # 2. 如果没有空闲的，只能创建一个新的 (冷启动)
        new_container_id = self._create_new_container()
        if new_container_id:
            with self.lock:
                # 确保新创建的容器也设置为busy并返回其端口
                container_data = self.containers[new_container_id]
                container_data["status"] = "busy" # 新创建的容器直接用于请求、直接投入使用，所以是busy
                container_data["last_active"] = time.time()
                print(f"Assigned newly created container {new_container_id[:12]} for {self.function_name}.")
                return container_data["host_port"], new_container_id
        return None, None

    """
    释放容器：请求处理完后，调用此方法归还容器（置为idle放回队列）（逻辑删除）
    """
    def release_container(self, container_id):
        with self.lock:
            if container_id in self.containers:
                self.containers[container_id]["status"] = "idle"
                self.containers[container_id]["last_active"] = time.time()
                print(f"Container {container_id[:12]} for {self.function_name} released and set to idle.")

    """
    删除容器（物理删除）
    """
    def _remove_container(self, container_id, container_obj):
        try:
            print(f"Stopping and removing container {container_id[:12]} (name: {container_obj.name}) for {self.function_name}...")
            # 尝试停止容器，给定一个短的超时
            container_obj.stop(timeout=5)
            # 强制删除容器，即使它仍在运行或停止失败
            container_obj.remove(force=True)
            with self.lock:
                if container_id in self.containers:
                    del self.containers[container_id]
            print(f"Container {container_id[:12]} removed.")
        except docker.errors.NotFound:
            print(f"Container {container_id[:12]} not found, likely already removed.")
            with self.lock:
                if container_id in self.containers:
                    del self.containers[container_id]
        except Exception as e:
            print(f"Error removing container {container_id[:12]}: {e}. Forcing internal cleanup.")
            # 即使移除失败，也要尝试从 internal 列表中删除，避免重复尝试
            with self.lock:
                if container_id in self.containers:
                    del self.containers[container_id]

    """
    后台清理与预热：这是一个死循环线程，负责维护容器池的健康。
    """
    def _run_cleaner(self):
        """
        后台守护线程：【分频】执行预热与清理。
        策略：
        1. 预热 (Keeper 逻辑): 每 5 秒执行一次 (高频，保证库存)。
        2. 清理 (Cleaner 逻辑): 每 30 秒执行一次 (低频，节省性能)。
        """
        # 基础心跳间隔 (Keeper 的频率)
        TICK_INTERVAL = 5 
        # 清理的时间阈值
        CLEANUP_INTERVAL = 30
        
        last_cleanup_time = 0 # 上次清理的时间戳

        while not self._cleaner_stop_event.is_set():
            current_time = time.time()
            
            # ===============================================================
            # 任务一：预热 (高频：每 5s 必做)
            # ===============================================================
            to_create = 0
            with self.lock:
                current_idle_count = sum(
                    1 for data in self.containers.values()
                    if data["status"] == "idle" and data["container_obj"].status == 'running'
                )
                if current_idle_count < self.min_idle_containers:
                    to_create = self.min_idle_containers - current_idle_count
                    print(f"[Keeper] Pool low ({current_idle_count}/{self.min_idle_containers}). Creating {to_create} containers.")

            for _ in range(to_create):
                if self._cleaner_stop_event.is_set(): break
                try:
                    self._create_new_container()
                    time.sleep(0.1)
                except Exception as e:
                    print(f"[Keeper] Error creating container: {e}")

            # ===============================================================
            # 任务二：清理 (低频：每 30s 做一次)
            # ===============================================================
            # 检查：距离上次清理是否已经过了 30 秒？
            if current_time - last_cleanup_time > CLEANUP_INTERVAL:
                # print("[Cleaner] 30s interval reached. Checking for idle containers...")
                containers_to_remove = []
                
                with self.lock:
                    # 1. 筛选 idle 容器
                    idle_candidates = []
                    for cid, data in list(self.containers.items()):
                        if data["status"] == "idle" and data["container_obj"].status == 'running':
                            idle_candidates.append((cid, data))
                    
                    # 2. 按最后活跃时间排序 (最老的在前)
                    idle_candidates.sort(key=lambda item: item[1]["last_active"])

                    # 3. 标记需要删除的
                    for i, (container_id, data) in enumerate(idle_candidates):
                        num_idle_after = len(idle_candidates) - i
                        is_redundant = num_idle_after > self.min_idle_containers
                        is_timeout = (current_time - data["last_active"]) > self.idle_timeout

                        if is_redundant and is_timeout:
                            containers_to_remove.append((container_id, data["container_obj"]))
                        else:
                            break # 后面的肯定更不满足，跳出
                
                # 执行删除
                if containers_to_remove:
                    print(f"[Cleaner] Found {len(containers_to_remove)} expired containers. Removing...")
                
                for container_id, container_obj in containers_to_remove:
                    if self._cleaner_stop_event.is_set(): break
                    try:
                        self._remove_container(container_id, container_obj)
                    except Exception as e:
                        print(f"[Cleaner] Error removing {container_id[:12]}: {e}")
                
                # 更新清理时间戳
                last_cleanup_time = time.time()

            # ===============================================================
            # 休眠 (等待下一个 5s 心跳)
            # ===============================================================
            self._cleaner_stop_event.wait(timeout=TICK_INTERVAL)

    """
    停止所有容器：用于程序退出时的清理。
    """
    def stop_all_containers(self):
        self._cleaner_stop_event.set()# 通知后台线程停止
        
        print(f"Stopping all containers for {self.function_name}...")
        containers_to_stop = []
        with self.lock:
            # 复制一份，因为在迭代时可能会修改 self.containers
            containers_to_stop = list(self.containers.items()) 
            self.containers.clear() # 清空内部记录，避免再次操作

        for container_id, data in containers_to_stop:
            self._remove_container(container_id, data["container_obj"])
        print(f"All containers for {self.function_name} stopped and removed.")