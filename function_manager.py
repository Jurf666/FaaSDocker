import random
import docker
import time
import threading
import os
import requests

class FunctionManager:
    def __init__(self, function_name, image_name, container_port,  host_port_start=8000, idle_timeout=300, min_idle_containers=0,cpuset_cpus=None):
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
                "cpu_period": 100000,    # CPU 调度周期，单位微秒 (通常设为 100ms)
                "cpu_quota": 20000,      # 周期内允许使用的时长 (20ms / 100ms = 0.2 CPU)
            }
            '''
            # =========== 允许跨物理核与逻辑核心 ===========
            if self.cpuset_cpus:
                run_kwargs['cpuset_cpus'] = self.cpuset_cpus
            # =========== 结束 ===========
            '''
            # =========== 不允许跨物理核，允许跨逻辑核 ===========
            final_cpuset = self.cpuset_cpus # 默认为传入的原始配置

            if self.cpuset_cpus:
                try:
                    # 将字符串配置转换为列表，例如 "0,1,2..." -> [0, 1, 2...]
                    cpu_list = [int(x) for x in self.cpuset_cpus.split(',') if x.strip()]
                    
                    # 判定：如果可用核数 > 2，视为 Baseline (大池子模式)
                    # 实验组通常只有 2 个核 (如 "0,64")，不会进入此分支
                    if len(cpu_list) > 2:
                        # 筛选出物理核 (0-63)，排除逻辑核以便后续配对
                        # 注意：这里硬编码了你的机器架构 (0-63 为物理核)
                        physical_cores = [c for c in cpu_list if c < 64]
                        
                        if physical_cores:
                            # 1. 随机选中一个物理核
                            chosen_phy = random.choice(physical_cores)
                            
                            # 2. 计算其超线程兄弟 (偏移量 64)
                            sibling = chosen_phy + 64
                            # 3. 构造新的绑定字符串 "物理核,兄弟核"
                            # 确保兄弟核也在允许列表中
                            if sibling in cpu_list:
                                final_cpuset = f"{chosen_phy},{sibling}"
                            else:
                                final_cpuset = f"{chosen_phy}"
                            
                            #final_cpuset = f"{chosen_phy}"
                            print(f"[Affinity] Baseline detected. Pinning {container_name} to {final_cpuset}")
                except Exception as e:
                    print(f"[Affinity] Error generating random cpuset: {e}")

            # 将最终决定的 cpuset 写入参数
            if final_cpuset:
                run_kwargs['cpuset_cpus'] = final_cpuset
            # =========== 结束 ===========

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

    # ===== [Warmup增强开始] =====
    def ensure_min_total_containers(self, target_total):
        """
        主动补齐当前函数的总容器数到 target_total。
        详细说明：
        1) 该方法只创建缺口容器，不做删除；
        2) 供 wait_for_warmup 在启动前即时触发，减少等待 keeper 周期；
        3) 返回创建统计，方便上层日志与排查。
        """
        # 入参容错，避免非法值导致 warmup 中断。
        try:
            target_total = int(target_total)
        except (TypeError, ValueError):
            try:
                target_total = int(self.min_idle_containers)
            except (TypeError, ValueError):
                target_total = 0

        if target_total < 0:
            target_total = 0

        # 统计在锁内完成，创建在锁外完成，避免长时间持锁影响正常请求分配。
        with self.lock:
            current_total = sum(
                1 for d in self.containers.values()
                if d["container_obj"].status == 'running'
            )

        missing = max(0, target_total - current_total)
        created = 0
        for _ in range(missing):
            # 如果管理器进入停止流程，则立即中止补齐。
            if self._cleaner_stop_event.is_set():
                break
            cid = self._create_new_container()
            if cid:
                created += 1

        with self.lock:
            final_total = sum(
                1 for d in self.containers.values()
                if d["container_obj"].status == 'running'
            )

        return {
            "target_total": target_total,
            "current_total": current_total,
            "created": created,
            "final_total": final_total
        }
    # ===== [Warmup增强结束] =====

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

    def _run_keeper_once(self):
        """
        Keeper 单次周期：仅负责补足空闲池，不做任何回收动作。
        """
        # 详细注释：先算缺口，再在锁外创建，避免长时间持锁阻塞 dispatch 路径。
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
            if self._cleaner_stop_event.is_set():
                break
            try:
                self._create_new_container()
                time.sleep(0.1)
            except Exception as e:
                print(f"[Keeper] Error creating container: {e}")

    def _run_cleaner_once(self, current_time, last_cleanup_time, cleanup_interval):
        """
        Cleaner 单次周期：按超时与冗余规则回收容器。
        :return: 更新后的 last_cleanup_time
        """
        if current_time - last_cleanup_time <= cleanup_interval:
            return last_cleanup_time

        containers_to_remove = []
        with self.lock:
            # 详细注释：先筛 idle，再按 last_active 从旧到新排序，优先回收最久未使用的容器。
            idle_candidates = []
            for cid, data in list(self.containers.items()):
                if data["status"] == "idle" and data["container_obj"].status == 'running':
                    idle_candidates.append((cid, data))

            idle_candidates.sort(key=lambda item: item[1]["last_active"])

            for i, (container_id, data) in enumerate(idle_candidates):
                num_idle_after = len(idle_candidates) - i
                is_redundant = num_idle_after > self.min_idle_containers
                is_timeout = (current_time - data["last_active"]) > self.idle_timeout
                if is_redundant and is_timeout:
                    containers_to_remove.append((container_id, data["container_obj"]))
                else:
                    break

        if containers_to_remove:
            print(f"[Cleaner] Found {len(containers_to_remove)} expired containers. Removing...")

        for container_id, container_obj in containers_to_remove:
            if self._cleaner_stop_event.is_set():
                break
            try:
                self._remove_container(container_id, container_obj)
            except Exception as e:
                print(f"[Cleaner] Error removing {container_id[:12]}: {e}")

        return time.time()
    
    """
    后台清理与预热：这是一个死循环线程，负责维护容器池的健康。
    """
    def _run_cleaner(self):
        # 详细注释：调度器只负责“分频触发”，具体逻辑拆分到 keeper/cleaner 单次函数中。
        TICK_INTERVAL = 5
        CLEANUP_INTERVAL = 30
        last_cleanup_time = 0

        while not self._cleaner_stop_event.is_set():
            current_time = time.time()
            self._run_keeper_once()
            last_cleanup_time = self._run_cleaner_once(current_time, last_cleanup_time, CLEANUP_INTERVAL)
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
   