import random
import docker
import time
import threading
import os
import requests


class FunctionManager:
    def __init__(
        self,
        function_name,
        image_name,
        container_port,
        host_port_start=8000,
        idle_timeout=300,
        min_idle_containers=0,
        cpuset_cpus=None,
    ):
        self.function_name = function_name
        self.image_name = image_name
        self.container_port = container_port
        self.host_port_start = host_port_start
        self.idle_timeout = idle_timeout
        self.min_idle_containers = min_idle_containers
        self.cpuset_cpus = cpuset_cpus
        self._cpuset_cpu_list = self._parse_cpuset_list(cpuset_cpus)
        self._physical_candidates = sorted(c for c in self._cpuset_cpu_list if c < 64)
        self._is_baseline_pool = len(self._cpuset_cpu_list) > 2 and bool(self._physical_candidates)
        self.docker_client = docker.from_env()

        # 核心容器池结构：
        # {container_id: {"container_obj": ..., "status": "idle/busy", "last_active": ts, "host_port": ...}}
        self.containers = {}
        self.lock = threading.Lock()  # 保护 self.containers 的并发读写

        # 后台 keeper/cleaner 线程控制
        self._cleaner_stop_event = threading.Event()
        self.cleaner_thread = threading.Thread(target=self._run_cleaner, daemon=True)
        self.cleaner_thread.start()

        print(f"FunctionManager for {self.function_name} initialized.")

    @staticmethod
    def _parse_cpuset_list(cpuset_cpus):
        # 将字符串配置转换为有序列表，例如 "0,1,2..." -> [0, 1, 2...]
        if not cpuset_cpus:
            return []
        cpu_list = [int(x) for x in cpuset_cpus.split(',') if x.strip()]
        return sorted(cpu_list)

    def _choose_request_cpuset(self):
        """
        Per-request affinity:
        - baseline big-pool mode (>2 CPUs): random physical core (+ sibling if present)
        - experiment/small pool mode: keep original cpuset
        """

        if not self._is_baseline_pool:
            return None

        chosen_phy = random.choice(self._physical_candidates)
        sibling = chosen_phy + 64
        if sibling in self._cpuset_cpu_list:
            return f"{chosen_phy},{sibling}"
        return str(chosen_phy)


    def apply_request_affinity(self, container_id):
        """Apply affinity before each request and return selected cpuset."""
        chosen_cpuset = self._choose_request_cpuset()
        if not chosen_cpuset:
            return None

        with self.lock:
            data = self.containers.get(container_id)
            if not data:
                return None
            container_obj = data["container_obj"]

        try:
            container_obj.update(cpuset_cpus=chosen_cpuset)
            with self.lock:
                if container_id in self.containers:
                    self.containers[container_id]["last_cpuset"] = chosen_cpuset
            print(
                f"[Affinity] Request-level pinning for {self.function_name} "
                f"{container_id[:12]} -> {chosen_cpuset}"
            )
            return chosen_cpuset
        except Exception as e:
            print(f"[Affinity] Failed to update cpuset for {container_id[:12]}: {e}")
            return None

    def _wait_for_container_service(self, host_port, timeout=30, check_interval=0.01):
        """轮询容器 /status 接口，直到服务就绪或超时。"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                # 修改说明：轮询容器 /status，确认服务是否就绪。
                response = requests.get(f"http://127.0.0.1:{host_port}/status", timeout=check_interval)
                if response.status_code == 200:
                    try:
                        data = response.json()
                    except Exception:
                        data = {}
                    if data.get("status") in ["new", "ok", "ready"]:
                        print(f"Container service on port {host_port} is ready.")
                        return True
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                # 修改说明：连接未就绪时继续重试，直到超时。
                pass
            time.sleep(check_interval)

        print(f"Container service on port {host_port} did not become ready within {timeout} seconds.")
        return False

    def _create_new_container(self):
        """创建新容器，等待端口与服务就绪，并注册到容器池。"""
        container_name = f"{self.function_name}-{os.urandom(4).hex()}"
        try:
            print(f"Creating new container '{container_name}' ...")
            run_kwargs = {
                "detach": True,
                "ports": {f"{self.container_port}/tcp": None},
                "name": container_name,
                "cpu_period": 100000,
                "cpu_quota": 20000,
            }
            
            if self.cpuset_cpus:
                run_kwargs['cpuset_cpus'] = self.cpuset_cpus

            container = self.docker_client.containers.run(
                self.image_name,
                **run_kwargs,
            )
            print(f"Created container id={container.id:12}")

        except docker.errors.ImageNotFound:
            print(f"Error: Image '{self.image_name}' not found.")
            return None
        except Exception as e:
            print(f"Error creating container '{container_name}': {e}")
            return None

        # Docker 端口映射可能是异步生效：轮询等待最多 30s
        host_port = None
        for _ in range(60):
            try:
                container.reload()
                mapping = (
                    container.attrs
                    .get("NetworkSettings", {})
                    .get("Ports", {})
                    .get(f"{self.container_port}/tcp")
                )
                if mapping and mapping[0].get("HostPort"):
                    host_port = int(mapping[0]["HostPort"])
                    break
            except Exception as e:
                print("container inspect exception:", e)
            time.sleep(0.5)

        # 失败场景1：端口映射迟迟不可用
        if not host_port:
            print(f"Service mapping not available for container {container.id[:12]}; attrs={container.attrs}")
            try:
                print("Container logs (tail 50):")
                print(container.logs(tail=50).decode(errors='ignore'))
            except Exception:
                pass
            try:
                container.stop(timeout=1)
                container.remove(force=True)
            except Exception as e:
                print("cleanup error:", e)
            return None

        # 失败场景2：服务健康检查未通过
        if not self._wait_for_container_service(host_port, timeout=30, check_interval=0.1):
            print(
                f"Service for newly created container {container.id[:12]} "
                f"on port {host_port} not ready, removing it."
            )
            try:
                print("Container logs (tail 80):")
                print(container.logs(tail=80).decode(errors='ignore'))
            except Exception:
                pass
            try:
                container.stop(timeout=1)
                container.remove(force=True)
            except Exception as e:
                print("Error cleaning up failed new container:", e)
            return None

        # 成功：登记到容器池，初始状态设为 idle
        with self.lock:
            self.containers[container.id] = {
                "container_obj": container,
                "status": "idle",
                "last_active": time.time(),
                "host_port": host_port,
            }

        print(
            f"Container '{container_name}' created id={container.id[:12]} "
            f"host_port={host_port}. Service ready."
        )
        return container.id

    def get_container_for_request(self):
        """获取可用容器：优先复用 idle，否则冷启动创建。"""
        with self.lock:
            # 1) 优先复用已存在的 idle 且 running 的容器
            for container_id, data in self.containers.items():
                if data["status"] == "idle" and data["container_obj"].status == 'running':
                    data["status"] = "busy"
                    data["last_active"] = time.time()
                    print(f"Assigned existing idle container {container_id[:12]} for {self.function_name}.")
                    return data["host_port"], container_id

        # 2) 没有可复用容器时，尝试新建
        new_container_id = self._create_new_container()
        if new_container_id:
            with self.lock:
                container_data = self.containers[new_container_id]
                container_data["status"] = "busy"
                container_data["last_active"] = time.time()
                print(f"Assigned newly created container {new_container_id[:12]} for {self.function_name}.")
                return container_data["host_port"], new_container_id

        return None, None

    def release_container(self, container_id):
        """释放容器：请求完成后将其标记回 idle。"""
        with self.lock:
            if container_id in self.containers:
                self.containers[container_id]["status"] = "idle"
                self.containers[container_id]["last_active"] = time.time()
                print(f"Container {container_id[:12]} for {self.function_name} released and set to idle.")

    # ===== [Warmup增强-开始] =====
    def ensure_min_total_containers(self, target_total):
        """
        主动补齐当前函数的总容器数到 target_total。

        说明：
        1) 仅补齐缺口，不做删除；
        2) 供 wait_for_warmup 触发，减少等待 keeper 周期；
        3) 返回创建统计，方便上层日志与排障。
        """
        # 入参容错：避免非法值导致 warmup 中断
        try:
            target_total = int(target_total)
        except (TypeError, ValueError):
            try:
                target_total = int(self.min_idle_containers)
            except (TypeError, ValueError):
                target_total = 0

        if target_total < 0:
            target_total = 0

        # 统计放锁内，创建放锁外，避免长时间持锁影响正常分配
        with self.lock:
            current_total = sum(
                1 for d in self.containers.values()
                if d["container_obj"].status == 'running'
            )

        missing = max(0, target_total - current_total)
        created = 0
        for _ in range(missing):
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
            "final_total": final_total,
        }
    # ===== [Warmup增强-结束] =====

    def _remove_container(self, container_id, container_obj):
        """物理删除容器，并同步清理内部记录。"""
        try:
            print(
                f"Stopping and removing container {container_id[:12]} "
                f"(name: {container_obj.name}) for {self.function_name}..."
            )
            container_obj.stop(timeout=5)
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
            # 即使 Docker 删除失败，也要尽量清理内部索引，避免反复尝试
            with self.lock:
                if container_id in self.containers:
                    del self.containers[container_id]

    def _run_keeper_once(self):
        """Keeper 单次循环：仅负责补齐 idle 池，不执行回收。"""
        # 修改说明：先算缺口，再在锁外创建，避免长时间持锁阻塞 dispatch。
        to_create = 0
        with self.lock:
            current_idle_count = sum(
                1 for data in self.containers.values()
                if data["status"] == "idle" and data["container_obj"].status == 'running'
            )
            if current_idle_count < self.min_idle_containers:
                to_create = self.min_idle_containers - current_idle_count
                print(
                    f"[Keeper] Pool low ({current_idle_count}/{self.min_idle_containers}). "
                    f"Creating {to_create} containers."
                )

        for _ in range(to_create):
            if self._cleaner_stop_event.is_set():
                break
            try:
                self._create_new_container()
                time.sleep(0.1)
            except Exception as e:
                print(f"[Keeper] Error creating container: {e}")

    def _run_cleaner_once(self, current_time, last_cleanup_time, cleanup_interval):
        """Cleaner 单次循环：按超时与冗余规则回收 idle 容器。"""
        if current_time - last_cleanup_time <= cleanup_interval:
            return last_cleanup_time

        containers_to_remove = []
        with self.lock:
            # 修改说明：先筛 idle，再按 last_active 从旧到新排序，优先回收最久未使用容器。
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

    def _run_cleaner(self):
        """后台调度线程：周期执行 keeper 与 cleaner。"""
        # 修改说明：调度器只负责分频触发，具体逻辑拆分到 keeper/cleaner 单次函数。
        TICK_INTERVAL = 5
        CLEANUP_INTERVAL = 30
        last_cleanup_time = 0

        while not self._cleaner_stop_event.is_set():
            current_time = time.time()
            self._run_keeper_once()
            last_cleanup_time = self._run_cleaner_once(
                current_time,
                last_cleanup_time,
                CLEANUP_INTERVAL,
            )
            self._cleaner_stop_event.wait(timeout=TICK_INTERVAL)

    def stop_all_containers(self):
        """停止并移除当前函数的全部容器。"""
        self._cleaner_stop_event.set()

        print(f"Stopping all containers for {self.function_name}...")
        containers_to_stop = []
        with self.lock:
            containers_to_stop = list(self.containers.items())
            self.containers.clear()

        for container_id, data in containers_to_stop:
            self._remove_container(container_id, data["container_obj"])

        print(f"All containers for {self.function_name} stopped and removed.")
