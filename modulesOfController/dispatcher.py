import time
import uuid
import requests
import threading
import logging
from function_manager import FunctionManager

logger = logging.getLogger("Dispatcher")

class Dispatcher:
    def __init__(self):
        self.function_managers = {}
        self.manager_lock = threading.Lock()

    def get_or_create_manager(self, function_name, cpuset_cpus=None,min_idle_containers = 1):
        """
        线程安全地获取 FunctionManager，如果不存在则使用默认配置创建。
        """
         # 1. 快速检查（无锁），稍微提高性能
        if function_name in self.function_managers:
            return self.function_managers[function_name]

        # 2. 加锁创建
        with self.manager_lock:
            # 双重检查：防止在等待锁的过程中已经被别的线程创建了
            if function_name not in self.function_managers:
                # 统一在这里配置镜像名、端口等参数
                self.function_managers[function_name] = FunctionManager(
                    function_name=function_name,
                    image_name='yyxie-test2',
                    container_port=5000, 
                    min_idle_containers=min_idle_containers,
                    cpuset_cpus=cpuset_cpus
                )
            return self.function_managers[function_name]
    
    def clean_up(self):
        logger.info("Stopping all containers...")
        with self.manager_lock:
            for m in self.function_managers.values():
                m.stop_all_containers()

    def dispatch_sync(self, function_name, payload, is_workflow=False):
        """
        核心分发逻辑
        :param is_workflow: 决定 Payload 的构造方式 (input_mapping vs flat)
        :return: (final_result, container_id, func_duration)
        """
        request_id = f"req-{uuid.uuid4().hex[:8]}"
        manager = self.get_or_create_manager(function_name)
        container_id = None

        try:
            # 1. 获取容器
            host_port, container_id = manager.get_container_for_request()
            if not host_port:
                raise Exception(f"No container available for {function_name}")

            # 2. 构造 Payload (还原原始 controller.py 的逻辑)
            # 关键差异：工作流模式下，参数被包裹在 input_mapping 中
            if is_workflow:
                proxy_payload = {
                    "request_id": request_id,
                    "action": function_name,
                    "input_mapping": payload 
                }
            else:
                proxy_payload = payload.copy() if isinstance(payload, dict) else {}
                proxy_payload['action'] = function_name

            # 3. Init (Optional)
            try:
                requests.post(f"http://127.0.0.1:{host_port}/init", json={"action": function_name}, timeout=5)
            except requests.RequestException:
                pass 

            # 4. RUN
            start = time.time()
            resp = requests.post(f"http://127.0.0.1:{host_port}/run", json=proxy_payload, timeout=2000)
            
            if resp.status_code != 200:
                logger.error(f"Container Error ({resp.status_code}): {resp.text}")
                raise Exception(f"Container Error: {resp.text}") 
            resp.raise_for_status()

            # 5. 解析结果
            full_data = resp.json() 
            proxy_result = full_data.get("result", {})
            # proxy 返回结构: { start_time, end_time, duration(HTTP 全过程), result: { func_result, func_duration, ... } }
            duration_from_proxy = proxy_result.get("func_duration", 0)
            
            duration = time.time() - start
            mode_str = "WORKFLOW" if is_workflow else "SIMPLE"
            logger.info(f"[Run][{mode_str}] {function_name} finished in {duration:.2f}s (func: {duration_from_proxy:.4f}s)")
            
            # 6. 提取返回值 (还原逻辑)
            if is_workflow:
                final_result = proxy_result.get("output_keys", {}) 
            else:
                final_result = proxy_result.get("func_result")
                
            # return final_result, container_id, duration_from_proxy

            if isinstance(final_result, dict):
                out = dict(final_result)  # shallow copy
            else:
                out = {"_value": final_result}

            out.setdefault('__meta__', {})
            out['__meta__'].update({
                'container_id': container_id,
                'duration': duration_from_proxy
            })
            return out

        except Exception as e:
            logger.error(f"[Dispatch] Failed for {function_name}: {e}")
            raise e
        finally:
            if container_id:
                manager.release_container(container_id)