import logging
import threading
import time
import uuid

import requests

from function_manager import FunctionManager

logger = logging.getLogger("Dispatcher")


class Dispatcher:
    def __init__(self):
        self.function_managers = {}
        self.manager_lock = threading.Lock()

    @staticmethod
    def _parse_physical_cores(cpuset_value):
        """
        根据逻辑核得到物理核
        """
        if cpuset_value is None:
            return []

        if isinstance(cpuset_value, (list, tuple, set)):
            raw_items = list(cpuset_value)
        else:
            raw_items = str(cpuset_value).split(",")

        logical_cpus = set()#set具有不重复性
        for item in raw_items:
            token = str(item).strip()
            if not token:
                continue
            try:
                logical_cpus.add(int(token))
            except ValueError:
                continue
        return sorted({cpu - 64 if cpu >= 64 else cpu for cpu in logical_cpus})

    def get_or_create_manager(self, function_name, cpuset_cpus=None):
        if function_name in self.function_managers:
            return self.function_managers[function_name]

        with self.manager_lock:
            if function_name not in self.function_managers:
                self.function_managers[function_name] = FunctionManager(
                    function_name=function_name,
                    image_name="jywang_test",
                    container_port=5000,
                    cpuset_cpus=cpuset_cpus,
                )
            return self.function_managers[function_name]

    def clean_up(self):
        logger.info("Stopping all containers...")
        with self.manager_lock:
            for manager in self.function_managers.values():
                manager.stop_all_containers()

    def dispatch_sync(self, function_name, payload, is_workflow=False):
        """
        Core dispatch flow:
          1) acquire container and apply request affinity
          2) call proxy /run
          3) fill output __meta__

        Timing semantics:
          - use only runner.run() internal exec+main timing
        """
        request_id = f"req-{uuid.uuid4().hex[:8]}"
        manager = self.get_or_create_manager(function_name)
        container_id = None
        selected_cpuset = None
        affinity_lease_id = None

        try:
            host_port, container_id = manager.get_container_for_request()
            if not host_port:
                raise Exception(f"No container available for {function_name}")
            selected_cpuset, affinity_lease_id = manager.apply_request_affinity(
                container_id,
                request_id=request_id,
                wait_timeout=1200,
            )
            if not selected_cpuset:
                raise Exception(f"Affinity allocation failed for {function_name}")

            if is_workflow:
                proxy_payload = {
                    "request_id": request_id,
                    "action": function_name,
                    "input_mapping": payload,
                }
            else:
                proxy_payload = payload.copy() if isinstance(payload, dict) else {}
                proxy_payload["action"] = function_name

            try:
                requests.post(
                    f"http://127.0.0.1:{host_port}/init",
                    json={"action": function_name},
                    timeout=5,
                )
            except requests.RequestException:
                pass

            start = time.time()
            resp = requests.post(
                f"http://127.0.0.1:{host_port}/run",
                json=proxy_payload,
                timeout=2000,
            )
            if resp.status_code != 200:
                logger.error(f"Container Error ({resp.status_code}): {resp.text}")
                raise Exception(f"Container Error: {resp.text}")
            resp.raise_for_status()

            # Only parse function execution fields from result (exec+main)
            full_data = resp.json()
            proxy_result = full_data.get("result", {})
            duration_from_proxy = proxy_result.get("func_duration", 0)
            func_main_start_ns = proxy_result.get("func_main_start_ns")
            func_main_end_ns = proxy_result.get("func_main_end_ns")
            func_duration_ns = proxy_result.get("func_duration_ns")

            physical_cores = self._parse_physical_cores(selected_cpuset)
            if not physical_cores:
                physical_cores = self._parse_physical_cores(getattr(manager, "cpuset_cpus", None))

            elapsed = time.time() - start
            mode_str = "WORKFLOW" if is_workflow else "SIMPLE"
            logger.info(
                f"[Run][{mode_str}] {function_name} finished in {elapsed:.2f}s "
                f"(func: {duration_from_proxy:.4f}s)"
            )

            if is_workflow:
                final_result = proxy_result.get("output_keys", {})
            else:
                final_result = proxy_result.get("func_result")

            if isinstance(final_result, dict):
                out = dict(final_result)
            else:
                out = {"_value": final_result}

            out.setdefault("__meta__", {})
            out["__meta__"].update(
                {
                    "request_id": request_id,
                    "function_name": function_name,
                    "container_id": container_id,
                    "duration": duration_from_proxy,  # seconds (exec+main)
                    "cpuset": selected_cpuset,
                    "physical_cores": physical_cores,
                    # field names kept for compatibility; semantics are exec+main
                    "func_main_start_ns": func_main_start_ns,
                    "func_main_end_ns": func_main_end_ns,
                    "func_duration_ns": func_duration_ns,
                }
            )
            return out

        except Exception as e:
            logger.error(f"[Dispatch] Failed for {function_name}: {e}")
            raise e
        finally:
            if affinity_lease_id:
                manager.release_request_affinity_lease(affinity_lease_id)
            if container_id:
                manager.release_container(container_id)
