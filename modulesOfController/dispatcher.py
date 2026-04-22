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
        Convert cpuset (logical CPUs) to physical core ids.
        Examples:
          "2,66" -> [2]
          "0-3"  -> [0, 1, 2, 3]
        """
        if cpuset_value is None:
            return []

        if isinstance(cpuset_value, (list, tuple, set)):
            raw_items = list(cpuset_value)
        else:
            raw_items = str(cpuset_value).split(",")

        logical_cpus = set()
        for item in raw_items:
            token = str(item).strip()
            if not token:
                continue
            try:
                logical_cpus.add(int(token))
            except ValueError:
                continue

        # SMT sibling offset is 64 in this environment
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
          - do not use outer /run wrapper timing
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
            '''
            #用于1period-2client模式
            selected_cpuset, affinity_lease_id = manager.apply_request_affinity(
                container_id,
                request_id=request_id,
                wait_timeout=1200,
            )

            '''
            #用于其它模式
            selected_cpuset, affinity_lease_id = manager.apply_request_affinity_free(
                container_id,
                request_id=request_id,
                max_containers_per_core=2,
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
            func_cpu_time = proxy_result.get("func_cpu_time")
            func_cpu_time_ns = proxy_result.get("func_cpu_time_ns")
            process_cpu_time = proxy_result.get("process_cpu_time", func_cpu_time)
            process_cpu_time_ns = proxy_result.get("process_cpu_time_ns", func_cpu_time_ns)
            container_cpu_time = proxy_result.get("container_cpu_time")
            container_cpu_time_ns = proxy_result.get("container_cpu_time_ns")
            cgroup_cpu_stat_path = proxy_result.get("cgroup_cpu_stat_path")
            cgroup_cpu_usage_source = proxy_result.get("cgroup_cpu_usage_source")
            cgroup_nr_periods_delta = proxy_result.get("cgroup_nr_periods_delta")
            cgroup_nr_throttled_delta = proxy_result.get("cgroup_nr_throttled_delta")
            cgroup_throttled_time_ns_delta = proxy_result.get("cgroup_throttled_time_ns_delta")
            cgroup_throttled_time_seconds_delta = proxy_result.get("cgroup_throttled_time_seconds_delta")
            cgroup_throttle_ratio_delta = proxy_result.get("cgroup_throttle_ratio_delta")

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

            if proxy_result.get("error"):
                final_result = {
                    "error": proxy_result.get("error"),
                    "traceback": proxy_result.get("traceback"),
                }

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
                    "func_cpu_time": func_cpu_time,
                    "func_cpu_time_ns": func_cpu_time_ns,
                    "process_cpu_time": process_cpu_time,
                    "process_cpu_time_ns": process_cpu_time_ns,
                    "container_cpu_time": container_cpu_time,
                    "container_cpu_time_ns": container_cpu_time_ns,
                    "cgroup_cpu_stat_path": cgroup_cpu_stat_path,
                    "cgroup_cpu_usage_source": cgroup_cpu_usage_source,
                    "cgroup_nr_periods_delta": cgroup_nr_periods_delta,
                    "cgroup_nr_throttled_delta": cgroup_nr_throttled_delta,
                    "cgroup_throttled_time_ns_delta": cgroup_throttled_time_ns_delta,
                    "cgroup_throttled_time_seconds_delta": cgroup_throttled_time_seconds_delta,
                    "cgroup_throttle_ratio_delta": cgroup_throttle_ratio_delta,
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
