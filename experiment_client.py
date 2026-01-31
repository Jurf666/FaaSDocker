"""
ExperimentClient - 负责实验客户端的管理和执行
"""
import time
import threading
import requests
import numpy as np
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed


class ExperimentClient:
    """实验客户端管理器"""
    
    def __init__(self, controller_url='http://localhost:5000'):
        """
        初始化 ExperimentClient
        
        Args:
            controller_url: Controller URL
        """
        self.controller_url = controller_url
        self.perf_data = defaultdict(list)
        self.data_lock = threading.Lock()
    
    def dispatch_simple(self, func_name, payload, request_id):
        """
        发送简单函数请求
        
        Args:
            func_name: 函数名
            payload: 请求参数
            request_id: 请求 ID
            
        Returns:
            dict: 响应结果
        """
        start_time = time.time()
        try:
            resp = requests.post(
                f"{self.controller_url}/dispatch/{func_name}",
                json=payload,
                timeout=1200
            )
            
            if resp.status_code != 200:
                print(f"[ERROR] Request {request_id} ({func_name}) failed: {resp.status_code}")
                print(f"[ERROR] Response body: {resp.text}")
                return None
            
            data = resp.json()
            out = data.get('output') if isinstance(data, dict) else None
            print(f"[DEBUG] {request_id} ({func_name}): response keys={list(data.keys())}, "
                  f"output type={type(out)}")
            
            container_id = None
            duration = None
            
            if isinstance(out, dict):
                meta = out.get('__meta__', {})
                container_id = meta.get('container_id')
                duration = meta.get('duration') or meta.get('func_duration')
                print(f"[DEBUG] {request_id} ({func_name}): meta={meta}, duration={duration}")
            else:
                print(f"[DEBUG] {request_id} ({func_name}): output is not dict, out={out}")
            
            # 检查 duration 是否有效，且 output 不包含错误信息
            is_valid_duration = (duration is not None) and (duration > 0)
            has_error = isinstance(out, dict) and 'error' in out
            
            if has_error:
                print(f"[WARN] Request {request_id} failed with error: {out['error']}")
            
            if is_valid_duration and not has_error:
                with self.data_lock:
                    self.perf_data[func_name].append(duration)
            else:
                if duration == 0 or duration is None:
                    print(f"[FILTER] Ignored invalid duration {duration} for {func_name}")
            
            end_time = time.time()
            latency = end_time - start_time
            
            return {
                'request_id': request_id,
                'function': func_name,
                'duration': duration,
                'latency': latency,
                'container_id': container_id,
                'output': out
            }
        
        except Exception as e:
            print(f"[ERROR] Request {request_id} ({func_name}) exception: {e}")
            return None
    
    def client_worker(self, client_id, func_name, payload_template, end_time):
        """
        固定功能的闭环 client: 在截止时间前持续发送请求
        
        Args:
            client_id: 客户端 ID
            func_name: 函数名
            payload_template: 请求参数模板
            end_time: 截止时间
        """
        request_counter = 0
        
        try:
            while time.time() < end_time:
                request_id = f"{func_name}-{client_id}-{request_counter}"
                payload = payload_template.copy() if isinstance(payload_template, dict) else {}
                result = self.dispatch_simple(func_name, payload, request_id)
                
                if result:
                    print(f"[CLIENT {client_id}] Completed request {request_id} ({func_name})")
                else:
                    print(f"[CLIENT {client_id}] Failed request {request_id} ({func_name})")
                
                request_counter += 1
        except Exception as e:
            print(f"[CLIENT {client_id}] Exception: {e}")
    
    def run_experiment(self, client_configs, test_duration):
        """
        运行实验
        
        Args:
            client_configs: 客户端配置列表 [(func_name, payload), ...]
            test_duration: 测试时长(秒)
            
        Returns:
            dict: 性能数据
        """
        num_clients = len(client_configs)
        print(f"\n[INFO] Starting {num_clients} client threads...")
        
        start_experiment = time.time()
        end_experiment_deadline = start_experiment + test_duration
        
        executor = ThreadPoolExecutor(max_workers=num_clients)
        client_futures = []
        
        for idx, (func_name, payload) in enumerate(client_configs):
            future = executor.submit(
                self.client_worker, 
                idx, 
                func_name, 
                payload, 
                end_experiment_deadline
            )
            client_futures.append(future)
        
        for future in as_completed(client_futures):
            future.result()
        
        executor.shutdown(wait=True)
        
        end_experiment = time.time()
        total_time = end_experiment - start_experiment
        total_completed = sum(len(times) for times in self.perf_data.values())
        
        print(f"\n[INFO] All clients finished in {total_time:.2f}s")
        print(f"[INFO] Completed Requests: {total_completed}")
        print(f"[INFO] Throughput: {total_completed / total_time:.2f} req/s")
        
        return {
            'total_time': total_time,
            'total_completed': total_completed,
            'throughput': total_completed / total_time if total_time > 0 else 0
        }
    
    def compute_statistics(self):
        """
        计算统计数据
        
        Returns:
            dict: 统计结果
        """
        print("\n=== Performance Statistics ===")
        stats = {}
        
        for func_name in sorted(self.perf_data.keys()):
            times = self.perf_data[func_name]
            stat = self._compute_stability(times)
            stats[func_name] = stat
            
            print(f"\n[{func_name}]")
            print(f"  Count: {stat['count']}")
            print(f"  Mean: {stat['mean']:.6f}s")
            print(f"  Std: {stat['std']:.6f}s")
            print(f"  CV (Coefficient of Variation): {stat['cv']:.4f}")
            print(f"  Min: {stat['min']:.6f}s")
            print(f"  Max: {stat['max']:.6f}s")
            print(f"  P90: {stat['p90']:.6f}s")
            print(f"  P95: {stat['p95']:.6f}s")
        
        return stats
    
    def _compute_stability(self, times):
        """计算统计指标"""
        if not times:
            return {}
        
        arr = np.array(times)
        mean = float(np.mean(arr))
        std = float(np.std(arr))
        cv = float(std / mean) if mean != 0 else 0.0
        
        return {
            "count": len(times),
            "mean": mean,
            "variance": float(np.var(arr)),
            "std": std,
            "cv": cv,
            "min": float(np.min(arr)),
            "max": float(np.max(arr)),
            "iqr": float(np.percentile(arr, 75) - np.percentile(arr, 25)),
            "p90": float(np.percentile(arr, 90)),
            "p95": float(np.percentile(arr, 95))
        }
    
    def get_perf_data(self):
        """获取性能数据"""
        return self.perf_data
    
    def init_controller_managers(self, cgroup_configs, func_to_group):
        """
        初始化 Controller 上的 Function Managers
        
        Args:
            cgroup_configs: cgroup 配置字典
            func_to_group: 函数到分组的映射
        """
        print("[INFO] Initializing Function Managers on Controller with CPU sets...")
        
        all_funcs = set(func_to_group.keys())
        
        for func_name in all_funcs:
            group_id = func_to_group[func_name]
            group_name = f"group_{group_id}"
            
            cpuset = None
            if group_name in cgroup_configs:
                cpuset = cgroup_configs[group_name]['cpus']
            
            try:
                resp = requests.post(
                    f"{self.controller_url}/create_manager",
                    json={
                        "function_name": func_name,
                        "cpuset_cpus": cpuset
                    },
                    timeout=5
                )
                if resp.status_code in [200, 201]:
                    print(f"   > Init {func_name}: cpuset={cpuset} (OK)")
                else:
                    print(f"   > Init {func_name}: Failed {resp.text}")
            except Exception as e:
                print(f"   > Init {func_name}: Error {e}")
