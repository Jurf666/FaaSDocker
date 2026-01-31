"""
WorkflowWarmer - 负责工作流预热，生成可复用的中间结果
"""
import uuid
import json
import redis


class WorkflowWarmer:
    """工作流预热管理器"""
    
    def __init__(self, redis_host='172.17.0.1', redis_port=6379, controller_url='http://localhost:5000'):
        """
        初始化 WorkflowWarmer
        
        Args:
            redis_host: Redis 服务器地址
            redis_port: Redis 端口
            controller_url: Controller URL
        """
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.controller_url = controller_url
        self.redis_client = None
        self.cached_payloads = {}
        
    def init_redis(self):
        """初始化 Redis 连接"""
        if self.redis_client:
            return self.redis_client
        try:
            self.redis_client = redis.StrictRedis(
                host=self.redis_host,
                port=self.redis_port,
                db=0,
                decode_responses=True
            )
            self.redis_client.ping()
            print(f"[INFO] Connected to Redis at {self.redis_host}:{self.redis_port}")
        except Exception as e:
            self.redis_client = None
            print(f"[WARN] Redis not available, workflow cache warmup skipped: {e}")
        return self.redis_client
    
    def warmup_all_workflows(self, dispatch_func):
        """
        预热所有工作流
        
        Args:
            dispatch_func: 用于发送请求的函数
            
        Returns:
            dict: 缓存的 payload 字典
        """
        if not self.init_redis():
            print(f"[WARN] Redis client init failed, skipping workflow cache warmup")
            return {}
        
        self.cached_payloads = {}
        self._warmup_video_workflow(dispatch_func)
        self._warmup_recognizer_workflow(dispatch_func)
        self._warmup_svd_workflow(dispatch_func)
        self._warmup_wordcount_workflow(dispatch_func)
        
        print(f"[INFO] Prepared workflow caches for {len(self.cached_payloads)} "
              f"subfunctions: {list(self.cached_payloads.keys())}")
        return self.cached_payloads
    
    def _warmup_video_workflow(self, dispatch_func):
        """预热 Video 工作流"""
        print("[INFO] === Warming up Video workflow ===")
        try:
            video_up = dispatch_func('video_upload', {}, 'warmup-video_upload')
            if video_up is None:
                print("[WARN] video_upload dispatch failed")
                return
            
            up_out = self._get_output(video_up) or {}
            print(f"[DEBUG] video_upload output keys: {list(up_out.keys())}")
            
            video_key = self._first_item(up_out.get('video'))
            video_name_key = self._first_item(up_out.get('video_name'))
            segment_time_key = self._first_item(up_out.get('segment_time'))
            
            if video_key and video_name_key and segment_time_key:
                self.cached_payloads['video_upload'] = {}
                split_payload = {
                    "video": video_key,
                    "video_name": video_name_key,
                    "segment_time": segment_time_key
                }
                self.cached_payloads['video_split'] = split_payload
                
                split_res = dispatch_func('video_split', split_payload, 'warmup-video_split')
                if split_res:
                    split_out = self._get_output(split_res) or {}
                    chunks = split_out.get('splited_video') or []
                    chunk_key = self._first_item(chunks)
                    
                    target_type_key = f"const_target_{uuid.uuid4().hex[:4]}"
                    try:
                        self.redis_client.set(target_type_key, "avi")
                    except Exception:
                        pass
                    
                    if chunk_key:
                        transcode_payload = {"video": chunk_key, "target_type": target_type_key}
                        self.cached_payloads['video_transcode'] = transcode_payload
                        
                        trans_res = dispatch_func('video_transcode', transcode_payload, 
                                                 'warmup-video_transcode')
                        if trans_res:
                            trans_out = self._get_output(trans_res) or {}
                            trans_list = trans_out.get('transcoded_video') or []
                            if not isinstance(trans_list, list):
                                trans_list = [trans_list] if trans_list else []
                            
                            if trans_list:
                                merge_input_key = f"sys-merge-list-{uuid.uuid4().hex}"
                                try:
                                    self.redis_client.set(merge_input_key, 
                                                         "LIST_REF:" + json.dumps(trans_list))
                                    self.cached_payloads['video_merge'] = {
                                        "video": merge_input_key, 
                                        "target_type": target_type_key
                                    }
                                except Exception as e:
                                    print(f"[WARN] Failed to set video merge cache: {e}")
        except Exception as e:
            print(f"[WARN] Video workflow warmup failed: {e}")
    
    def _warmup_recognizer_workflow(self, dispatch_func):
        """预热 Recognizer 工作流"""
        print("[INFO] === Warming up Recognizer workflow ===")
        try:
            rec_up = dispatch_func('recognizer_upload', {}, 'warmup-recognizer_upload')
            if rec_up is None:
                print("[WARN] recognizer_upload dispatch failed")
                return
            
            rec_out = self._get_output(rec_up) or {}
            img_key = self._first_item(rec_out.get('img'))
            
            if img_key:
                self.cached_payloads['recognizer_upload'] = {}
                self.cached_payloads['recognizer_adult'] = {"img": img_key}
                self.cached_payloads['recognizer_violence'] = {"img": img_key}
                self.cached_payloads['recognizer_extract'] = {"img": img_key}
                
                extr_res = dispatch_func('recognizer_extract', {"img": img_key}, 
                                        'warmup-recognizer_extract')
                if extr_res:
                    extr_out = self._get_output(extr_res) or {}
                    text_key = self._first_item(extr_out.get('text'))
                    if text_key:
                        self.cached_payloads['recognizer_censor'] = {"text": text_key}
                        self.cached_payloads['recognizer_translate'] = {"text": text_key}
                
                self.cached_payloads['recognizer_mosaic'] = {"img": img_key}
        except Exception as e:
            print(f"[WARN] Recognizer workflow warmup failed: {e}")
    
    def _warmup_svd_workflow(self, dispatch_func):
        """预热 SVD 工作流"""
        print("[INFO] === Warming up SVD workflow ===")
        try:
            svd_start = dispatch_func('svd_start', {}, 'warmup-svd_start')
            if svd_start is None:
                print("[WARN] svd_start dispatch failed")
                return
            
            svd_out = self._get_output(svd_start) or {}
            matrix_key = self._first_item(svd_out.get('matrix'))
            
            if matrix_key:
                self.cached_payloads['svd_start'] = {}
                self.cached_payloads['svd_compute'] = {"matrix": matrix_key}
                
                compute_res = dispatch_func('svd_compute', {"matrix": matrix_key}, 
                                           'warmup-svd_compute')
                if compute_res:
                    comp_out = self._get_output(compute_res) or {}
                    comp_key = self._first_item(comp_out.get('res'))
                    if comp_key:
                        merge_list = [comp_key]
                        merge_key = f"sys-svd-list-{uuid.uuid4().hex}"
                        try:
                            self.redis_client.set(merge_key, 
                                                 "LIST_REF:" + json.dumps(merge_list))
                            self.cached_payloads['svd_merge'] = {"res": merge_key}
                        except Exception as e:
                            print(f"[WARN] Failed to set svd merge cache: {e}")
        except Exception as e:
            print(f"[WARN] SVD workflow warmup failed: {e}")
    
    def _warmup_wordcount_workflow(self, dispatch_func):
        """预热 WordCount 工作流"""
        print("[INFO] === Warming up WordCount workflow ===")
        try:
            wc_start = dispatch_func('wordcount_start', {}, 'warmup-wordcount_start')
            if wc_start is None:
                print("[WARN] wordcount_start dispatch failed")
                return
            
            wc_out = self._get_output(wc_start) or {}
            file_key = self._first_item(wc_out.get('file'))
            
            if file_key:
                self.cached_payloads['wordcount_start'] = {}
                self.cached_payloads['wordcount_count'] = {"file": file_key}
                
                count_res = dispatch_func('wordcount_count', {"file": file_key}, 
                                         'warmup-wordcount_count')
                if count_res:
                    count_out = self._get_output(count_res) or {}
                    count_key = self._first_item(count_out.get('res'))
                    if count_key:
                        merge_list = [count_key]
                        merge_key = f"sys-wc-list-{uuid.uuid4().hex}"
                        try:
                            self.redis_client.set(merge_key, 
                                                 "LIST_REF:" + json.dumps(merge_list))
                            self.cached_payloads['wordcount_merge'] = {"res": merge_key}
                        except Exception as e:
                            print(f"[WARN] Failed to set wordcount merge cache: {e}")
        except Exception as e:
            print(f"[WARN] WordCount workflow warmup failed: {e}")
    
    def _get_output(self, res):
        """提取响应中的 output 字段"""
        if res is None:
            return {}
        return res.get('output') if isinstance(res, dict) else {}
    
    def _first_item(self, val):
        """提取列表首元素或直接返回值"""
        if isinstance(val, list) and val:
            return val[0]
        return val
    
    def get_cached_payloads(self):
        """获取缓存的 payloads"""
        return self.cached_payloads
