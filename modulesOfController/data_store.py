import os
import redis
import couchdb
import threading
import json
import logging

logger = logging.getLogger("DataStore")

class DataStore:
    def __init__(self):
        # 配置
        self.redis_host = os.environ.get('REDIS_HOST', '127.0.0.1')
        self.redis_port = int(os.environ.get('REDIS_PORT', 6379))
        self.couch_url = os.environ.get('COUCHDB_URL', 'http://openwhisk:openwhisk@127.0.0.1:5984/')
        
        # 客户端
        self.redis_client = None
        self.couch_db = None
        
        # 任务状态存储
        self.task_status_store = {}
        self.task_store_lock = threading.Lock()
        
        self._init_connections()

    # --- 数据库初始化 ---
    def _init_connections(self):
        try:
            self.redis_client = redis.StrictRedis(
                host=self.redis_host, port=self.redis_port, db=0, decode_responses=True
            )
            self.redis_client.ping()
            
            couch_server = couchdb.Server(self.couch_url)
            if 'faas_data' not in couch_server:
                couch_server.create('faas_data')
            self.couch_db = couch_server['faas_data']
            logger.info("DB Connected: Redis & CouchDB ready.")
        except Exception as e:
            logger.warning(f"DB Connection Failed: {e}. Workflows may fail.")

    def update_task_status(self, task_id, status, subtasks=None):
        '''
            线程安全地更新后台任务的状态。
            它主要被 run_workflow_async 函数调用:
                任务刚开始时：标记为 "running"
                任务成功结束时：标记为 "completed"
                任务报错时：标记为 "failed"
        '''
        with self.task_store_lock:
            self.task_status_store[task_id] = {
                "status": status, 
                "subtasks": subtasks or []
            }

    def get_task_info(self, task_id):
        with self.task_store_lock:
            return self.task_status_store.get(task_id, {"status": "unknown", "subtasks": []})

    def save_result(self, db_key, filename):
        """用于可视化工作流执行情况"""
        if not self.redis_client:
            return
        output_dir = './results'
        if not os.path.exists(output_dir): 
            os.makedirs(output_dir)
        filepath = os.path.join(output_dir, filename)
        
        try:
            val = self.redis_client.get(db_key)
            if val and val.startswith("COUCH_REF:") and self.couch_db:
                doc_id = val.split(":", 1)[1]
                if doc_id in self.couch_db:
                    with open(filepath, 'wb') as f:
                        f.write(self.couch_db.get_attachment(doc_id, 'data').read())
            elif val:
                with open(filepath, 'w') as f:
                    try:
                        json.dump(json.loads(val), f, indent=2)
                    except:
                        f.write(str(val))
        except Exception as e:
            logger.error(f"[Save] Error: {e}")