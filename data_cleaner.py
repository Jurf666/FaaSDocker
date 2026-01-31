"""
DataCleaner - 负责清理工作流产生的中间数据
"""
import fnmatch
import redis


class DataCleaner:
    """数据清理管理器"""
    
    def __init__(self, redis_host='172.17.0.1', redis_port=6379, 
                 couchdb_url='http://openwhisk:openwhisk@172.17.0.1:5984/'):
        """
        初始化 DataCleaner
        
        Args:
            redis_host: Redis 服务器地址
            redis_port: Redis 端口
            couchdb_url: CouchDB URL
        """
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.couchdb_url = couchdb_url
        self.redis_client = None
        self.couchdb_client = None
    
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
            print(f"[WARN] Redis not available: {e}")
        return self.redis_client
    
    def init_couchdb(self):
        """初始化 CouchDB 连接"""
        if self.couchdb_client:
            return self.couchdb_client
        try:
            import couchdb
            self.couchdb_client = couchdb.Server(self.couchdb_url)
            print(f"[INFO] Connected to CouchDB at {self.couchdb_url}")
        except Exception as e:
            self.couchdb_client = None
            print(f"[WARN] CouchDB not available: {e}")
        return self.couchdb_client
    
    def cleanup_all(self):
        """清理所有工作流中间数据"""
        print("\n[INFO] === Cleaning up workflow intermediate data ===")
        self._cleanup_redis()
        self._cleanup_couchdb()
        print("[INFO] === Cleanup completed ===")
    
    def _cleanup_redis(self):
        """清理 Redis 中的工作流相关 key"""
        if not self.init_redis():
            return
        
        try:
            all_keys = self.redis_client.keys('*')
            workflow_patterns = [
                'req-*', 'warmup-*', 'sys-*', 'const_target_*',
                '*video*', '*recognizer*', '*svd*', '*wordcount*',
                '*split*', '*transcode*', '*merge*', '*upload*',
                '*adult*', '*violence*', '*extract*', '*censor*',
                '*translate*', '*mosaic*', '*compute*', '*count*'
            ]
            
            keys_to_delete = []
            for key in all_keys:
                for pattern in workflow_patterns:
                    if fnmatch.fnmatch(key, pattern):
                        keys_to_delete.append(key)
                        break
            
            if keys_to_delete:
                deleted = self.redis_client.delete(*keys_to_delete)
                print(f"[INFO] Deleted {deleted} workflow keys from Redis")
            else:
                print(f"[INFO] No workflow keys found in Redis (total keys: {len(all_keys)})")
        except Exception as e:
            print(f"[WARN] Failed to cleanup Redis: {e}")
    
    def _cleanup_couchdb(self):
        """清理 CouchDB 中的 faas_data 数据库"""
        if not self.init_couchdb():
            return
        
        try:
            import couchdb
            if 'faas_data' in self.couchdb_client:
                db = self.couchdb_client['faas_data']
                doc_count = 0
                docs_to_delete = []
                
                # 收集所有文档
                for doc_id in db:
                    if not doc_id.startswith('_'):
                        doc = db[doc_id]
                        docs_to_delete.append({
                            '_id': doc_id, 
                            '_rev': doc['_rev'], 
                            '_deleted': True
                        })
                        doc_count += 1
                
                # 批量删除
                if docs_to_delete:
                    db.update(docs_to_delete)
                    print(f"[INFO] Deleted {doc_count} documents from CouchDB faas_data database")
                else:
                    print(f"[INFO] No documents found in CouchDB faas_data database")
            else:
                print(f"[INFO] CouchDB faas_data database does not exist, nothing to clean")
        except Exception as e:
            print(f"[WARN] Failed to cleanup CouchDB: {e}")
