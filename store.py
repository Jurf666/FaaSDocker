import os
import json
import redis
import couchdb
import pickle
import base64

# --- 配置部分 ---
# 在 Proxy 启动时通过环境变量注入，或者使用默认值
REDIS_HOST = os.environ.get('REDIS_HOST', '172.17.0.1')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
COUCHDB_URL = os.environ.get('COUCHDB_URL', 'http://openwhisk:openwhisk@172.17.0.1:5984/')

# 阈值：超过 32KB 的数据存入 CouchDB，否则存 Redis
LARGE_DATA_THRESHOLD = 32 * 1024 
# 【修复标记-小二进制Redis编码】
# decode_responses=True 的 Redis 客户端对原始 bytes 不安全，
# 小二进制统一使用 Base64 文本 + 前缀落在 Redis，fetch 时再还原为 bytes。
REDIS_OCTET_B64_PREFIX = "B64_OCTET:"

class Store:
    def __init__(self, request_id, input_mapping):
        """
        初始化 Store
        :request_id: 当前请求的唯一 ID (用于生成 Key)
        :input_mapping: 字典，告诉 Store 输入参数对应的数据库 Key
                              例如: {'video': 'req-101_split_output_0'}
        """
        self.request_id = request_id
        self.input_mapping = input_mapping
        
        # 初始化连接
        self.redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        # Redis 二进制连接 (用于直接存取 bytes)
        self.redis_client_bin = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=False)
        
        self.couch = couchdb.Server(COUCHDB_URL)
        self.db_name = "faas_data"
        
        # 确保 CouchDB 数据库存在
        if self.db_name not in self.couch:
            try:
                self.couch.create(self.db_name)
            except:
                pass # 忽略并发创建时的错误
        self.couch_db = self.couch[self.db_name]

    def fetch(self, keys):
        """
        获取数据 (支持批量)
        keys: 列表，例如 ['video', 'segment_time']
        return一个字典，例如 {'video': b'...', 'segment_time': 10}
        """
        result = {}
        for key_name in keys:
            # 1. 从 input_mapping 查找真实的 DB Key
            # 如果 input_mapping 里没有，说明这是一个普通参数，直接尝试用 key_name 去 Redis 找（或者是 Controller 直接传的值）
            db_key = self.input_mapping.get(key_name)
            
            if not db_key:
                print(f"[Store] Warning: Input key '{key_name}' not found in mapping.")
                result[key_name] = None
                continue

            # 2. 从 Redis 获取元数据或值
            redis_val = self.redis_client.get(db_key)
            
            if redis_val is None:
                print(f"[Store] Error: Key '{db_key}' not found in Redis.")
                result[key_name] = None
                continue
            # --- 新增: 支持 LIST_REF ---
            if isinstance(redis_val, str) and redis_val.startswith("LIST_REF:"):
                # 解析 Key 列表
                key_list_json = redis_val.split(":", 1)[1]
                key_list = json.loads(key_list_json)
                
                # 递归 fetch 列表中的每个 key
                # 这里我们需要手动调用 internal fetch，不能调 self.fetch 因为它依赖 mapping
                data_list = []
                for k in key_list:
                    # 直接从 Redis 取
                    val = self.redis_client.get(k)
                    if val and val.startswith("COUCH_REF:"):
                        cid = val.split(":", 1)[1]
                        data_list.append(self._fetch_from_couch(cid))
                    # 【修复标记-小二进制Redis解码】
                    # LIST_REF 里的每个元素也可能是 B64_OCTET 前缀编码的二进制数据。
                    elif isinstance(val, str) and val.startswith(REDIS_OCTET_B64_PREFIX):
                        b64_payload = val[len(REDIS_OCTET_B64_PREFIX):]
                        data_list.append(base64.b64decode(b64_payload.encode('ascii')))
                    else:
                        try:
                            data_list.append(json.loads(val))
                        except:
                            data_list.append(val)
                result[key_name] = data_list
            # ---------------------------
            # 3. 解析数据
            # 检查是否是指向 CouchDB 的指针
            elif redis_val.startswith("COUCH_REF:"):
                couch_doc_id = redis_val.split(":", 1)[1]
                result[key_name] = self._fetch_from_couch(couch_doc_id)
            # 【修复标记-小二进制Redis解码】
            # 非 LIST_REF 直读路径也要支持 Base64 前缀还原。
            elif isinstance(redis_val, str) and redis_val.startswith(REDIS_OCTET_B64_PREFIX):
                b64_payload = redis_val[len(REDIS_OCTET_B64_PREFIX):]
                result[key_name] = base64.b64decode(b64_payload.encode('ascii'))
            else:
                # 尝试解析 JSON，如果失败则作为普通字符串/数字返回
                try:
                    result[key_name] = json.loads(redis_val)
                except (json.JSONDecodeError, TypeError):
                     # 如果不是 JSON，可能是 Controller 传入的简单字符串
                    result[key_name] = redis_val

        return result

    def _fetch_from_couch(self, doc_id):
        """内部方法：从 CouchDB 下载附件"""
        try:
            doc = self.couch_db[doc_id]
            # 假设附件名固定为 'data'
            attachment = self.couch_db.get_attachment(doc, 'data')
            if attachment:
                raw_bytes = attachment.read()
                # 【修复标记-Couch返回类型统一】
                # json 类型在写入时是 json.dumps 文本，这里应恢复为 Python 对象；
                # octet 类型保持 bytes，避免破坏二进制处理链路。
                if doc.get('type') == 'json':
                    try:
                        return json.loads(raw_bytes.decode('utf-8'))
                    except Exception:
                        # 兜底：即便反序列化失败，也返回原始文本，避免 silent data loss。
                        return raw_bytes.decode('utf-8', errors='replace')
                return raw_bytes # 返回 bytes
            return None
        except Exception as e:
            print(f"[Store] CouchDB fetch error: {e}")
            return None

    def post(self, key, value, datatype='json'):
        """
        上传数据
        :key: 输出数据的名称 (例如 'transcoded_video')
        :value: 数据内容 (bytes, str, int, dict, list)
        :datatype: 'json' 或 'octet' (二进制流)
        :return: 实际存储在 Redis 中的 Key (Controller 需要这个 Key 传给下一个 Action)
        """
        # 内部维护一个计数器，处理同名 Key 的多次输出 (用于 Generator 模式)
        if not hasattr(self, 'key_counters'):
            self.key_counters = {}
        
        if key not in self.key_counters:
            self.key_counters[key] = 0
        else:
            self.key_counters[key] += 1
            
        index = self.key_counters[key]
        
        # 生成带序号的 Key: req-123_matrix_0, req-123_matrix_1
        # 如果是第一次且后续不再有(无法预知)，通常 list 类型的 output 我们都加后缀
        # 为简化，我们在 key 后统一加下标
        db_key = f"{self.request_id}_{key}_{index}"
        
        data_size = 0
        
        # 1. 预处理数据
        if datatype == 'octet' or isinstance(value, bytes):
            data_size = len(value)
            content_to_store = value
        else:
            # 序列化 JSON
            content_to_store = json.dumps(value)
            data_size = len(content_to_store.encode('utf-8'))

        # 2. 判断存储位置
        if data_size > LARGE_DATA_THRESHOLD:
            # ---> 存入 CouchDB
            try:
                # 创建一个文档
                doc_id = db_key # 使用 db_key 作为文档 ID
                # 如果文档已存在（重试时），先删除或更新。简化起见，这里假设 request_id 唯一
                if doc_id in self.couch_db:
                    doc = self.couch_db[doc_id]
                    self.couch_db.delete(doc)
                
                self.couch_db[doc_id] = {'type': datatype, 'size': data_size}
                
                # 上传附件
                # 如果是 json 字符串，先转 bytes
                if isinstance(content_to_store, str):
                    content_to_store = content_to_store.encode('utf-8')
                    
                self.couch_db.put_attachment(self.couch_db[doc_id], content_to_store, filename='data', content_type='application/octet-stream')
                
                # 在 Redis 中存指针
                self.redis_client.set(db_key, f"COUCH_REF:{doc_id}")
                print(f"[Store] Large data '{key}' ({data_size} bytes) saved to CouchDB: {doc_id}")
                
            except Exception as e:
                print(f"[Store] Error saving to CouchDB: {e}")
                raise e
        else:
            # ---> 存入 Redis
            if datatype == 'octet' or isinstance(value, bytes):
                 # Redis 存二进制需要用 bin client 或 base64。这里为了简单，如果 store.py 用于纯文本协议，建议存 base64
                 # 但考虑到性能，我们可以让 Redis Client 自动处理 bytes，但前面的 input_mapping 获取时要注意
                 # 简单起见：小二进制数据我们暂时存 Base64 字符串，方便 JSON 序列化
                 if isinstance(value, bytes):
                     # 如果是小文件二进制，存 CouchDB 其实更省心，但为了演示分级：
                     # 强制转为 CouchDB 存储或者 Base64
                     # 这里我们策略调整：二进制无论大小，只要是 octet 且比较大就去 couch，
                     # 极小的二进制(如hash)存 Redis。
                     # 为防编码问题，建议凡是 'octet' 类型直接走 CouchDB 逻辑，或者在此处 Base64。
                     # 这里采用：存入 Redis 字符串
                     # 【修复标记-小二进制Redis编码】
                     # 避免 decode_responses=True 客户端在读取时对 bytes 解码失败。
                     b64_payload = base64.b64encode(value).decode('ascii')
                     content_to_store = f"{REDIS_OCTET_B64_PREFIX}{b64_payload}"
                 
                 self.redis_client.set(db_key, content_to_store)
            else:
                 self.redis_client.set(db_key, content_to_store)
            
            print(f"[Store] Small data '{key}' saved to Redis: {db_key}")

        # 3. 非常重要：Action 不直接返回数据了，而是将 "输出数据的 Key" 记录下来
        # 这些 Key 会被 Proxy 收集，最后返回给 Controller
        # 我们这里不需要 return，因为 Proxy 会想办法收集。
        # 最简单的方法：Store 内部维护一个 outputs 字典
        if not hasattr(self, 'output_keys'):
            self.output_keys = {}
        # 如果是第一次存这个 key，初始化为列表
        if key not in self.output_keys:
            self.output_keys[key] = []
        
        # 将生成的 db_key 加入列表
        self.output_keys[key].append(db_key)
        
        return db_key
