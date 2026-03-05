# utils/workflow_utils.py
import os
import json
import uuid
import fnmatch
from config import WORKFLOW_CACHE_PATTERNS
from utils.request_handler import dispatch_simple

# 【新增-与服务端保持一致】默认 32KB 阈值；可通过环境变量覆盖
LARGE_DATA_THRESHOLD = int(os.environ.get('LARGE_DATA_THRESHOLD', str(32 * 1024)))


def _first_item(val):
    """提取列表首元素；若不是列表则原样返回。"""
    return val[0] if isinstance(val, list) and val else val


# =====================================================================
# 【新增-预热阶段统一存储入口】
# 目的：避免 warmup 直接裸写 Redis，改为遵循“小数据 Redis / 大数据 CouchDB”。
# =====================================================================
def _store_json_payload(redis_client, couchdb_client, key_prefix, value):
    """
    存储 JSON 语义的数据：
    - 小于阈值：Redis 直接存 JSON 文本
    - 超过阈值：CouchDB 存附件 + Redis 存 COUCH_REF 指针
    """
    db_key = f"sys-{key_prefix}-{uuid.uuid4().hex}"
    content = json.dumps(value)
    content_bytes = content.encode('utf-8')

    if couchdb_client and len(content_bytes) > LARGE_DATA_THRESHOLD:
        try:
            if 'faas_data' not in couchdb_client:
                couchdb_client.create('faas_data')
            db = couchdb_client['faas_data']

            if db_key in db:
                db.delete(db[db_key])

            db[db_key] = {'type': 'json', 'size': len(content_bytes)}
            db.put_attachment(
                db[db_key],
                content_bytes,
                filename='data',
                content_type='application/json'
            )

            redis_client.set(db_key, f"COUCH_REF:{db_key}")
            return db_key
        except Exception as e:
            # CouchDB 失败时降级到 Redis，保证 warmup 不中断
            print(f"[WARN] Fallback to Redis for {key_prefix}: {e}")

    redis_client.set(db_key, content)
    return db_key


def _store_list_ref(redis_client, key_prefix, key_list):
    """
    存储 LIST_REF 元数据。
    注意：LIST_REF 需要放在 Redis，供 Store.fetch() 前缀解析。
    """
    db_key = f"sys-{key_prefix}-{uuid.uuid4().hex}"
    redis_client.set(db_key, "LIST_REF:" + json.dumps(key_list))
    return db_key


def prepare_workflow_caches(redis_client, couchdb_client=None):
    """
    预热工作流：提前跑一遍关键子函数，拿到可复用的输入 key，
    供后续压测 client 循环请求复用，减少冷启动和首轮数据准备开销。
    """
    if not redis_client:
        print("[WARN] Redis unavailable, skip workflow warmup")
        return {}

    caches = {}

    # ----------------------------
    # 预热 Video 工作流
    # ----------------------------
    def _warmup_video():
        try:
            up_out = dispatch_simple('video_upload', {}, 'warmup-video_upload', True)
            if not up_out:
                return

            video_key = _first_item(up_out.get('video'))
            name_key = _first_item(up_out.get('video_name'))
            time_key = _first_item(up_out.get('segment_time'))
            if not all([video_key, name_key, time_key]):
                return

            caches['video_upload'] = {}
            caches['video_split'] = {
                "video": video_key,
                "video_name": name_key,
                "segment_time": time_key
            }

            split_out = dispatch_simple('video_split', caches['video_split'], 'warmup-video_split', True)
            if not split_out:
                return

            chunk_key = _first_item(split_out.get('splited_video'))
            if not chunk_key:
                return

            # 【新增-统一存储】不再直接 redis.set('const_target_xxx', 'avi')
            target_key = _store_json_payload(redis_client, couchdb_client, 'const-target', 'avi')
            caches['video_transcode'] = {"video": chunk_key, "target_type": target_key}

            trans_out = dispatch_simple('video_transcode', caches['video_transcode'], 'warmup-video_transcode', True)
            if trans_out:
                trans_list = trans_out.get('transcoded_video', [])
                if trans_list:
                    # 【新增-统一存储】LIST_REF 通过 helper 创建
                    merge_key = _store_list_ref(redis_client, 'merge-list', trans_list)
                    caches['video_merge'] = {"video": merge_key, "target_type": target_key}

        except Exception as e:
            print(f"[WARN] Video workflow warmup failed: {e}")

    # ----------------------------
    # 预热 Recognizer 工作流
    # ----------------------------
    def _warmup_recognizer():
        try:
            rec_out = dispatch_simple('recognizer_upload', {}, 'warmup-recognizer_upload', True)
            if not rec_out:
                return

            img_key = _first_item(rec_out.get('img'))
            if not img_key:
                return

            caches['recognizer_upload'] = {}
            caches['recognizer_adult'] = {"img": img_key}
            caches['recognizer_violence'] = {"img": img_key}
            caches['recognizer_extract'] = {"img": img_key}

            extr_out = dispatch_simple('recognizer_extract', {"img": img_key}, 'warmup-recognizer_extract', True)
            if extr_out:
                text_key = _first_item(extr_out.get('text'))
                if text_key:
                    caches['recognizer_censor'] = {"text": text_key}
                    caches['recognizer_translate'] = {"text": text_key}

            caches['recognizer_mosaic'] = {"img": img_key}

        except Exception as e:
            print(f"[WARN] Recognizer workflow warmup failed: {e}")

    # ----------------------------
    # 预热 SVD 工作流
    # ----------------------------
    def _warmup_svd():
        try:
            svd_out = dispatch_simple('svd_start', {}, 'warmup-svd_start', True)
            if not svd_out:
                return

            matrix_key = _first_item(svd_out.get('matrix'))
            if not matrix_key:
                return

            caches['svd_start'] = {}
            caches['svd_compute'] = {"matrix": matrix_key}

            compute_out = dispatch_simple('svd_compute', {"matrix": matrix_key}, 'warmup-svd_compute', True)
            if compute_out:
                comp_key = _first_item(compute_out.get('res'))
                if comp_key:
                    # 【新增-统一存储】LIST_REF 通过 helper 创建
                    merge_key = _store_list_ref(redis_client, 'svd-list', [comp_key])
                    caches['svd_merge'] = {"res": merge_key}

        except Exception as e:
            print(f"[WARN] SVD workflow warmup failed: {e}")

    # ----------------------------
    # 预热 WordCount 工作流
    # ----------------------------
    def _warmup_wordcount():
        try:
            wc_out = dispatch_simple('wordcount_start', {}, 'warmup-wordcount_start', True)
            if not wc_out:
                return

            file_key = _first_item(wc_out.get('file'))
            if not file_key:
                return

            caches['wordcount_start'] = {}
            caches['wordcount_count'] = {"file": file_key}

            count_out = dispatch_simple('wordcount_count', {"file": file_key}, 'warmup-wordcount_count', True)
            if count_out:
                count_key = _first_item(count_out.get('res'))
                if count_key:
                    # 【新增-统一存储】LIST_REF 通过 helper 创建
                    merge_key = _store_list_ref(redis_client, 'wc-list', [count_key])
                    caches['wordcount_merge'] = {"res": merge_key}

        except Exception as e:
            print(f"[WARN] WordCount workflow warmup failed: {e}")

    # --- 执行预热 ---
    print("[INFO] Warming up Video workflow...")
    _warmup_video()
    print("[INFO] Warming up Recognizer workflow...")
    _warmup_recognizer()
    print("[INFO] Warming up SVD workflow...")
    _warmup_svd()
    print("[INFO] Warming up WordCount workflow...")
    _warmup_wordcount()

    print(f"[INFO] Workflow caches prepared for {len(caches)} functions")
    return caches


def cleanup_workflow_data(redis_client, couchdb_client):
    """清理工作流中间数据。"""
    print("\n[INFO] Cleaning up workflow data...")

    # 清理 Redis
    if redis_client:
        try:
            all_keys = redis_client.keys('*')
            keys_to_delete = []
            for key in all_keys:
                for pattern in WORKFLOW_CACHE_PATTERNS:
                    if fnmatch.fnmatch(key, pattern):
                        keys_to_delete.append(key)
                        break

            if keys_to_delete:
                deleted = redis_client.delete(*keys_to_delete)
                print(f"[INFO] Deleted {deleted} workflow keys from Redis")
            else:
                print(f"[INFO] No workflow keys found in Redis (total keys: {len(all_keys)})")
        except Exception as e:
            print(f"[WARN] Redis cleanup failed: {e}")

    # 清理 CouchDB
    if couchdb_client:
        try:
            if 'faas_data' in couchdb_client:
                db = couchdb_client['faas_data']
                doc_count = 0
                docs_to_delete = []

                for doc_id in db:
                    if not doc_id.startswith('_'):
                        doc = db[doc_id]
                        docs_to_delete.append({'_id': doc_id, '_rev': doc['_rev'], '_deleted': True})
                        doc_count += 1

                if docs_to_delete:
                    db.update(docs_to_delete)
                    print(f"[INFO] Deleted {doc_count} documents from CouchDB faas_data database")
                else:
                    print("[INFO] No documents found in CouchDB faas_data database")
            else:
                print("[INFO] CouchDB faas_data database does not exist, nothing to clean")
        except Exception as e:
            print(f"[WARN] Failed to cleanup CouchDB: {e}")

    print("[INFO] === Cleanup completed ===")
