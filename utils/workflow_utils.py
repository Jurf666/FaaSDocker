# utils/workflow_utils.py
import json
import uuid
import fnmatch
from config import WORKFLOW_CACHE_PATTERNS, SIMPLE_ACTIONS
from utils.request_handler import dispatch_simple

def prepare_workflow_caches(redis_client):
    """预热工作流一次, 固定生成各子函数可重复使用的输入"""
    if not redis_client:
        print("[WARN] Redis unavailable, skip workflow warmup")
        return {}
    
    caches = {}
    def _first_item(val):
        """提取列表首元素或直接返回值"""
        return val[0] if isinstance(val, list) and val else val
    
    """预热 Video 工作流"""
    def _warmup_video():
        try:
            up_out = dispatch_simple('video_upload', {}, 'warmup-video_upload', True)
            if not up_out:
                return
            video_key = _first_item(up_out.get('video'))
            name_key = _first_item(up_out.get('video_name'))
            time_key = _first_item(up_out.get('segment_time'))
            if not all([video_key, name_key, time_key]): return
            
            caches['video_upload'] = {}
            caches['video_split'] = {"video": video_key, "video_name": name_key, "segment_time": time_key}
            
            split_out = dispatch_simple('video_split', caches['video_split'], 'warmup-video_split', True)
            if split_out:
                chunk_key = _first_item(split_out.get('splited_video'))
                target_key = f"const_target_{uuid.uuid4().hex[:4]}"
                redis_client.set(target_key, "avi")
                caches['video_transcode'] = {"video": chunk_key, "target_type": target_key}
                
                trans_out = dispatch_simple('video_transcode', caches['video_transcode'], 'warmup-video_transcode', True)
                if trans_out:
                    trans_list = trans_out.get('transcoded_video', [])
                    merge_key = f"sys-merge-list-{uuid.uuid4().hex}"
                    redis_client.set(merge_key, "LIST_REF:" + json.dumps(trans_list))
                    caches['video_merge'] = {"video": merge_key, "target_type": target_key}
        except Exception as e:
            print(f"[WARN] Video workflow warmup failed: {e}")

    """预热 Recognizer 工作流"""
    def _warmup_recognizer():
        try:
            rec_out = dispatch_simple('recognizer_upload', {}, 'warmup-recognizer_upload', True)
            if not rec_out:
                return
            img_key = _first_item(rec_out.get('img'))
            if not img_key: return
            
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

    """预热 SVD 工作流"""
    def _warmup_svd():
        try:
            svd_out = dispatch_simple('svd_start', {}, 'warmup-svd_start', True)
            if not svd_out:
                return
            matrix_key = _first_item(svd_out.get('matrix'))
            if not matrix_key: return
            
            caches['svd_start'] = {}
            caches['svd_compute'] = {"matrix": matrix_key}
            
            compute_out = dispatch_simple('svd_compute', {"matrix": matrix_key}, 'warmup-svd_compute', True)
            if compute_out:
                comp_key = _first_item(compute_out.get('res'))
                if comp_key:
                    merge_list = [comp_key]
                    merge_key = f"sys-svd-list-{uuid.uuid4().hex}"
                    redis_client.set(merge_key, "LIST_REF:" + json.dumps(merge_list))
                    caches['svd_merge'] = {"res": merge_key}
        except Exception as e:
            print(f"[WARN] SVD workflow warmup failed: {e}")

    """预热 WordCount 工作流"""
    def _warmup_wordcount():
        try:
            wc_out = dispatch_simple('wordcount_start', {}, 'warmup-wordcount_start', True)
            if not wc_out:
                return
            file_key = _first_item(wc_out.get('file'))
            if not file_key: return
            
            caches['wordcount_start'] = {}
            caches['wordcount_count'] = {"file": file_key}
            
            count_out = dispatch_simple('wordcount_count', {"file": file_key}, 'warmup-wordcount_count', True)
            if count_out:
                count_key = _first_item(count_out.get('res'))
                if count_key:
                    merge_list = [count_key]
                    merge_key = f"sys-wc-list-{uuid.uuid4().hex}"
                    redis_client.set(merge_key, "LIST_REF:" + json.dumps(merge_list))
                    caches['wordcount_merge'] = {"res": merge_key}
        except Exception as e:
            print(f"[WARN] WordCount workflow warmup failed: {e}")

    # 执行预热
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
    """清理工作流中间数据"""
    print("\n[INFO] Cleaning up workflow data...")
    
    # 清理Redis
    if redis_client:
        try:
            all_keys = redis_client.keys('*')
            keys_to_delete = []
            for key in all_keys:
                # 检查是否匹配工作流模式
                for pattern in WORKFLOW_CACHE_PATTERNS:
                    if fnmatch.fnmatch(key, pattern):
                        keys_to_delete.append(key)
                        break
            if keys_to_delete:
                # 批量删除
                deleted = redis_client.delete(*keys_to_delete)
                print(f"[INFO] Deleted {deleted} workflow keys from Redis")
            else:
                print(f"[INFO] No workflow keys found in Redis (total keys: {len(all_keys)})")
        except Exception as e:
            print(f"[WARN] Redis cleanup failed: {e}")
    
    # 清理CouchDB
    if couchdb_client:
        try:
            import couchdb
            if 'faas_data' in couchdb_client:
                db = couchdb_client['faas_data']
                doc_count = 0
                docs_to_delete = []
                
                # 收集所有文档
                for doc_id in db:
                    # 跳过设计文档
                    if not doc_id.startswith('_'):
                        doc = db[doc_id]
                        docs_to_delete.append({'_id': doc_id, '_rev': doc['_rev'], '_deleted': True})
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
    
    print("[INFO] === Cleanup completed ===")