import logging
import uuid
import json
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger("Workflows")

# 辅助函数：标准化 dispatch 调用
def call_action(dispatcher, name, payload):
    res= dispatcher.dispatch_sync(name, payload, is_workflow=True)
    # 【修复标记-元数据结构兼容】
    # dispatch_sync 当前把运行元数据放在 __meta__ 下；
    # 这里同时兼容历史结构（直接放在 res['container_id']/res['duration']），避免 KeyError。
    meta_from_res = res.get('__meta__', {}) if isinstance(res, dict) else {}
    # 构造 metadata 供前端/日志查看
    meta = {
        'container_id': meta_from_res.get('container_id', res.get('container_id') if isinstance(res, dict) else None),
        'duration': meta_from_res.get('duration', res.get('duration') if isinstance(res, dict) else None)
    }
    return res, meta

def workflow_video(dispatcher, data_store, payload=None):
    logger.info("=== Starting Video Workflow ===")
    if not data_store.redis_client: 
        raise Exception("Redis not connected")

    subtasks = [] # 收集所有子任务元数据
    
    # Upload
    # 【修复标记-函数签名对齐】
    # workflow_engine 会把 payload 作为第三个参数传入，这里显式兼容；
    # 若上层不传 payload，则回退为 {}，保持历史行为不变。
    upload_out, meta = call_action(dispatcher, "video_upload", payload or {})
    subtasks.append({'name': 'video_upload', **meta})
    
    video_key = upload_out['video'][0]
    name_key = upload_out['video_name'][0]
    time_key = upload_out['segment_time'][0]

    # Split
    split_out, meta = call_action(dispatcher, "video_split", {
        "video": video_key, "video_name": name_key, "segment_time": time_key
    })
    subtasks.append({'name': 'video_split', **meta})
    chunks_keys = split_out.get('splited_video', [])

    # Transcode
    target_type_key = f"const_target_{uuid.uuid4().hex[:4]}"
    data_store.redis_client.set(target_type_key, "avi")

    def _run_transcode(chunk_key):
        res, meta = call_action(dispatcher, "video_transcode", {
            "video": chunk_key, "target_type": target_type_key
        })
        subtasks.append({'name': 'video_transcode', **meta})
        return res.get('transcoded_video', [None])[0]

    with ThreadPoolExecutor(max_workers=4) as executor:
        transcode_results = list(executor.map(_run_transcode, chunks_keys))
    transcode_results = [k for k in transcode_results if k]

    # Merge
    merge_input_key = f"sys-merge-list-{uuid.uuid4().hex}"
    data_store.redis_client.set(merge_input_key, "LIST_REF:" + json.dumps(transcode_results))
    
    merge_out, meta = call_action(dispatcher, "video_merge", {
        "video": merge_input_key, "target_type": target_type_key
    })
    subtasks.append({'name': 'video_merge', **meta})
        
    final_key = merge_out.get('final_video', [None])[0]
    if final_key: 
        data_store.save_result(final_key, "final_video.avi")
    
    return subtasks

def workflow_recognizer(dispatcher, data_store, payload=None):
    logger.info("=== Starting Recognizer Workflow ===")
    if not data_store.redis_client: raise Exception("Redis not connected")

    subtasks = []
    
    # 1. Upload
    # 【修复标记-函数签名对齐】
    # 同 video：兼容 workflow_engine 透传 payload 的调用方式。
    upload_out, meta = call_action(dispatcher, "recognizer_upload", payload or {})
    subtasks.append({'name': 'recognizer_upload', **meta})
    img_key = upload_out['img'][0]
        
    # 2. Parallel Analysis
    def _run_branch(action):
        res, meta = call_action(dispatcher, action, {"img": img_key})
        subtasks.append({'name': action, **meta})
        return res

    with ThreadPoolExecutor(max_workers=3) as ex:
        f_adult = ex.submit(_run_branch, "recognizer_adult")
        f_viol = ex.submit(_run_branch, "recognizer_violence")
        f_extr = ex.submit(_run_branch, "recognizer_extract")
            
        key_adult = f_adult.result().get('illegal', [None])[0]
        key_viol = f_viol.result().get('illegal', [None])[0]
        key_text = f_extr.result().get('text', [None])[0]

    is_adult = json.loads(data_store.redis_client.get(key_adult))
    is_viol = json.loads(data_store.redis_client.get(key_viol))
        
    # 3. Text Analysis
    res_censor_out, meta = call_action(dispatcher, "recognizer_censor", {"text": key_text})
    subtasks.append({'name': 'recognizer_censor', **meta})
    _, meta = call_action(dispatcher, "recognizer_translate", {"text": key_text})
    subtasks.append({'name': 'recognizer_translate', **meta})
        
    key_censor = res_censor_out.get('illegal', [None])[0]
    is_censor = json.loads(data_store.redis_client.get(key_censor))

    # 4. Decision
    if is_adult or is_viol or is_censor:
        mosaic_out, meta = call_action(dispatcher, "recognizer_mosaic", {"img": img_key})
        subtasks.append({'name': 'recognizer_mosaic', **meta})
        mosaic_keys = mosaic_out.get('mosaic_image', [])
        if mosaic_keys: 
            data_store.save_result(mosaic_keys[0], "mosaic_result.jpg")
        
    # 5. Report
    report = {"is_adult": is_adult, "is_violence": is_viol, "is_censor": is_censor}
    report_key = f"report-{uuid.uuid4().hex}"
    data_store.redis_client.set(report_key, json.dumps(report))
    data_store.save_result(report_key, "recognizer_report.json")
    
    return subtasks

def workflow_svd(dispatcher, data_store):
    logger.info("=== Starting SVD Workflow ===")
    if not data_store.redis_client: raise Exception("Redis not connected")
    subtasks = []

    start_out, meta = call_action(dispatcher, "svd_start", {})
    subtasks.append({'name': 'svd_start', **meta})
    matrix_keys = start_out.get('matrix', [])
        
    def _run_compute(m_key):
        res, meta = call_action(dispatcher, "svd_compute", {"matrix": m_key})
        subtasks.append({'name': 'svd_compute', **meta})
        return res.get('res', [None])[0]
            
    with ThreadPoolExecutor(max_workers=len(matrix_keys) or 1) as ex:
        compute_results = list(ex.map(_run_compute, matrix_keys))
    compute_results = [k for k in compute_results if k]
        
    merge_input_key = f"sys-svd-list-{uuid.uuid4().hex}"
    data_store.redis_client.set(merge_input_key, "LIST_REF:" + json.dumps(compute_results))
    
    merge_out, meta = call_action(dispatcher, "svd_merge", {"res": merge_input_key})
    subtasks.append({'name': 'svd_merge', **meta})
        
    res_keys = merge_out.get('final_res', [])
    if res_keys: 
        data_store.save_result(res_keys[0], "svd_result.pkl")
    return subtasks

def workflow_wordcount(dispatcher, data_store):
    logger.info("=== Starting WordCount Workflow ===")
    if not data_store.redis_client: raise Exception("Redis not connected")
    subtasks = []

    start_out, meta = call_action(dispatcher, "wordcount_start", {})
    subtasks.append({'name': 'wordcount_start', **meta})
    file_keys = start_out.get('file', [])
    if not file_keys: return subtasks

    def _run_count(f_key):
        res, meta = call_action(dispatcher, "wordcount_count", {"file": f_key})
        subtasks.append({'name': 'wordcount_count', **meta})
        return res.get('res', [None])[0]
        
    with ThreadPoolExecutor(max_workers=len(file_keys)) as ex:
        count_results = list(ex.map(_run_count, file_keys))
    count_results = [k for k in count_results if k]
        
    merge_input_key = f"sys-wc-list-{uuid.uuid4().hex}"
    data_store.redis_client.set(merge_input_key, "LIST_REF:" + json.dumps(count_results))
    
    merge_out, meta = call_action(dispatcher, "wordcount_merge", {"res": merge_input_key})
    subtasks.append({'name': 'wordcount_merge', **meta})
        
    count_keys = merge_out.get('final_count', [])
    if count_keys: 
        data_store.save_result(count_keys[0], "wordcount_result.txt")
    return subtasks
