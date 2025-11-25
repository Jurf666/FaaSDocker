import json
import sys

# --- 主逻辑 ---

print("[Merge] Starting WordCount Merge...", flush=True)

try:
    # 1. 获取输入
    # Store 会处理 LIST_REF，返回一个列表，包含所有 Count 节点的输出
    data_map = store.fetch(['res'])
    counts_list = data_map.get('res')

    if counts_list is None:
        raise ValueError("Input 'res' is None. Check Controller LIST_REF logic.")
    
    print(f"[Merge] Received list with {len(counts_list)} items.", flush=True)

    final_res = {}

    # 2. 遍历并合并
    for i, wc in enumerate(counts_list):
        # --- 关键修复: 类型清洗 ---
        # 如果是 bytes，先转 string
        if isinstance(wc, bytes):
            wc = wc.decode('utf-8', errors='ignore')
        
        # 如果是 string，尝试解析为 JSON (Dict)
        if isinstance(wc, str):
            try:
                wc = json.loads(wc)
            except json.JSONDecodeError:
                print(f"[Merge] Warning: Item {i} is not valid JSON, skipping. Content: {wc}", flush=True)
                continue
        
        # 此时 wc 应该是字典
        if not isinstance(wc, dict):
            print(f"[Merge] Warning: Item {i} is type {type(wc)}, expected dict. Skipping.", flush=True)
            continue
        # ------------------------

        # 执行合并逻辑
        for w, count in wc.items():
            if w not in final_res:
                final_res[w] = count
            else:
                final_res[w] += count

    print(f"[Merge] Merged total unique words: {len(final_res)}", flush=True)

    # 3. 上传结果
    # 这里我们上传 total count (int)，也可以上传 final_res (dict)
    store.post('final_count', len(final_res))
    
    # 如果您想保存完整单词列表供查看，可以取消下面这行的注释
    # store.post('full_result', final_res)

except Exception as e:
    print(f"[Merge] Error: {e}", flush=True)
    import traceback
    traceback.print_exc()
    raise e