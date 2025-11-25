import re
import sys

# --- 主逻辑 ---

print("[Count] Starting action...", flush=True)

try:
    # 1. 获取数据
    data_map = store.fetch(['file'])
    content = data_map.get('file')

    # 2. 类型检查与解码 (修复 Bug 的关键步骤)
    if content is None:
        raise ValueError("Input content is None")
    
    # 如果是 bytes 类型，解码成字符串
    if isinstance(content, bytes):
        print(f"[Count] Decoding {len(content)} bytes to string...", flush=True)
        content = content.decode('utf-8', errors='ignore') # 使用 ignore 忽略可能的编码错误
    
    # 3. 执行统计
    res = {}
    # 此时 content 已经是 str 类型，可以安全使用 r'\W+' (str pattern)
    words = re.split(r'\W+', content)
    
    for w in words:
        if not w: continue
        w = w.lower()
        if w not in res:
            res[w] = 1
        else:
            res[w] += 1
            
    print(f"[Count] Processed {len(words)} words. Unique: {len(res)}", flush=True)

    # 4. 上传结果
    store.post('res', res)

except Exception as e:
    print(f"[Count] Error: {e}", flush=True)
    import traceback
    traceback.print_exc()
    raise e