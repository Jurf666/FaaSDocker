import json
import os
import re
from collections import defaultdict

STORAGE_DIR = '/storage'

def main(event):
    # 从 controller 接收*一个*切片路径
    chunk_path = event.get('chunk_path')
    
    if not chunk_path or not os.path.exists(chunk_path):
        raise FileNotFoundError(f"WORDCOUNT_COUNT: Chunk file not found at {chunk_path}")

    print(f"WORDCOUNT_COUNT: Processing chunk {chunk_path}")
    
    # 1. 从 /storage 加载文本块
    with open(chunk_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 2. 执行单词计数逻辑
    # (使用 main(count).py 中的逻辑)
    dic = defaultdict(int)
    words = re.findall(r'[a-zA-Z0-9]+', content.lower())
    
    for word in words:
        dic[word] += 1
            
    # 3. 确保输出目录存在
    output_dir = os.path.join(STORAGE_DIR, 'output','wordcount_count')
    os.makedirs(output_dir, exist_ok=True)
    
    # 4. 将部分结果 (dict) 保存为 JSON 文件
    # (从输入路径派生一个唯一的文件名)
    base_name = os.path.basename(chunk_path) # e.g., "chunk_0.txt"
    result_filename = f"count_{os.path.splitext(base_name)[0]}.json" # e.g., "count_chunk_0.json"
    result_filepath = os.path.join(output_dir, result_filename)
    
    with open(result_filepath, 'w') as f:
        json.dump(dic, f)

    print(f"WORDCOUNT_COUNT: Finished chunk {chunk_path}. Result saved to {result_filepath}")

    # 5. 返回指向*结果路径*的 JSON
    return {
        "result_path": result_filepath
    }