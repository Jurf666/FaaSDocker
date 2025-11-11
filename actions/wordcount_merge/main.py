import json
import os
from collections import defaultdict

def main(event):
    # 从 controller 接收所有 count 任务的结果路径
    # event['result_paths'] 应该是一个列表:
    # [ "/storage/wordcount_output/count_chunk_0.json",
    #   "/storage/wordcount_output/count_chunk_1.json", ... ]
    result_paths = event.get('result_paths', [])
    
    if not result_paths:
        raise ValueError("WORDCOUNT_MERGE: No result paths to merge.")
        
    print(f"WORDCOUNT_MERGE: Merging {len(result_paths)} partial count files...")

    # 1. 加载所有部分计数字典
    final_dic = defaultdict(int) #
    
    for path in result_paths:
        if not os.path.exists(path):
            print(f"Warning: Result file not found {path}, skipping.")
            continue
            
        with open(path, 'r') as f:
            partial_dic = json.load(f)
        
        # 2. 执行合并逻辑
        for key, value in partial_dic.items():
            final_dic[key] += value #

    print(f"WORDCOUNT_MERGE: Merge complete. Total unique words: {len(final_dic)}")

    # 3. 直接返回最终的字典
    return {
        "final_word_count": final_dic
    }