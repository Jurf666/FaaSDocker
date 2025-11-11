import json
import os

STORAGE_DIR = '/storage'

def main(event):
    # 从 controller 接收参数
    input_filename = event.get('input_filename') # e.g., "test.txt"
    slice_num = int(event.get('slice_num', 4))
    
    if not input_filename:
        raise ValueError("input_filename is required")

    # 1. 定义输入和输出路径
    input_filepath = os.path.join(STORAGE_DIR, 'sources', input_filename)
    output_dir = os.path.join(STORAGE_DIR, 'wordcount_input')
    os.makedirs(output_dir, exist_ok=True)
    
    if not os.path.exists(input_filepath):
        raise FileNotFoundError(f"WORDCOUNT_START: Input file not found at {input_filepath}")
        
    print(f"WORDCOUNT_START: Reading {input_filepath} and splitting into {slice_num} slices.")
    
    # 2. 读取和分割文件
    try:
        with open(input_filepath, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        # 尝试用另一种编码
        with open(input_filepath, 'r', encoding='latin-1') as f:
            content = f.read()

    split_size = len(content) // slice_num
    text_list = []
    for i in range(slice_num):
        if i == slice_num - 1:
            text_list.append(content[i * split_size:])
        else:
            text_list.append(content[i * split_size: (i + 1) * split_size])
    
    # 3. 将切片保存到 /storage
    chunk_paths = []
    for i, text_chunk in enumerate(text_list):
        chunk_filename = f'chunk_{i}.txt'
        chunk_filepath = os.path.join(output_dir, chunk_filename)
        
        with open(chunk_filepath, 'w', encoding='utf-8') as f:
            f.write(text_chunk)
            
        chunk_paths.append(chunk_filepath)
        print(f"WORDCOUNT_START: Saved chunk {i} to {chunk_filepath}")

    # 4. 返回包含所有切片*路径*的列表
    return {
        "chunk_paths": chunk_paths,
        "chunk_num": len(chunk_paths)
    }