import os

# --- 主逻辑 ---

# 扫描本地 /text 目录 (需要在 Dockerfile 里把测试文本 COPY 进去)
# 这里的 /text 需要确保存在
text_dir = '/text'
if os.path.exists(text_dir):
    fn = list(os.listdir(text_dir))
    for fname in fn:
        file_path = os.path.join(text_dir, fname)
        with open(file_path, 'r') as f:
            data = f.read()
        
        # 循环 Post
        store.post('file', data)
else:
    print(f"Warning: {text_dir} does not exist. Posting dummy data.")
    store.post('file', "hello world hello python")
    store.post('file', "hello docker hello faas")