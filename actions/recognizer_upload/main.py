import os
import json

STORAGE_DIR = '/storage'

def main(event):
    image_filename = event.get('image_filename') # e.g., "test.png"
    if not image_filename:
        raise Exception("image_filename is required")
    
    # 假设源文件已被 trigger_workflow.py 复制到 /storage/source/
    image_path = os.path.join(STORAGE_DIR, 'sources', image_filename)
    
    if not os.path.exists(image_path):
        raise FileNotFoundError(f"File not found at {image_path}")

    # 返回 Action 可以使用的 *容器内共享路径*
    return {"image_path": image_path}