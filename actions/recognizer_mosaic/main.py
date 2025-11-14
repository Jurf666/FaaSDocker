import cv2
import os
import json

STORAGE_DIR = '/storage'

def main(event):
    image_path = event.get('image_path')
    if not image_path or not os.path.exists(image_path):
        raise FileNotFoundError(f"Valid image_path required: {image_path}")

    # 原始代码 应用了全屏马赛克。我们将保持这个逻辑。
    img = cv2.imread(image_path, 1)
    
    # 注意：原始代码缩放了 0.1。我们保留它。
    img = cv2.resize(img, None, fx=0.1, fy=0.1)
    
    height, width, deep = img.shape
    mosaic_height = 8
    for m in range(height - mosaic_height):
        for n in range(width - mosaic_height):
            if m % mosaic_height == 0 and n % mosaic_height == 0:
                for i in range(mosaic_height):
                    for j in range(mosaic_height):
                        b, g, r = img[m, n]
                        img[m + i, n + j] = (b, g, r)

    # 将处理后的文件写入 *共享存储*
    # 我们需要从原始文件名派生出一个新文件名
    base_name = os.path.basename(image_path)
    name, ext = os.path.splitext(base_name)
    
    mosaic_filename = f"{name}_mosaic.jpg"
    
    # --- 关键：使用您指定的 'output/recognizer_mosaic' 目录 ---
    output_dir = os.path.join(STORAGE_DIR, 'output', 'recognizer_mosaic')
    os.makedirs(output_dir, exist_ok=True)
    
    mosaic_filepath = os.path.join(output_dir, mosaic_filename)
    
    cv2.imwrite(mosaic_filepath, img)

    # 返回新文件的 *容器内路径*
    return {"mosaic_image_path": mosaic_filepath}