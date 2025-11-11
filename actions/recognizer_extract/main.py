import cv2
import pytesseract
import os, json
import numpy as np
from PIL import Image

def get_string(img_path):
    img = cv2.imread(img_path)
    # 注意：原始代码缩放了 0.1，这对于OCR可能非常糟糕
    # img = cv2.resize(img, None, fx=0.1, fy=0.1) 
    img = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    kernel = np.ones((1, 1), np.uint8)
    img = cv2.dilate(img, kernel, iterations=1)
    img = cv2.erode(img, kernel, iterations=1)
    
    # 使用 pytesseract
    result = pytesseract.image_to_string(img)
    return result

def main(event):
    image_path = event.get('image_path')
    if not image_path or not os.path.exists(image_path):
        raise FileNotFoundError(f"Valid image_path required: {image_path}")

    text = get_string(image_path)
    
    # 返回提取的文本
    return {"text": text}