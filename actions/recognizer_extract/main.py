import cv2
import pytesseract
import os
import numpy as np
from PIL import Image

def get_string(img_path):
    img = cv2.imread(img_path)
    img = cv2.resize(img, None, fx=0.1, fy=0.1)
    img = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    
    kernel = np.ones((1, 1), np.uint8)
    img = cv2.dilate(img, kernel, iterations=1)
    img = cv2.erode(img, kernel, iterations=1)

    temp_path = os.path.join(ENV_WORKDIR, 'thres.png')
    cv2.imwrite(temp_path, img)
    
    result = pytesseract.image_to_string(Image.open(temp_path))
    return result

# --- 主逻辑 ---

image_data = store.fetch(['img'])['img']

input_filepath = os.path.join(ENV_WORKDIR, 'input.png')
with open(input_filepath, 'wb') as f:
    f.write(image_data)

text = get_string(input_filepath)

store.post('text', text)