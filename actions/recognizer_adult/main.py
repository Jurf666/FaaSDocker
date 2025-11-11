import numpy as np
from tensorflow.keras.models import load_model
from tensorflow.keras.preprocessing import image
import os

model_file_path = '/proxy/resnet50_final_adult.h5'
SIZE = (224, 224)
model = None  # <-- 1. 初始化为 None (惰性加载)

def main(event):
    global model  # <-- 2. 声明我们将修改全局变量
    
    # --- 3. 惰性加载模型 ---
    if model is None:
        print("recognizer_adult: Model is None. Loading model for the first time...")
        # 4. 添加 compile=False 来修复 ValueError
        model = load_model(model_file_path, compile=False) 
        print("recognizer_adult: Model loaded successfully.")
    
    image_path = event.get('image_path')
    if not image_path or not os.path.exists(image_path):
        raise FileNotFoundError(f"Valid image_path required: {image_path}")

    img = image.load_img(image_path, target_size=SIZE)
    input_x = image.img_to_array(img)
    input_x = np.expand_dims(input_x, axis=0)
    preds = model.predict(input_x)

    illegal = False
    if preds[0][0] > 0.95:
        illegal = True
    
    return {"illegal": illegal, "confidence": str(preds[0][0])}