import numpy as np
from tensorflow.keras.models import load_model
from tensorflow.keras.preprocessing import image
import os

# 模型路径
model_file_path = '/proxy/resnet50_final_adult.h5'

# --- 关键修改：添加 compile=False ---
# 这样可以忽略优化器参数版本不兼容的问题 ('lr' vs 'learning_rate')
model = load_model(model_file_path, compile=False)
SIZE = (224, 224)

# --- 主逻辑 ---

try:
    image_data = store.fetch(['img'])['img']

    input_filepath = os.path.join(ENV_WORKDIR, 'input.png')
    with open(input_filepath, 'wb') as f:
        f.write(image_data)

    img = image.load_img(input_filepath, target_size=SIZE)
    input_x = image.img_to_array(img)
    input_x = np.expand_dims(input_x, axis=0)

    preds = model.predict(input_x)

    illegal = False
    if preds[0][0] > 0.95:
        illegal = True

    store.post('illegal', illegal)

except Exception as e:
    print(f"[Adult] Error: {e}")
    # 打印完整堆栈方便调试
    import traceback
    traceback.print_exc()
    raise e