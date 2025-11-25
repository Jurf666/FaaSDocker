import numpy as np
import os
from tensorflow.keras.models import load_model
from tensorflow.keras.preprocessing import image

model_file_path = '/proxy/resnet50_final_violence.h5'

# --- 关键修改：增加 compile=False ---
model = load_model(model_file_path, compile=False)
SIZE = (224, 224)

# --- 主逻辑 ---
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