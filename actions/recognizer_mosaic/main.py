import cv2
import os

# --- 主逻辑 ---

image_data = store.fetch(['img'])['img']

input_filepath = os.path.join(ENV_WORKDIR, 'input.png')
with open(input_filepath, 'wb') as f:
    f.write(image_data)

img = cv2.imread(input_filepath, 1)
# 调整大小，避免处理过慢
img = cv2.resize(img, None, fx=0.1, fy=0.1)

height, width, deep = img.shape
mosaic_height = 8

# 马赛克处理
for m in range(0, height - mosaic_height, mosaic_height):
    for n in range(0, width - mosaic_height, mosaic_height):
        b, g, r = img[m, n]
        for i in range(mosaic_height):
            for j in range(mosaic_height):
                img[m + i, n + j] = (b, g, r)

mosaic_filename = 'output.jpg'
mosaic_filepath = os.path.join(ENV_WORKDIR, mosaic_filename)
cv2.imwrite(mosaic_filepath, img)

# 上传结果 (虽然业务逻辑里可能不关注返回内容，但存下来做审计)
with open(mosaic_filepath, 'rb') as f:
    mosaic_data = f.read()

store.post('mosaic_image', mosaic_data, datatype='octet')
store.post('content', 'ok')