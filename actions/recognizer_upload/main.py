import os

# --- 主逻辑 ---

# 模拟上传，读取本地（镜像内）的测试图片
# 如果未来改为从 HTTP 请求体获取，可以修改 controller ingest 逻辑
image_path = '/proxy/test.png'

if os.path.exists(image_path):
    with open(image_path, 'rb') as f:
        img_data = f.read()
    
    # 将图片存入数据库，供下游（extract/adult/violence）并行使用
    store.post('img', img_data, datatype='octet')
else:
    print(f"Error: Source image {image_path} not found.")