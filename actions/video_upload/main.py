import os

# --- 主逻辑 ---

# 模拟上传：从镜像内的固定路径读取视频文件
# 在实际场景中，这里可能是从外部 URL 下载，或者处理 HTTP 请求体中的文件流
video_source_path = '/proxy/my_video.mp4'  # 请确保 Dockerfile 里 COPY 了这个文件到 /proxy
video_name = 'my_video.mp4'

if os.path.exists(video_source_path):
    with open(video_source_path, 'rb') as f:
        video_data = f.read()
    
    # 1. 上传视频数据 (二进制流)
    # Store 会根据大小自动决定是存 Redis 还是 CouchDB
    store.post('video', video_data, datatype='octet')
    
    # 2. 上传元数据
    store.post('video_name', video_name)
    store.post('segment_time', 10) # 默认切片时间，也可以由 Controller 传入覆盖
else:
    print(f"Error: Video source {video_source_path} not found.")