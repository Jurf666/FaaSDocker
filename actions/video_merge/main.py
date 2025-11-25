import subprocess
import os

# --- 主逻辑 ---

# 1. 获取输入 (因为 Store 支持 LIST_REF，这里拿到的直接是二进制数据的列表)
video_list = store.fetch(['video'])['video']
target_type = store.fetch(['target_type'])['target_type']

# 2. 将所有片段写入临时目录
list_filepath = os.path.join(ENV_WORKDIR, 'files.txt')
with open(list_filepath, 'w') as list_file:
    for i, video_data in enumerate(video_list):
        segment_name = f'seg_{i}.{target_type}'
        segment_path = os.path.join(ENV_WORKDIR, segment_name)
        with open(segment_path, 'wb') as f:
            f.write(video_data)
        # FFmpeg concat file list format: file 'path'
        list_file.write(f"file '{segment_path}'\n")

# 3. 合并
output_filename = f'final_video.{target_type}'
output_filepath = os.path.join(ENV_WORKDIR, output_filename)

# 使用 concat demuxer
cmd = f"ffmpeg -f concat -safe 0 -i {list_filepath} -c copy {output_filepath}"
subprocess.run(cmd, shell=True, check=True)

# 4. 上传最终结果
with open(output_filepath, 'rb') as f:
    final_data = f.read()

store.post('final_video', final_data, datatype='octet')