import subprocess
import logging
import os
import math

LOGGER = logging.getLogger()

class FFmpegError(Exception):
    def __init__(self, message, status):
        super().__init__(message, status)
        self.message = message
        self.status = status

def exec_FFmpeg_cmd(cmd_lst):
    try:
        subprocess.run(
            cmd_lst, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, shell=True)
    except subprocess.CalledProcessError as exc:
        LOGGER.error(f'FFmpeg Error: {exc.output}')
        raise FFmpegError(exc.output, exc.returncode)

def getVideoDuration(input_video):
    cmd = f'ffprobe -i {input_video} -show_entries format=duration -v quiet -of csv="p=0"'
    raw_result = subprocess.check_output(cmd, shell=True)
    return float(raw_result.decode().replace("\n", "").strip())

def get_fileNameExt(filename):
    (shortname, extension) = os.path.splitext(filename)
    return shortname, extension

# --- 主逻辑 ---

# 1. 获取输入
evt = store.fetch(['video_name', 'segment_time'])
video_name = evt['video_name']
segment_time = int(evt['segment_time'])

video_data = store.fetch(['video'])['video']

# 2. 写入临时文件
input_filepath = os.path.join(ENV_WORKDIR, video_name)
with open(input_filepath, 'wb') as f:
    f.write(video_data)

# 准备输出目录
video_proc_dir = os.path.join(ENV_WORKDIR, 'split_output')
os.makedirs(video_proc_dir, exist_ok=True)

# 3. 计算切片并执行 FFmpeg
video_duration = getVideoDuration(input_filepath)
MAX_SPLIT_NUM = 4
split_num = math.ceil(video_duration / segment_time)
if split_num > MAX_SPLIT_NUM:
    segment_time = int(math.ceil(video_duration / MAX_SPLIT_NUM)) + 1

shortname, extension = get_fileNameExt(video_name)
output_pattern = os.path.join(video_proc_dir, f"split_{shortname}_piece_%02d{extension}")

command = f'ffmpeg -i {input_filepath} -c copy -f segment -segment_time {segment_time} -reset_timestamps 1 {output_pattern}'
exec_FFmpeg_cmd([command])

# 4. 上传结果
split_keys = []
for filename in sorted(os.listdir(video_proc_dir)):
    if filename.startswith(f'split_{shortname}'):
        file_path = os.path.join(video_proc_dir, filename)
        with open(file_path, 'rb') as f:
            chunk_data = f.read()
        
        # 循环 Post，Store 会自动处理 Key (如 req_splited_video_0, req_splited_video_1...)
        store.post('splited_video', chunk_data, datatype='octet')
        split_keys.append(filename)

# 提交文件名列表作为元数据 (可选，方便调试)
store.post('split_keys', split_keys)