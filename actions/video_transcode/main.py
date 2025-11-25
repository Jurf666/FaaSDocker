import subprocess
import logging
import os

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

# --- 主逻辑 ---

evt = store.fetch(['target_type'])
target_type = evt['target_type']
video_data = store.fetch(['video'])['video']

# 写入输入文件
input_filepath = os.path.join(ENV_WORKDIR, 'split_video.mp4')
with open(input_filepath, 'wb') as f:
    f.write(video_data)

# 转码
transcoded_filename = f'transcoded.{target_type}'
transcoded_filepath = os.path.join(ENV_WORKDIR, transcoded_filename)

cmd = f'ffmpeg -y -threads 1 -i {input_filepath} -threads 1 {transcoded_filepath}'
exec_FFmpeg_cmd([cmd])

# 上传结果
with open(transcoded_filepath, 'rb') as f:
    transcoded_data = f.read()

store.post('transcoded_video', transcoded_data, datatype='octet')