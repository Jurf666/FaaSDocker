# ----------------------------------------------------------------------
# 1. 基础镜像
#    - 我们使用 3.9-slim (基于 Debian 11 "Bullseye")
#    - 它受支持，并且与 tensorflow 和其他库兼容
#    - 它自带正确的软件源，不再需要修改 sources.list
# ----------------------------------------------------------------------
FROM python:3.9-slim

# ----------------------------------------------------------------------
# 2. 安装系统依赖
# ----------------------------------------------------------------------
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        # Video 工作流需要
        ffmpeg \
        # Recognizer 工作流需要
        tesseract-ocr \
        libgl1 \
        libglib2.0-0 \
        libsm6 \
        libxext6 \
    # 清理
    && rm -rf /var/lib/apt/lists/*

# ----------------------------------------------------------------------
# 3. 复制 FaaS 平台代码
# ----------------------------------------------------------------------
# (这部分与您的代码相同)
RUN mkdir /proxy && \
    mkdir /proxy/exec

COPY proxy.py /proxy/
COPY store.py /proxy/
COPY actions /proxy/exec/actions
COPY models/ /proxy/

# 复制源文件供 Upload/Start Action 读取
COPY sources/my_video.mp4 /proxy/
COPY sources/test.png /proxy/

# 为 WordCount 准备文本目录
COPY sources/book.txt /text/book.txt

# (可选) 复制您的模型文件，如果它们在本地的话
# COPY models/ /proxy/models/

WORKDIR /proxy/exec
EXPOSE 5000

# ----------------------------------------------------------------------
# 4. 安装 Python 依赖
# ----------------------------------------------------------------------
# 复制 requirements.txt (推荐) 或直接安装 (如下)
# 我们只安装 proxy 和 actions 明确需要的包
RUN pip install --no-cache-dir \
    gevent \
    flask \
    redis \
    couchdb \
    googletrans==4.0.0-rc1 \
    tensorflow-cpu \
    opencv-python-headless \
    pytesseract \
    numpy \
    Pillow \
    scipy \
    markdown \
    scikit-learn \
    requests
    

# ----------------------------------------------------------------------
# 5. 启动命令
# ----------------------------------------------------------------------
CMD [ "python3", "/proxy/proxy.py" ]