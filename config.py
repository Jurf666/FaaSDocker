# config.py
import os

# 实验基础配置
TEST_DURATION = int(os.environ.get('TEST_DURATION', '600'))
RANDOM_SEED = int(os.environ.get('RANDOM_SEED', '42'))
NUMA_NODE = int(os.environ.get('NUMA_NODE', '0'))
CLIENTS_PER_FUNCTION = int(os.environ.get('CLIENTS_PER_FUNCTION', '4'))

# 文件路径
TASK_GROUPS_FILE = os.environ.get('TASK_GROUPS_FILE', 'baseline_groups.json')
REFERENCE_GROUPS_FILE = 'task_groups.json'

# 服务端地址
CONTROLLER_HOST = os.environ.get('CONTROLLER_HOST', '10.2.27.23')
CONTROLLER_PORT = os.environ.get('CONTROLLER_PORT', '5002')
CONTROLLER_URL = f"http://{CONTROLLER_HOST}:{CONTROLLER_PORT}"

# Redis配置
REDIS_HOST = os.environ.get('REDIS_HOST', '10.2.27.23')
REDIS_PORT = int(os.environ.get('REDIS_PORT', '6379'))

# CouchDB配置
COUCHDB_URL = os.environ.get('COUCHDB_URL', 'http://openwhisk:openwhisk@10.2.27.23:5984/')

# 简单函数参数（抽离常量）
SIMPLE_ACTIONS = {
    "float_operation": {"param": 500000},
    "matmul": {"param": 1000},
    "linpack": {"param": 1000},
    "k-means": {},
    "image": {},
    "network": {"name": "10mb"},
    "markdown2html": {},
    "map_reduce": {},
    "disk": {"bs": "1M", "count": 100},
    "couchdb_test": {},
}

# 工作流相关常量（可扩展）
WORKFLOW_CACHE_PATTERNS = [
    'req-*', 'warmup-*', 'sys-*', 'const_target_*',
    '*video*', '*recognizer*', '*svd*', '*wordcount*',
    '*split*', '*transcode*', '*merge*', '*upload*',
    '*adult*', '*violence*', '*extract*', '*censor*',
    '*translate*', '*mosaic*', '*compute*', '*count*'
]
TARGET_CONTAINERS = 4