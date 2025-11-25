import numpy as np

# --- 主逻辑 ---

# 假设这里是生成大矩阵并切分
# 为了演示，我们生成一个随机矩阵切分
# 实际业务可以是读取文件

ROW = 2000
COL = 100
SPLIT = 2 # 切分成几份

# 生成
data = np.random.rand(ROW, COL)

# 切分
splits = np.array_split(data, SPLIT, axis=0)

for chunk in splits:
    # 序列化并发送
    # Store 会自动生成 Key: req_matrix_0, req_matrix_1
    store.post('matrix', chunk.dumps(), datatype='octet')