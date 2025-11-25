import numpy as np
import pickle

# --- 主逻辑 ---

# 获取单个分块 (二进制 pickle流)
m_data = store.fetch(['matrix'])['matrix']
m = pickle.loads(m_data)

# SVD 计算
U, Sigma, VT = np.linalg.svd(m)

# 简单的计算逻辑演示：Sigma * VT
# 这里需要注意 output 的大小
res = np.dot(np.diag(Sigma), VT)

store.post('res', res.dumps(), datatype='octet')