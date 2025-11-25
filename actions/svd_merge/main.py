import numpy as np
import pickle

# --- 主逻辑 ---

# Fetch 得到的是列表
res_list_data = store.fetch(['res'])['res']

# 反序列化
matrices = [pickle.loads(data) for data in res_list_data]

# 合并结果 (这里演示简单的纵向堆叠，具体视算法而定)
# 假设是恢复 U 或其他
if matrices:
    final_res = np.concatenate(matrices, axis=0)
    store.post('final_res', final_res.dumps(), datatype='octet')
else:
    store.post('final_res', b'empty')