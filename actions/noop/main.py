import time

def main(args):
    # 1. 接收参数但不处理
    # 这很重要！因为 json 解析的开销是框架噪音的一部分。
    # 如果真实任务接收一个 10MB 的图片路径，空任务也要接收它（虽然不读文件）。
    
    # 2. 极简的计时（可选，仅用于完整性）
    start = time.time()
    
    # 3. 什么都不做 (Do nothing)
    pass 
    
    # 4. 返回一个最小的空结果
    # 保持返回格式的一致性
    return {"status": "ok", "latency": time.time() - start}