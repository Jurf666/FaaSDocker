# 方案 A 实施总结：工作流子函数 REST 端点

## 🎯 目标
为工作流子函数添加独立的 REST 端点，使其可以像简单函数一样被直接调用，从而支持预热和客户端级别的性能测试。

## 📝 实施范围

### 1. Controller.py - 新增 17 个 REST 端点

在 `/dispatch/<function_name>` 路由之后添加了以下端点：

#### Video Workflow (4 个)
```
POST /dispatch/video_upload      → dispatch('video_upload', payload, is_workflow=True)
POST /dispatch/video_split       → dispatch('video_split', payload, is_workflow=True)
POST /dispatch/video_transcode   → dispatch('video_transcode', payload, is_workflow=True)
POST /dispatch/video_merge       → dispatch('video_merge', payload, is_workflow=True)
```

#### Recognizer Workflow (7 个)
```
POST /dispatch/recognizer_upload      → dispatch('recognizer_upload', payload, is_workflow=True)
POST /dispatch/recognizer_adult       → dispatch('recognizer_adult', payload, is_workflow=True)
POST /dispatch/recognizer_violence    → dispatch('recognizer_violence', payload, is_workflow=True)
POST /dispatch/recognizer_extract     → dispatch('recognizer_extract', payload, is_workflow=True)
POST /dispatch/recognizer_censor      → dispatch('recognizer_censor', payload, is_workflow=True)
POST /dispatch/recognizer_translate   → dispatch('recognizer_translate', payload, is_workflow=True)
POST /dispatch/recognizer_mosaic      → dispatch('recognizer_mosaic', payload, is_workflow=True)
```

#### SVD Workflow (3 个)
```
POST /dispatch/svd_start        → dispatch('svd_start', payload, is_workflow=True)
POST /dispatch/svd_compute      → dispatch('svd_compute', payload, is_workflow=True)
POST /dispatch/svd_merge        → dispatch('svd_merge', payload, is_workflow=True)
```

#### WordCount Workflow (3 个)
```
POST /dispatch/wordcount_start  → dispatch('wordcount_start', payload, is_workflow=True)
POST /dispatch/wordcount_count  → dispatch('wordcount_count', payload, is_workflow=True)
POST /dispatch/wordcount_merge  → dispatch('wordcount_merge', payload, is_workflow=True)
```

### 2. run_experiment_closed_loop.py - 增强预热逻辑

#### 修改 `prepare_workflow_caches()` 函数：
- 利用新增 REST 端点进行预热
- 逐层提取中间结果（如 video_key, chunks, etc.）
- 缓存为后续客户端可复用的 payload
- 完整的错误处理和调试日志

**工作流预热流程示例**：
```
video_upload()
    ↓ output: {video_key, video_name_key, segment_time_key}
video_split({video, video_name, segment_time})
    ↓ output: {splited_video: [chunk_key, ...]}
video_transcode({video: chunk_key, target_type})
    ↓ output: {transcoded_video: [trans_key, ...]}
video_merge({video: merge_list_key, target_type})  ← 缓存作为客户端输入
    ↓ output: {final_video: [result_key, ...]}
```

#### 修改客户端配置逻辑：
```python
client_configs = []
# 简单函数：10 种 × 2 客户端 = 20 个
for func_name, payload in SIMPLE_ACTIONS.items():
    for _ in range(2):
        client_configs.append((func_name, 'simple', payload.copy()))

# 工作流子函数：预热成功的函数 × 2 客户端
# 最多 17 种（如果全部预热成功）
for sub_func, payload in workflow_cached_payloads.items():
    for _ in range(2):
        client_configs.append((sub_func, 'simple', payload_copy))

# 填充到 CPU 数的倍数
padding_needed = (cpu_count - (len(client_configs) % cpu_count)) % cpu_count
for _ in range(padding_needed):
    client_configs.append(("noop", 'simple', {}))
```

