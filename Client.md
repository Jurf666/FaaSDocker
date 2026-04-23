# Client 文档

## 1. 项目概述

Client 是一个**闭循环（Closed-Loop）性能评测框架**，用于向 Server 端的 FaaS 控制器发送请求，并精确测量无服务器函数（Serverless Functions）在不同 CPU 资源调度策略下的执行性能。

其核心研究目标是：通过对比 Baseline（大池共享 CPU）与 Experiment（细粒度分组隔离 CPU）两种模式，评估 CPU 亲和性/绑核策略对函数执行延迟、CPU 利用效率和 throttle 损耗的影响。

---

## 2. 架构与目录结构

```
Client/
├── config.py                    # 全局配置
├── run.py                       # 主实验入口（闭循环压测）
├── visualize.py                 # 结果可视化（33张图）
├── baseline_groups.json         # Baseline 分组配置（所有函数同属 group 0）
├── task_groups.json             # Experiment 分组配置（细粒度分组）
├── run_all_experiments.sh       # 多轮自动化实验脚本
├── closed_loop_results/         # 实验结果输出目录
│   ├── *_results.json           # 统计结果
│   ├── *_same_core_overlaps.json  # 同核重叠分析详情
│   ├── *_raw_samples.json       # 原始样本数组
│   └── plots/                   # 可视化图表
└── utils/
    ├── cgroup_manager.py        # cgroup 配置生成与管理
    ├── connections.py           # Redis/CouchDB/Controller 连接与 Warmup
    ├── core_overlap_analyzer.py # 同物理核并发重叠分析
    ├── metrics_calculator.py    # 统计指标计算
    ├── request_handler.py       # 请求分发与指标采集
    └── workflow_utils.py        # 工作流预热与缓存准备
```

---

## 3. 核心模块详解

### 3.1 config.py — 全局配置

定义实验参数与外部服务连接信息，支持通过环境变量覆盖默认值。

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `TEST_DURATION` | `1200` (20分钟) | 单次压测持续时间（秒） |
| `RANDOM_SEED` | `42` | 随机种子 |
| `NUMA_NODE` | `0` | NUMA 节点，决定 CPU 选择范围 |
| `CLIENTS_PER_FUNCTION` | `4` | 每个函数的并发客户端线程数 |
| `TASK_GROUPS_FILE` | `baseline_groups.json` | 当前实验使用的分组文件 |
| `REFERENCE_GROUPS_FILE` | `task_groups.json` | 参考分组文件（用于计算 CPU 全集） |
| `CONTROLLER_HOST/PORT` | `10.2.27.23:5002` | Server Controller 地址 |
| `REDIS_HOST/PORT` | `10.2.27.23:6379` | Redis 地址 |
| `COUCHDB_URL` | `...` | CouchDB 连接 URL |
| `FREQ_STABILIZE_SECS` | `30` | CPU 频率稳定等待时间 |
| `TARGET_CONTAINERS` | `4` | Warmup 目标容器数 |

**支持的函数类型：**

- **计算密集型**：`float_operation`、`matmul`、`linpack`、`k-means`
- **I/O 密集型**：`disk`、`network`、`couchdb_test`
- **数据处理型**：`image`、`markdown2html`、`map_reduce`
- **工作流型**：`video_*`、`recognizer_*`、`svd_*`、`wordcount_*`

### 3.2 run.py — 主实验流程

`run.py` 是闭循环压测的核心入口，执行以下阶段：

```
1. 构建 cgroup 配置与函数映射
2. 初始化 Controller 上的 FunctionManager
3. 等待 Warmup 完成（确保容器就绪）
4. 初始化 Redis/CouchDB，清理旧数据，准备工作流缓存
5. 等待 CPU 频率稳定（轮询 /freq_stable 接口）
6. 启动系统监控（/start_monitor）
7. 启动多线程闭循环压测
8. 停止系统监控（/stop_monitor）
9. 聚合指标，生成统计报告
10. 同核重叠分析
11. 保存结果到 JSON 文件
12. 清理工作流临时数据
```

**闭循环（Closed-Loop）语义：**
每个客户端 worker 在收到上一个请求的响应后，才发送下一个请求。通过 `cycle_time_s` 记录相邻请求的发送间隔，用于分析客户端负载节奏。

**输出文件：**

| 文件 | 说明 |
|------|------|
| `closed_loop_results/{prefix}_results.json` | 汇总统计结果（含 config、summary、statistics） |
| `closed_loop_results/{prefix}_same_core_overlaps.json` | 同核重叠详细分析 |
| `closed_loop_results/{prefix}_raw_samples.json` | 各函数原始样本数组（供过滤分析） |

### 3.3 utils/cgroup_manager.py — cgroup 配置管理

负责根据分组文件生成 CPU 亲和性配置，确保 Baseline 与 Experiment 两组实验使用的**总物理资源完全一致**。

#### 核心逻辑

1. **生成 NUMA CPU 列表**：从 `NUMA_NODE` 开始，按物理核+超线程对生成 `[(0,64), (2,66), ...]`
2. **计算参考组 CPU 分配**：基于 `REFERENCE_GROUPS_FILE`（通常是 experiment 配置）计算所需 CPU 全集
3. **模式判断**：
   - **Experiment 模式**（`current_file == reference_file`）：细粒度分组，每个 group 分配独立的 CPU 子集
   - **Baseline 模式**（`current_file != reference_file`）：将参考组的全部 CPU 统一分配给每个 baseline group

#### CPU 分配算法

```python
cpus_needed = ceil( (len(funcs) * CLIENTS_PER_FUNCTION) / 5.0 )
# 确保为偶数（保持超线程对完整）
if cpus_needed % 2 != 0:
    cpus_needed += 1
```

### 3.4 utils/connections.py — 连接管理与 Warmup

#### init_controller_managers()
遍历所有函数，调用 Controller 的 `/create_manager` 接口，传入函数名和 `cpuset_cpus`，使 Server 在创建容器时直接绑定指定 CPU。

#### wait_for_warmup()
轮询每个函数的 `/manager_status` 接口，确保总容器数达到 `TARGET_CONTAINERS`。
- 若容器不足，调用 `/ensure_warmup` 触发异步补容器（HTTP 202 接收即成功）
- 支持服务端 Warmup 异步化改造：202 状态码表示请求已接收或在途去重，客户端通过轮询判断真正完成

### 3.5 utils/request_handler.py — 请求分发与指标采集

#### dispatch_simple()
向 Controller 发送单个请求，并解析响应中的 `__meta__` 元数据，提取以下指标：

| 指标 | 来源字段 | 说明 |
|------|----------|------|
| `exec_wall_time_s` | `duration` | exec+main 墙钟时间 |
| `effective_cpu_time_s` | `container_cpu_time` 或 `process_cpu_time` | 优先 cgroup CPU，回退进程 CPU |
| `container_cpu_time_s` | `container_cpu_time` | 整个容器/cgroup CPU 时间 |
| `process_cpu_time_s` | `process_cpu_time` | 代理 Python 进程 CPU 时间 |
| `cgroup_nr_periods` | `cgroup_nr_periods_delta` | cgroup CPU 周期数 |
| `cgroup_nr_throttled` | `cgroup_nr_throttled_delta` | throttle 周期数 |
| `cgroup_throttled_time_s` | `cgroup_throttled_time_seconds_delta` | throttle 时间 |
| `cgroup_throttle_ratio` | `cgroup_throttle_ratio_delta` | throttle 比率 |
| `cycle_time_s` | 客户端计算 | 相邻请求发送间隔 |

#### client_worker()
每个 worker 的闭循环逻辑：
```python
while time.monotonic_ns() < end_deadline:
    cycle_time = 当前时间与上次发送时间的差值
    dispatch_simple(...)  # 发请求并采集指标
```

#### 请求分类计数器
- `attempt`：总尝试数
- `success`：成功请求（200 + 有效 duration）
- `http_fail`：HTTP 非 200
- `logic_fail`：HTTP 200 但逻辑错误或无效 duration
- `timeout_fail`：客户端超时
- `exception_fail`：客户端异常

### 3.6 utils/core_overlap_analyzer.py — 同核重叠分析

分析同一物理核上不同函数调用的时间区间重叠情况，用于量化**同核并发干扰**。

#### 核心算法：Sweep-Line（扫描线）

1. **标准化样本**：解析 `physical_cores`（要求恰好 1 个物理核）、时间边界（优先 ns，回退 s）
2. **按核分桶**：不同核之间天然不重叠
3. **逐核排序后扫描**：
   - 维护 `active` 列表（当前仍在执行的调用）
   - 新调用到来时，清理已结束的区间
   - `active` 中剩余即为"启动瞬间已在运行"的请求
   - 两两计算区间交集，累计 overlap_ns

#### 输出结构

```json
{
  "function_level_summary": {
    "float_operation": {
      "invocations": 1000,
      "invocations_with_overlap": 320,
      "overlap_hit_rate": 0.32,
      "avg_peer_request_count": 1.5,
      "top_co_running_functions": [...]
    }
  },
  "core_level_summary": {
    "2": {"invocations": 500, "invocations_with_overlap": 150, "overlap_hit_rate": 0.30}
  },
  "per_invocation": [...]
}
```

### 3.7 utils/metrics_calculator.py — 统计计算

基于 NumPy 计算样本统计量：

| 统计量 | 说明 |
|--------|------|
| `mean` | 均值 |
| `std` | 标准差 |
| `cv` | 变异系数（std/mean） |
| `min/max` | 最小/最大值 |
| `iqr` | 四分位距（P75-P25） |
| `p90/p95/p99` | 百分位数 |

### 3.8 utils/workflow_utils.py — 工作流预热

工作流函数（如 `video_upload` → `video_split` → `video_transcode` → `video_merge`）需要前置步骤产生的中间数据作为输入。

#### prepare_workflow_caches()
在正式压测前，**串行执行一遍完整工作流**，将产生的中间数据 key 保存到 Redis/CouchDB，供后续压测循环复用。

支持 4 种工作流：
- **Video**：upload → split → transcode → merge
- **Recognizer**：upload → parallel(adult, violence, extract) → censor/translate/mosaic
- **SVD**：start → compute → merge
- **WordCount**：start → count → merge

#### 统一存储策略
- **小数据**（< 32KB）：直接存 Redis
- **大数据**（≥ 32KB）：CouchDB 存附件 + Redis 存 `COUCH_REF:` 指针

#### cleanup_workflow_data()
实验结束后清理 Redis 和 CouchDB 中的工作流中间数据，避免跨实验污染。

### 3.9 visualize.py — 结果可视化

基于 Matplotlib 为每轮实验生成 **33 张图**：

| 图表类型 | 数量 | 说明 |
|----------|------|------|
| 每函数双视图对比 | ~27 张 | 2×2 网格：Baseline(all/filtered) vs Experiment(all/filtered) |
| Overview | 4 张 | Baseline/Experiment × all/filtered |
| Comparison | 2 张 | Baseline vs Experiment 的 KDE 密度对比 |

每张图包含：
- 直方图 + 高斯核密度估计（KDE）
- 均值（红虚线）、P95（橙实线）、P99（紫实线）标记

---

## 4. 分组配置机制

### 4.1 baseline_groups.json

所有函数映射到同一个 group（`0`），表示**不区分函数类型，共享同一大池 CPU**。

```json
{
  "float_operation": 0,
  "matmul": 0,
  "disk": 0,
  ...
}
```

### 4.2 task_groups.json

函数按类型/特征映射到不同 group，表示**细粒度隔离**，每个 group 拥有独立的 CPU 子集。

```json
{
  "recognizer_translate": 0,
  "map_reduce": 1,
  "float_operation": 2,
  ...
}
```

### 4.3 资源公平性保证

`cgroup_manager.py` 的关键设计：**Baseline 组分配到的 CPU 总数 = Experiment 组所有 group 的 CPU 总和**。这确保了两组对比实验在总物理资源消耗上完全一致，避免"实验组用了更多核所以表现更好"的偏差。

---

## 5. 运行方式

### 5.1 单轮实验

```bash
# 激活虚拟环境
source .venv/bin/activate

# Baseline 模式
TASK_GROUPS_FILE=baseline_groups.json python run.py

# Experiment 模式
TASK_GROUPS_FILE=task_groups.json python run.py

# 可视化
python visualize.py
```

### 5.2 多轮自动化实验

```bash
# 运行 10 轮（每轮包含 baseline + experiment）
bash ./run_all_experiments.sh 10
```

脚本会自动：
1. 轮询服务器 `/experiment_ready` 等待就绪
2. 执行 `run.py`
3. 保存并校验结果文件
4. 通知服务器 `/client_done`
5. 每轮结束后自动运行 `visualize.py`
6. 全部轮次完成后生成汇总 CSV

### 5.3 中断实验

```bash
sudo pkill -f "run_all_experiments.sh"
sudo pkill -f "run.py"
```

---

## 6. 输出文件说明

### 6.1 {prefix}_results.json

```json
{
  "config": { "test_duration": 1200, "num_clients": 108, ... },
  "metric_schema": { "statistics": {...}, "execution_samples": {...}, "counters": {...} },
  "summary": {
    "total_time": 1205.3,
    "attempted_requests": 5420,
    "successful_requests": 5380,
    "failure_rate": 0.0074,
    "overall_metric_means": { "effective_cpu_time_s_mean": 0.45, ... }
  },
  "statistics": {
    "float_operation": {
      "count": 120, "mean": 0.523, "std": 0.032, "cv": 0.061,
      "p90": 0.58, "p95": 0.61, "p99": 0.67,
      "attempts": 120, "success": 118, "failure_rate": 0.0167,
      "effective_cpu_time_s_mean": 0.48,
      "container_cpu_time_s_mean": 0.50,
      "process_cpu_time_s_mean": 0.46,
      "exec_wall_time_stats": { ... }
    }
  },
  "same_core_function_summary": { ... },
  "same_core_core_summary": { ... }
}
```

### 6.2 {prefix}_same_core_overlaps.json

包含每次调用的详细重叠信息，以及按函数/核的聚合统计。

### 6.3 {prefix}_raw_samples.json

保留各函数的原始样本数组，供后续过滤分析使用：

```json
{
  "float_operation": {
    "exec_wall_time_s": [0.51, 0.53, 0.49, ...],
    "effective_cpu_time_s": [0.48, 0.50, 0.47, ...],
    ...
  }
}
```

---

## 7. 关键设计要点

1. **闭循环而非开循环**：请求发送速率由服务端处理能力自然限制，而非客户端固定速率注入，更贴近真实用户行为
2. **频率稳定等待**：实验前轮询 `/freq_stable`，排除 CPU 频率波动对延迟的干扰
3. **双层 CPU 指标**：同时采集 `container_cpu_time`（含子进程）和 `process_cpu_time`（仅代理进程），优先使用前者
4. **请求级绑核重叠分析**：利用 `__meta__` 中返回的 `start_ns/end_ns/physical_cores` 精确计算同核并发干扰
5. **工作流预热**：避免冷启动和首轮数据准备开销，确保压测期间工作流请求可直接复用缓存数据
