# Server 文档

## 1. 项目概述

Server 是一个基于 **Docker** 的轻量级 **FaaS（Function-as-a-Service）运行时与控制器**，负责管理无服务器函数容器的全生命周期（创建、调度、复用、回收），并支持多种 CPU 亲和性/绑核策略的实验对比。

它是整个实验平台的**后端核心**，与 Client 配合完成多轮自动化性能评测。Server 提供 HTTP API 供 Client 控制函数管理器的初始化、Warmup、请求分发和系统监控，同时在请求处理过程中精确采集 CPU 时间、throttle 等底层指标并返回给 Client。

---

## 2. 架构与目录结构

```
Server/
├── controller.py                # Flask HTTP API 网关（主入口）
├── function_manager.py          # 函数容器生命周期管理器
├── Dockerfile                   # 容器镜像构建配置
├── README.md                    # 快速运行说明
├── pre_experiment.sh            # 单轮实验前环境准备
├── run_all_experiments.sh       # 多轮自动化实验脚本
├── plot_server_metrics.py       # server_metrics.csv 可视化
├── modulesOfController/
│   ├── dispatcher.py            # 请求分发中枢
│   ├── workflow_engine.py       # 工作流引擎
│   ├── definitions.py           # 工作流定义（video/recognizer/svd/wordcount）
│   ├── data_store.py            # Redis/CouchDB 连接与任务状态存储
│   └── monitor.py               # 系统监控（CPU 利用率/频率/温度）
├── models/                      # 预训练模型文件（如 ResNet50）
├── actions/                     # 各函数的业务逻辑实现
│   ├── float_operation/         # 浮点运算
│   ├── matmul/                  # 矩阵乘法
│   ├── linpack/                 # Linpack 基准测试
│   ├── k-means/                 # K-Means 聚类
│   ├── image/                   # 图像处理
│   ├── network/                 # 网络 I/O
│   ├── disk/                    # 磁盘 I/O
│   ├── couchdb_test/            # CouchDB 操作
│   ├── markdown2html/           # Markdown 转 HTML
│   ├── map_reduce/              # MapReduce
│   ├── video_*/                 # 视频处理工作流子函数
│   ├── recognizer_*/            # 图像识别工作流子函数
│   ├── svd_*/                   # SVD 工作流子函数
│   └── wordcount_*/             # WordCount 工作流子函数
└── sources/                     # 源代码/资源文件
```

---

## 3. 核心模块详解

### 3.1 controller.py — HTTP API 网关

基于 **Flask + gevent** 的高并发 HTTP 服务，监听端口 `5002`，是 Client 与 Server 交互的唯一入口。

#### 主要路由

| 路由 | 方法 | 说明 |
|------|------|------|
| `/create_manager` | POST | 创建/初始化指定函数的 FunctionManager，传入 `cpuset_cpus` |
| `/dispatch/<function_name>` | POST | 同步分发单个函数请求 |
| `/dispatch_workflow` | POST | 异步提交工作流任务，返回 `task_id` |
| `/check_task/<task_id>` | GET | 查询工作流任务状态（running/completed/failed） |
| `/start_monitor` | POST | 启动系统监控（CPU 利用率/频率/温度采集） |
| `/stop_monitor` | POST | 停止系统监控 |
| `/manager_status/<function_name>` | GET | 查询函数容器池状态（total/idle/busy/ports） |
| `/ensure_warmup/<function_name>` | POST | 异步触发补容器，目标总数 `target_total_containers` |
| `/freq_stable` | GET | 查询 CPU 频率是否稳定（供 Client 实验前等待） |
| `/set_ready` | POST | 标记当前轮次就绪（多轮实验同步） |
| `/experiment_ready` | GET | Client 轮询：查询服务器是否就绪 |
| `/client_done` | POST | Client 通知：本轮实验完成 |
| `/wait_client_done` | GET | Server 轮询：查询 Client 是否已完成 |

#### Warmup 异步化改造

`/ensure_warmup` 接口经过改造：
1. **立即返回 202**：不再同步阻塞等待补容器完成，避免客户端 Read timeout
2. **同函数在途去重**：若某函数已有 queued/running 的 warmup 任务，复用该任务而非新建
3. **后台线程执行**：`_run_warmup_job` 在锁外启动，调用 `manager.ensure_min_total_containers()` 补齐容器

#### CPU 频率稳定检测（`/freq_stable`）

稳定条件：
- 所有在线 CPU 的当前频率与最大频率之比 ≥ 0.95
- 连续两次采样（间隔 1s）的频率变化 < 100 MHz

Client 在实验前通过轮询此接口，确保 CPU 频率已稳定在最高性能状态。

#### 多轮实验同步状态机

```
not_ready  →  ready（Server 调用 /set_ready）
    ↑                          ↓
client_done ← Client 调用 /client_done
```

---

### 3.2 function_manager.py — 容器生命周期管理

`FunctionManager` 是容器池化管理的核心类，每个函数对应一个实例。

#### 核心属性

| 属性 | 说明 |
|------|------|
| `function_name` | 函数名 |
| `image_name` | Docker 镜像名（统一使用 `jywang_test`） |
| `container_port` | 容器内服务端口（5000） |
| `cpuset_cpus` | 容器默认绑定的 CPU 集合 |
| `containers` | 容器池字典：`{container_id: {container_obj, status, last_active, host_port, fixed_cpuset}}` |
| `idle_timeout` | 空闲容器超时回收时间（默认 300s） |
| `min_idle_containers` | 最小空闲容器数（Keeper 补齐目标） |

#### 容器状态流转

```
_create_new_container() → status: idle
get_container_for_request() → status: busy
release_container() → status: idle
_run_cleaner() (超时+冗余) → 物理删除容器
```

#### 创建容器流程

1. 使用 `docker_client.containers.run()` 创建容器
2. 默认参数：`cpu_period=100000`, `cpu_quota=50000`（即 0.5 CPU）
3. 等待 Docker 端口映射生效（轮询最多 30s）
4. 轮询容器 `/status` 接口确认服务就绪（最多 30s）
5. 任一检查失败则清理容器并返回 `None`

#### Keeper / Cleaner 后台线程

`_run_cleaner()` 每 5 秒调度一次：
- **Keeper**：检查 idle 容器数是否低于 `min_idle_containers`，不足则创建补齐
- **Cleaner**：每 30 秒执行一次，回收同时满足以下条件的 idle 容器：
  - 空闲时间 > `idle_timeout`（300s）
  - 回收后剩余 idle 容器数 ≥ `min_idle_containers`（非必要不回收）

#### Warmup 增强

`ensure_min_total_containers(target_total)`：
- 仅补齐缺口，不做删除
- 统计当前 running 容器数，按需创建
- 返回创建统计（target/current/created/final）

---

### 3.3 modulesOfController/dispatcher.py — 请求分发中枢

`Dispatcher` 负责将 Client 的请求路由到正确的 FunctionManager，并在请求前后处理 CPU 亲和性。

#### dispatch_sync() 核心流程

```
1. 获取或创建 FunctionManager
2. 从容器池获取可用容器（优先复用 idle，否则冷启动）
3. 应用请求级 CPU 亲和性（见第 5 节）
4. 向容器 /init 发送 action 名称（预初始化）
5. 向容器 /run 发送实际请求（超时 2000s）
6. 解析响应中的 func_duration、cpu_time、cgroup 指标
7. 在 __meta__ 中填充完整运行时元数据
8. 释放容器（标记为 idle）和 affinity lease
9. 返回结果
```

#### 元数据填充

`__meta__` 中包含以下字段供 Client 分析：

| 字段 | 说明 |
|------|------|
| `request_id` | 本次请求唯一 ID |
| `container_id` | 执行容器 ID |
| `duration` | exec+main 墙钟时间（秒） |
| `cpuset` | 本次请求绑定的 CPU 集合 |
| `physical_cores` | 解析后的物理核列表 |
| `func_main_start_ns/end_ns` | 函数执行开始/结束时间戳 |
| `func_duration_ns` | 函数执行时长（纳秒） |
| `process_cpu_time` / `process_cpu_time_ns` | 代理进程 CPU 时间 |
| `container_cpu_time` / `container_cpu_time_ns` | 容器 cgroup CPU 时间 |
| `cgroup_nr_periods_delta` | cgroup CPU 周期数变化 |
| `cgroup_nr_throttled_delta` | throttle 周期数变化 |
| `cgroup_throttled_time_ns_delta` | throttle 时间变化（纳秒） |
| `cgroup_throttle_ratio_delta` | throttle 比率变化 |

---

### 3.4 modulesOfController/workflow_engine.py — 工作流引擎

支持 4 种复合工作流的异步执行：

| 工作流 | 说明 |
|--------|------|
| `video` | 视频上传 → 分割 → 并行转码 → 合并 |
| `recognizer` | 图片上传 → 并行检测（成人/暴力/提取）→ 文本审查/翻译/马赛克 |
| `svd` | 矩阵生成 → 并行计算 → 合并 |
| `wordcount` | 文件生成 → 并行计数 → 合并 |

工作流通过 `ThreadPoolExecutor` 实现内部子任务的并行执行（如多个视频片段的并行转码）。

---

### 3.5 modulesOfController/definitions.py — 工作流定义

定义各工作流的具体执行步骤和数据流转逻辑。

#### 数据传递机制

工作流子函数之间通过 **Redis key** 传递数据：
- 上游函数输出 `{"video": ["key1"], ...}`
- 下游函数通过 Redis `get(key1)` 获取实际数据
- 支持 `LIST_REF:` 和 `COUCH_REF:` 两种引用格式

#### call_action() 辅助函数

标准化调用 `dispatcher.dispatch_sync()`，并兼容新旧元数据结构：
- 新结构：`res['__meta__']['container_id']`
- 旧结构：`res['container_id']`

---

### 3.6 modulesOfController/data_store.py — 数据存储

管理 Redis 和 CouchDB 连接，以及工作流任务状态存储。

#### 数据库连接

| 数据库 | 用途 |
|--------|------|
| Redis | 小数据缓存、LIST_REF 指针、工作流中间数据传递 |
| CouchDB (`faas_data`) | 大数据存储（>32KB 的 JSON 附件） |

#### save_result()

将工作流最终结果持久化到 `./results/` 目录：
- 若 Redis 值为 `COUCH_REF:doc_id`，从 CouchDB 读取附件
- 否则直接写入 JSON 文本

---

### 3.7 modulesOfController/monitor.py — 系统监控

`SystemMonitor` 以 **1Hz** 频率采集系统指标并写入 CSV。

#### 采集指标

| 指标 | 来源 |
|------|------|
| 每核 CPU 利用率 | `psutil.cpu_percent(percpu=True)` |
| 每核 CPU 频率 | `/sys/devices/system/cpu/cpu*/cpufreq/scaling_cur_freq` |
| 最高 CPU 温度 | `/sys/class/thermal/thermal_zone*/temp` |
| 平均频率 | 所有在线核频率的均值 |

#### CSV 格式

```
timestamp,CPU_0_util%(group_0),CPU_0_freq_MHz(group_0),CPU_2_util%(group_1),...
```

列顺序与 Client 传入的 `cgroup_configs` 分组顺序一致，便于后续按 group 分析。

---

## 4. models/ 与 actions/ 目录

### 4.1 models/

存放预训练深度学习模型，当前包含：
- `resnet50_final_adult.h5` — 成人内容识别模型
- `resnet50_final_violence.h5` — 暴力内容识别模型

### 4.2 actions/

每个子目录对应一个独立的函数实现，内部通常包含：
- `Dockerfile` 或统一使用根目录 `Dockerfile` 构建的镜像
- `main.py` / `server.py` — 函数服务入口（暴露 `/status`、`/init`、`/run`）
- 业务逻辑代码

**函数分类：**

| 类型 | 函数 |
|------|------|
| 计算密集型 | `float_operation`、`matmul`、`linpack`、`k-means` |
| I/O 密集型 | `disk`、`network`、`couchdb_test` |
| 数据处理型 | `image`、`markdown2html`、`map_reduce` |
| Video 工作流 | `video_upload`、`video_split`、`video_transcode`、`video_merge` |
| Recognizer 工作流 | `recognizer_upload`、`recognizer_adult`、`recognizer_violence`、`recognizer_extract`、`recognizer_censor`、`recognizer_translate`、`recognizer_mosaic` |
| SVD 工作流 | `svd_start`、`svd_compute`、`svd_merge` |
| WordCount 工作流 | `wordcount_start`、`wordcount_count`、`wordcount_merge` |

---

## 5. CPU 亲和性策略详解

项目的 CPU 亲和性策略分为**两种模式**，分别对应两类实验。通过修改 `function_manager.py` 和 `dispatcher.py` 中的代码注释来切换，不会在运行时动态选择。

### 5.1 物理核与逻辑核映射

系统使用 **SMT（超线程）**，映射规则：
- 物理核 `N` 对应逻辑核 `N` 和 `N+64`
- `cpu < 64` 表示物理核，`cpu >= 64` 表示超线程逻辑核

---

### 5.2 模式一：1*2 实验模式（严格隔离模式）

**启用方式**：
- `function_manager.py`：启用 `_choose_container_create_cpuset()`，注释掉 `self.cpuset_cpus`
- `function_manager.py`：`cpu_quota` 设为 `100000`
- `dispatcher.py`：启用 `manager.apply_request_affinity()`，注释掉 `manager.apply_request_affinity_free()`
- `config.py`：`CLIENTS_PER_FUNCTION=2`，`TARGET_CONTAINERS=2`

**核心特征**：

| 维度 | 配置 | 效果 |
|------|------|------|
| 单容器配额 | `quota=100000`（1 CPU） | 每个容器拥有完整逻辑核的 CPU 配额 |
| 容器创建绑核 | `_choose_container_create_cpuset()` | 容器创建时即固定到单个逻辑核（如 cpuset 为 `0,64` 时，容器 A 绑 `0`，容器 B 绑 `64`） |
| 请求级绑核 | `apply_request_affinity()` | Baseline 大池模式下，请求来时通过 lease 机制申请**空闲逻辑核**，同一逻辑核同一时刻只服务一个请求 |
| 并发度 | 2 clients / 函数 | 形成"2 容器 ↔ 2 逻辑核（1 物理核）"的严格映射 |

**研究目标**：在 SMT 环境下，对比"同函数的 2 个请求被强制绑定到同一个物理核的 2 个超线程上"（Experiment 组，同构竞争）与"不同函数的请求在大池中随机配对共核"（Baseline 组，异构竞争）的性能差异。

---

### 5.3 模式二：其他实验模式（自由共享模式，如 0.5*4）

**启用方式**：
- `function_manager.py`：启用 `self.cpuset_cpus`，注释掉 `_choose_container_create_cpuset()`
- `function_manager.py`：`cpu_quota` 设为 `50000`
- `dispatcher.py`：启用 `manager.apply_request_affinity_free()`，注释掉 `manager.apply_request_affinity()`
- `config.py`：`CLIENTS_PER_FUNCTION=4`（或其他），`TARGET_CONTAINERS=4`

**核心特征**：

| 维度 | 配置 | 效果 |
|------|------|------|
| 单容器配额 | `quota=50000`（0.5 CPU） | 每个容器拥有半个逻辑核的 CPU 配额 |
| 容器创建绑核 | `self.cpuset_cpus`（整个 group 的 cpuset） | 容器创建时绑定到 group 的整个 CPU 池，不做细粒度拆分 |
| 请求级绑核 | `apply_request_affinity_free()` | 不检查逻辑核是否已被占用，只限制**每个物理核上 busy 容器的密度**（`max_containers_per_core=4`） |
| 并发度 | 4 clients / 函数（或其他） | 多个请求共享核池，由调度器动态切片 |

#### `apply_request_affinity_free()` 的核心逻辑

1. **候选核筛选**：从 `_physical_candidates`（`cpu < 64` 的物理核）中筛选
2. **负载感知**：统计当前 `busy` 容器在各物理核上的分布
3. **容量限制**：只选择 `busy 容器数 < max_containers_per_core` 的物理核
4. **超线程对绑定**：选中物理核 `N` 后，绑定 `cpuset = "N,N+64"`（若均在池内）

**`max_containers_per_core=4` 的含义**：

限制的是**每个物理核（及其超线程对）上可同时运行的最大 busy 容器数**。计数时只统计 `busy` 状态的容器、只统计物理核（`cpu_id < 64`）、跳过当前容器自身。若所有物理核均达到上限，则回退到全池随机选择。

---

## 6. HTTP API 详细说明

### 6.1 POST /create_manager

**请求体：**
```json
{
  "function_name": "float_operation",
  "cpuset_cpus": "0,64,2,66"
}
```

**响应：**
- `201`：`{"status": "created"}`
- `200`：`{"status": "exists"}`（已存在）

### 6.2 POST /dispatch/<function_name>

**请求体：** 函数输入 payload，可包含 `"is_workflow": true`

**响应（成功）：**
```json
{
  "status": "success",
  "output": {
    "result_key": "value",
    "__meta__": {
      "request_id": "req-abc123",
      "duration": 0.523,
      "cpuset": "2,66",
      "physical_cores": [2],
      "container_cpu_time": 0.501,
      "cgroup_throttle_ratio_delta": 0.02
    }
  }
}
```

### 6.3 POST /ensure_warmup/<function_name>

**请求体：**
```json
{"target_total_containers": 4}
```

**响应（202）：**
```json
{
  "accepted": true,
  "deduplicated": false,
  "job_id": "warmup-a1b2c3d4",
  "function": "float_operation",
  "status": "queued",
  "target_total": 4
}
```

### 6.4 GET /manager_status/<function_name>

**响应：**
```json
{
  "function": "float_operation",
  "total": 4,
  "idle": 2,
  "busy": 2,
  "containers": [
    {"id": "a1b2c3d4e5f6", "host_port": 32768}
  ]
}
```

### 6.5 GET /freq_stable

**响应：**
```json
{
  "stable": true,
  "avg_cur_mhz": 2400.5,
  "avg_max_mhz": 2400.0,
  "ratio": 1.0002,
  "max_delta_mhz": 12.3
}
```

---

## 7. 运行方式

### 7.1 手动单轮运行

```bash
# 1. 环境准备（关闭 Turbo Boost、设置 performance、清理容器等）
sudo ./pre_experiment.sh baseline

# 2. 启动基础服务
sudo docker run -d --name redis --cpuset-cpus="1" --cpuset-mems="1" -p 6379:6379 redis
sudo docker run -d --name couchdb-test --cpuset-cpus="3" --cpuset-mems="1" \
    -p 5984:5984 -e COUCHDB_USER=openwhisk -e COUCHDB_PASSWORD=openwhisk \
    apache/couchdb:2.3
python3 ./actions/network/server.py &

# 3. 启动 Controller
TASK_GROUPS_FILE=baseline_groups.json python3 controller.py > base.log 2>&1
```

### 7.2 多轮自动化实验

```bash
# 激活虚拟环境
. venv/bin/activate

# 运行 10 轮，定时 23:00 启动
sudo PYTHON=$(which python3) bash ./run_all_experiments.sh 10 23:00
```

脚本会自动执行：
1. 定时等待（如指定了启动时间）
2. 系统调优（关闭 Turbo Boost、禁用 C-state、禁用 Swap、设置 performance 调速器等）
3. 每轮执行：
   - **Baseline 模式**：清理环境 → 启动服务 → 启动 Controller → 等待 Client → 保存监控数据
   - **Experiment 模式**：同上，使用 `task_groups.json`
4. 实验结束后恢复机器原始状态（governor、Turbo、Swap、NTP 等）

### 7.3 中断实验

按 `Ctrl+C` 即可，`trap restore_env EXIT` 会确保环境恢复。

---

## 8. 实验自动化流程

Server 与 Client 通过 HTTP 状态机完成多轮实验的精确同步：

```
Server 侧 (run_all_experiments.sh)          Client 侧 (run_all_experiments.sh)
─────────────────────────────────          ─────────────────────────────────
1. 清理环境、启动基础服务
2. 启动 controller.py
3. 调用 /set_ready {round, mode}
                                           4. 轮询 /experiment_ready
                                           5. 收到 ready 信号
                                           6. 执行 run.py
                                           7. 调用 /client_done
8. 轮询 /wait_client_done
9. 收到 client_done
10. 停止监控、保存结果
11. 清理环境，进入下一轮
```

此设计避免了共享目录、SSH 等复杂依赖，仅通过 HTTP 即可实现跨机器的实验协调。

---

## 9. 关键设计要点

1. **容器池化复用**：优先复用 idle 容器，减少冷启动开销；Keeper 自动补齐、Cleaner 超时回收
2. **请求级 vs 容器级绑核**：支持两种粒度，前者灵活但需 lease 管理，后者固定但实现简单
3. **异步 Warmup + 去重**：避免客户端超时，同函数在途任务复用，提升启动效率
4. **双层 CPU 时间采集**：同时返回 `container_cpu_time`（cgroup 级别，含子进程）和 `process_cpu_time`（代理进程级别），Client 优先使用前者
5. **cgroup 指标透传**：将 `nr_periods`、`nr_throttled`、`throttled_time` 等底层调度指标透传给 Client，用于分析 CPU 带宽限制的影响
6. **频率稳定保障**：`/freq_stable` 接口确保实验在 CPU 频率稳定后才开始，排除频率波动干扰
7. **环境隔离**：每轮实验彻底清理所有容器，`pre_experiment.sh` 确保 Baseline 与 Experiment 起始条件一致
