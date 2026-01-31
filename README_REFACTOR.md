# 实验脚本重构说明

## 概述

原始的 `run_experiment_closed_loop.py` 脚本包含了大量功能，可读性较差。经过重构后，将不同功能模块分离到独立文件中，使用面向对象的方式组织代码。

## 文件结构

### 核心模块文件

1. **`cgroup_manager.py`** - Cgroup 配置管理
   - `CgroupManager` 类：负责基于任务分组文件生成 cgroup 配置
   - 主要方法：
     - `generate_configs()`: 生成 cgroup 配置
     - `get_cgroup_for_function()`: 根据函数名获取对应的 cgroup 配置

2. **`workflow_warmer.py`** - 工作流预热
   - `WorkflowWarmer` 类：负责工作流预热，生成可复用的中间结果
   - 主要方法：
     - `warmup_all_workflows()`: 预热所有工作流
     - 支持 Video、Recognizer、SVD、WordCount 工作流

3. **`system_monitor.py`** - 系统监控
   - `SystemMonitor` 类：负责系统监控，记录 CPU 使用率等指标
   - 主要方法：
     - `start()`: 启动监控线程
     - `stop()`: 停止监控线程
     - 数据保存到 CSV 文件

4. **`data_cleaner.py`** - 数据清理
   - `DataCleaner` 类：负责清理工作流产生的中间数据
   - 主要方法：
     - `cleanup_all()`: 清理所有工作流中间数据
     - 支持 Redis 和 CouchDB 数据清理

5. **`experiment_client.py`** - 实验客户端
   - `ExperimentClient` 类：负责实验客户端的管理和执行
   - 主要方法：
     - `dispatch_simple()`: 发送简单函数请求
     - `client_worker()`: 客户端工作线程
     - `run_experiment()`: 运行实验
     - `compute_statistics()`: 计算统计数据

### 主脚本文件

6. **`run_experiment_closed_loop_refactored.py`** - 重构后的主脚本
   - `ClosedLoopExperiment` 类：实验主类，协调各个模块
   - 主要方法：
     - `setup()`: 设置实验环境
     - `run()`: 运行实验
     - `save_results()`: 保存实验结果
     - `cleanup()`: 清理实验数据

## 使用方法

### 运行重构后的实验

```bash
# 使用默认参数
python run_experiment_closed_loop_refactored.py

# 使用环境变量配置参数
TEST_DURATION=600 NUMA_NODE=0 python run_experiment_closed_loop_refactored.py
```

### 环境变量配置

- `CONTROLLER_HOST`: Controller 地址 (默认: localhost)
- `CONTROLLER_PORT`: Controller 端口 (默认: 5000)
- `TEST_DURATION`: 测试时长(秒) (默认: 300)
- `RANDOM_SEED`: 随机种子 (默认: 42)
- `NUMA_NODE`: NUMA 节点号 (默认: 0)
- `REDIS_HOST`: Redis 地址 (默认: 172.17.0.1)
- `REDIS_PORT`: Redis 端口 (默认: 6379)
- `COUCHDB_URL`: CouchDB URL

## 重构优势

### 1. 模块化设计
- 每个功能模块独立成文件，职责清晰
- 便于单独测试和维护
- 代码复用性更强

### 2. 面向对象
- 使用类封装相关功能
- 提高代码可读性和可维护性
- 便于扩展新功能

### 3. 清晰的代码结构
```
原始版本 (run_experiment_closed_loop.py):
├── Part 1: 准备工作 (多个函数)
├── Part 2: 核心代码 (多个函数)
├── Part 3: 指标监控与计算 (多个函数)
├── Part 4: 数据清理与辅助函数 (多个函数)
└── main() (主函数)

重构版本:
├── cgroup_manager.py (CgroupManager 类)
├── workflow_warmer.py (WorkflowWarmer 类)
├── system_monitor.py (SystemMonitor 类)
├── data_cleaner.py (DataCleaner 类)
├── experiment_client.py (ExperimentClient 类)
└── run_experiment_closed_loop_refactored.py (ClosedLoopExperiment 类)
```

### 4. 易于理解和修改
- 每个类的功能一目了然
- 修改某个功能只需关注对应的文件
- 不会因为文件过长而难以定位代码

## 功能保持一致性

重构后的代码完全保持了原有功能：
- ✅ Cgroup 配置生成逻辑不变
- ✅ 工作流预热逻辑不变
- ✅ 系统监控逻辑不变
- ✅ 数据清理逻辑不变
- ✅ 客户端请求逻辑不变
- ✅ 统计计算逻辑不变
- ✅ 结果保存格式不变

## 注意事项

1. **保留原脚本**: 原始的 `run_experiment_closed_loop.py` 仍然保留，可以继续使用
2. **新脚本命名**: 重构后的脚本命名为 `run_experiment_closed_loop_refactored.py`
3. **依赖关系**: 新脚本依赖于新创建的 5 个模块文件，需要在同一目录下
4. **测试建议**: 建议先在小规模测试中验证重构后的脚本，确认功能一致后再正式使用

## 后续扩展建议

1. 可以为每个类添加更多配置选项
2. 可以添加日志系统替代 print 语句
3. 可以添加单元测试
4. 可以添加配置文件支持，避免硬编码
