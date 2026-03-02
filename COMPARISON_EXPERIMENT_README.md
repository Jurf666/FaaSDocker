# 对比实验使用说明

## 问题背景

在之前的实验中发现，matmul和svd_compute这两个函数在baseline组（所有函数共享一个大组）中的运行时间远长于task_groups组（每个函数独立分组）：

- **matmul**: baseline平均1.78s vs task_groups平均0.34s (提升421%)
- **svd_compute**: baseline平均47.45s vs task_groups平均6.71s (提升607%)

## 关键修复

**已修复**：之前版本的对比脚本存在 **Controller 状态污染**问题。Controller 中的 `get_or_create_manager` 函数在函数管理器已存在时，不会更新 cpuset 配置。这导致：

1. 第一个实验（baseline）创建了 FunctionManager 并配置 baseline 的 cpuset
2. 后续实验（task_groups等）虽然传入了新的 cpuset，但由于 FunctionManager 已存在，**继续使用旧的 cpuset 配置**
3. 结果：所有实验实际使用的都是第一个实验的 CPU 配置！

**解决方案**：
- 在 [controller.py](controller.py) 中添加了 `/clear_managers` 端点，用于清空所有 FunctionManager
- 对比脚本在每个实验开始前会调用此端点，确保使用正确的 cpuset 配置
- 这样每个实验都会使用独立、正确的 CPU 资源配置

## 实验目的

通过四种不同的配置来逐步分析性能差异的原因：

| 配置 | 描述 | 分组方式 | 跨核 | 核心类型 |
|------|------|----------|------|----------|
| baseline | 所有函数一个大组 | 1个组 | 可跨核 | 物理核+逻辑核 |
| task_groups | 每个函数独立分组 | 27个组 | 不跨核 | 物理核+逻辑核 |
| baseline2 | 随机函数分组 | 6个组 | 不跨核 | 物理核+逻辑核 |
| baseline3 | 随机函数分组 | 6个组 | 不跨核 | 仅物理核 |

## 新增文件

1. **baseline2_groups.json** - 随机分组配置（6个组）
2. **baseline3_groups.json** - 随机分组配置（6个组，仅物理核）
3. **run_comparison_experiment.py** - 对比实验主脚本
4. **cgroup_manager.py** (已修改) - 新增physical_cores_only参数

## 使用方法

### 1. 确保Controller运行

```bash
python controller.py
```

### 2. 运行对比实验

```bash
python run_comparison_experiment.py
```

实验将自动依次运行四个配置，每个配置之间休息30秒。每个实验运行5分钟（可通过环境变量TEST_DURATION修改）。

### 3. 自定义参数（可选）

```bash
# 设置测试时长为180秒
export TEST_DURATION=180

# 使用NUMA节点1
export NUMA_NODE=1

# 运行实验
python run_comparison_experiment.py
```

## 输出结果

实验完成后，会在`comparison_results/`目录下生成：

1. **单个实验结果**：
   - `baseline_results.json`
   - `task_groups_results.json`
   - `baseline2_results.json`
   - `baseline3_results.json`

2. **对比报告**：
   - `performance_comparison_report.json` - JSON格式完整报告
   - `performance_comparison_report.md` - Markdown格式可读报告

3. **系统监控数据**：
   - `system_metrics_baseline.csv`
   - `system_metrics_task_groups.csv`
   - `system_metrics_baseline2.csv`
   - `system_metrics_baseline3.csv`

## 分析要点

查看生成的Markdown报告，重点关注：

1. **matmul和svd_compute**在四种配置下的平均耗时对比
2. 从baseline → baseline2 → task_groups 的性能变化趋势
3. baseline2 vs baseline3 对比物理核和逻辑核的影响

## 预期结论

通过这个逐步对比实验，可以确定性能差异的主要原因是：

- **资源竞争**：大组中多个函数共享CPU导致
- **跨核调度**：能否跨核调度的影响
- **超线程影响**：逻辑核vs物理核的性能差异

## 注意事项

1. 整个实验大约需要 **20-25分钟** （4个配置 × 5分钟 + 准备时间）
2. 确保系统资源充足，避免其他程序干扰
3. 如果Controller崩溃，需要重新启动后再运行实验
4. 每次实验开始前会自动清理Redis和CouchDB中的数据

## 快速查看结果

实验完成后，直接查看Markdown报告：

```bash
cat comparison_results/performance_comparison_report.md
```

或在VS Code中打开该文件以获得更好的阅读体验。
