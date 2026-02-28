import json
import pandas as pd
import os

def flatten_json_data(json_data, source_label):
    """
    将嵌套的JSON数据展平为列表，方便转换为DataFrame。
    提取 'statistics' 下的每个任务，以及 'summary' 作为特殊任务。
    """
    rows = []
    
    # 1. 处理 Statistics (具体的任务)
    stats = json_data.get('statistics', {})
    for task_name, metrics in stats.items():
        row = {'Task': task_name, 'Source': source_label, 'Type': 'Task'}
        # 将该任务下的所有指标 (mean, variance, p95...) 加入行数据
        row.update(metrics)
        rows.append(row)
        
    # 2. 处理 Summary (全局汇总数据，如 throughput)
    summary = json_data.get('summary', {})
    if summary:
        row = {'Task': 'Global_Summary', 'Source': source_label, 'Type': 'Summary'}
        row.update(summary)
        rows.append(row)
        
    return rows

def process_comparison(base_file, exp_file, output_file):
    print(f"正在处理: {base_file} vs {exp_file} ...")
    
    # 读取 JSON 文件
    try:
        with open(base_file, 'r', encoding='utf-8') as f:
            base_json = json.load(f)
        with open(exp_file, 'r', encoding='utf-8') as f:
            exp_json = json.load(f)
    except FileNotFoundError as e:
        print(f"错误: 找不到文件 - {e}")
        return

    # 展平数据
    base_rows = flatten_json_data(base_json, 'Base')
    exp_rows = flatten_json_data(exp_json, 'Exp')

    # 转换为 DataFrame
    df_base = pd.DataFrame(base_rows)
    df_exp = pd.DataFrame(exp_rows)

    # 既然我们要对比，我们以 Task 为索引进行合并
    # 假设 'Task' 是唯一标识符
    # 使用 suffix 区分 Base 和 Exp 的数据列
    df_merged = pd.merge(df_base, df_exp, on='Task', suffixes=('_Base', '_Exp'), how='outer')

    # 找出所有的指标列（排除掉 Task, Source, Type 等非数值列）
    # 我们通过检查列名是否以 _Base 结尾来识别指标
    metric_cols = [c.replace('_Base', '') for c in df_merged.columns if c.endswith('_Base') and c not in ['Source_Base', 'Type_Base']]

    # 计算变化率 (Change Rate)
    # 公式: (Exp - Base) / Base * 100
    final_columns = ['Task'] # 用于构建最终 CSV 的列顺序
    
    for metric in metric_cols:
        base_col = f"{metric}_Base"
        exp_col = f"{metric}_Exp"
        change_col = f"{metric}_Change%"

        # 执行计算
        df_merged[change_col] = ((df_merged[exp_col] - df_merged[base_col]) / df_merged[base_col]) * 100
        
        # 将列加入顺序列表，方便阅读 (Base, Exp, Change% 放在一起)
        final_columns.extend([base_col, exp_col, change_col])

    # 整理最终的 DataFrame
    # 提取需要的列
    df_final = df_merged[final_columns]
    
    # 可选：将 Global_Summary 置顶
    df_summary = df_final[df_final['Task'] == 'Global_Summary']
    df_tasks = df_final[df_final['Task'] != 'Global_Summary']
    df_final = pd.concat([df_summary, df_tasks])

    # 保存为 CSV
    df_final.to_csv(output_file, index=False, float_format='%.4f')
    print(f"成功生成文件: {output_file}")

# ==========================================
# 主执行区域
# ==========================================

if __name__ == "__main__":
    process_comparison(
        base_file='base.json', 
        exp_file='exp.json', 
        output_file='base-exp.csv'
    )
    process_comparison(
        base_file='base2.json', 
        exp_file='exp.json', 
        output_file='base2-exp.csv'
    )
    process_comparison(
        base_file='base3.json', 
        exp_file='exp.json', 
        output_file='base3-exp.csv'
    )
