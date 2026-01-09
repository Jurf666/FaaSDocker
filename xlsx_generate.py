import json
import sys
from typing import Dict, Any
from openpyxl import Workbook


HEADERS = [
    "function",
    "mean",
    "std",
    "cv",
    "min",
    "max",
    "iqr",
    "p90",
    "p95",
]


def load_stats(path: str) -> Dict[str, Dict[str, Any]]:
    """读取一个 JSON 文件，返回 {func: stats}。
    - 优先使用 data["statistics"]
    - 兼容 pinned_experiment_results.json 的纯映射格式（无 summary）
    """
    try:
        with open(path, "r") as f:
            data = json.load(f)
        stats = data.get("statistics")
        if isinstance(stats, dict):
            return stats
        # 兼容纯映射格式
        if isinstance(data, dict) and "summary" not in data:
            return data
        return {}
    except Exception as e:
        print(f"[ERROR] Failed to load {path}: {e}")
        return {}


def write_xlsx(stats_map: Dict[str, Dict[str, Any]], xlsx_path: str, sheet_name: str = "Results") -> None:
    """将统计数据写入 XLSX 文件（一个工作表）。去除 count/variance 等不需要列。"""
    wb = Workbook()
    ws = wb.active
    ws.title = sheet_name
    ws.append(HEADERS)

    for func, s in sorted(stats_map.items()):
        if not isinstance(s, dict):
            continue
        row = [func]
        for key in HEADERS[1:]:
            row.append(s.get(key))
        ws.append(row)

    wb.save(xlsx_path)


def main():
    input_json = sys.argv[1] if len(sys.argv) > 1 else "baseline_results.json"
    # 输出默认同名 .xlsx；若传第二个参数则使用之
    default_xlsx = (input_json.rsplit(".", 1)[0] + ".xlsx") if "." in input_json else "results.xlsx"
    output_xlsx = sys.argv[2] if len(sys.argv) > 2 else default_xlsx

    stats = load_stats(input_json)
    if not stats:
        print(f"[ERROR] No statistics found in {input_json}")
        return

    # 工作表名用文件基名，便于区分
    sheet_name = input_json.rsplit("/", 1)[-1]
    write_xlsx(stats, output_xlsx, sheet_name=sheet_name)
    print(f"[INFO] Excel 导出完成: {output_xlsx}")


if __name__ == "__main__":
    main()
