# encoding='utf-8

# @Time: 2025-08-12
# @File: %
#!/usr/bin/env
from __future__ import annotations
from typing import Any, Dict, List, Sequence, Optional
import polars as pl

# ---------- 辅助清洗函数 ----------
def _as_text(value: Any) -> str:
    """把 value 统一转成字符串；None -> ''"""
    if value is None:
        return ""
    return str(value)

def _list_of_text_items(value: Any) -> List[str]:
    """
    飞书里常见形态：
      - [{'text': 'xxx', 'type': 'text'}, ...]
      - ['str1', 'str2'] / 'str'
      - None
    统一抽取出纯文本列表
    """
    if value is None:
        return []
    if isinstance(value, list):
        out: List[str] = []
        for item in value:
            if isinstance(item, dict) and "text" in item:
                out.append(_as_text(item["text"]))
            else:
                out.append(_as_text(item))
        return out
    # 单值
    return [_as_text(value)]

def _first_text(value: Any) -> str:
    """取第一个文本项（SKU、中文名称、编号等通常只用第一个）"""
    arr = _list_of_text_items(value)
    return arr[0] if arr else ""

def _to_int_maybe(value: Any) -> Optional[int]:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except Exception:
        return None

# ---------- JSON -> Polars DataFrame ----------
def records_to_polars(records: Sequence[Dict[str, Any]]) -> pl.DataFrame:
    """
    将飞书 Bitable 记录（列表）拍平成表格，列顺序与截图一致：
    日期, 类别, 中文名称, 属性, SKU, Ozon ID, 编号
    """
    rows: List[Dict[str, Any]] = []
    for rec in records:
        f = rec.get("fields", {})
        # print(f)  # 调试用，看看 fields 结构
        row = {
            "日期": rec.get("日期") or f.get("日期") or "",  # 你上面是放在顶层的“日期”
            "类别": _as_text(f.get("类别")),
            "中文名称": _first_text(f.get("中文名称")),
            "属性": "；".join(_list_of_text_items(f.get("属性"))) if f.get("属性") else _as_text(f.get("属性")),
            "SKU": _first_text(f.get("SKU")),
            "Ozon ID": _to_int_maybe(f.get("Ozon ID")),
            "编号": _first_text(f.get("编号")),
        }
        rows.append(row)

    # 明确 schema，保证列顺序与类型友好
    schema = {
        "日期": pl.Utf8,
        "类别": pl.Utf8,
        "中文名称": pl.Utf8,
        "属性": pl.Utf8,
        "SKU": pl.Utf8,
        "Ozon ID": pl.Int64,  # 若有无法转 int 的会是 null
        "编号": pl.Utf8,
    }
    df = pl.DataFrame(rows, schema=schema)
    return df

# ---------- Polars DataFrame -> JSON ----------
def polars_to_records(df: pl.DataFrame) -> List[Dict[str, Any]]:
    """
    将拍平后的 DataFrame 再组装回飞书结构：
    [{'fields': {...}, '日期': 'yyyy-mm-dd'}, ...]
    把 SKU/中文名称/编号 组回 [{'text': 'xxx', 'type': 'text'}] 形式；
    属性组回 ['xxx'] 列表；Ozon ID 保持数字（可为 None）。
    """
    needed_cols = ["日期", "类别", "中文名称", "属性", "SKU", "Ozon ID", "编号"]
    missing = [c for c in needed_cols if c not in df.columns]
    if missing:
        raise KeyError(f"DataFrame 缺少列: {missing}")

    records: List[Dict[str, Any]] = []
    for row in df.iter_rows(named=True):
        fields = {
            "Ozon ID": row["Ozon ID"],  # 数字或 None
            "SKU": [{"text": _as_text(row["SKU"]), "type": "text"}] if row["SKU"] else [],
            "中文名称": [{"text": _as_text(row["中文名称"]), "type": "text"}] if row["中文名称"] else [],
            "属性": [_as_text(row["属性"])] if row["属性"] else [],
            "类别": _as_text(row["类别"]),
            "编号": [{"text": _as_text(row["编号"]), "type": "text"}] if row["编号"] else [],
        }
        rec = {
            "fields": fields,
        }
        # 如果有“日期”列，按你当前用法放在顶层
        if row["日期"]:
            rec["日期"] = _as_text(row["日期"])
        records.append(rec)
    return records

# ---------- 小示例 ----------
if __name__ == "__main__":
    exit()
    # 假设变量 records 就是你贴出的 JSON 列表
    df = records_to_polars(records)
    print(df.head())

    # 再转回飞书结构
    back = polars_to_records(df)
    print(back[0])
    # 你的 records 就是从 API 得到的列表（你上面打印出来的那个）
    # from feishu_records_polars import records_to_polars, polars_to_records

    df = records_to_polars(records)
    print(df)          # 就是截图那样的表结构

    # 如果你做了编辑/补字段，想写回
    records2 = polars_to_records(df)
    # records2 就回到了飞书字段结构，能直接用于创建/更新记录
