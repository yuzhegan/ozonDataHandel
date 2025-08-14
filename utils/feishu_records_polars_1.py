# encoding='utf-8

# @Time: 2025-08-12
# @File: %
#!/usr/bin/env
from typing import Any, Dict, List, Tuple, Sequence, Optional
import json
import polars as pl

# ---------- 类型判定/归一化 ----------

def _is_text_item(obj: Any) -> bool:
    return isinstance(obj, dict) and "text" in obj

def _normalize_value(val: Any) -> Tuple[Any, Dict[str, str]]:
    """
    把各种值规整成 DataFrame 友好的形式，并返回该字段的 schema 片段：
    - 返回 (normalized_value, {"kind": ..., "container": ...})
    kind: "number" | "bool" | "string" | "text" | "text_list" | "string_list" | "json"
    container: "scalar" | "list"
    """
    if val is None:
        # 无法判断，默认当 string scalar；还原时原样 None
        return None, {"kind": "string", "container": "scalar"}

    # 数字
    if isinstance(val, (int, float)) and not isinstance(val, bool):
        return val, {"kind": "number", "container": "scalar"}

    # 布尔
    if isinstance(val, bool):
        return val, {"kind": "bool", "container": "scalar"}

    # 纯字符串
    if isinstance(val, str):
        return val, {"kind": "string", "container": "scalar"}

    # 列表
    if isinstance(val, list):
        # [ {"text": ...}, ... ] 这种飞书“文本数组”
        if all(_is_text_item(x) for x in val):
            texts = [x.get("text", "") for x in val]
            return texts, {"kind": "text_list", "container": "list"}
        # 纯字符串列表
        if all(isinstance(x, str) for x in val):
            return val, {"kind": "string_list", "container": "list"}
        # 混合/其他复杂类型：序列化成 JSON 字符串存起来
        return json.dumps(val, ensure_ascii=False), {"kind": "json", "container": "scalar"}

    # 单个 dict：如果是 {"text": "..."} 也算文本
    if _is_text_item(val):
        return val.get("text", ""), {"kind": "text", "container": "scalar"}

    # 其他复杂类型：JSON 化
    try:
        return json.dumps(val, ensure_ascii=False), {"kind": "json", "container": "scalar"}
    except Exception:
        return str(val), {"kind": "string", "container": "scalar"}


def _denormalize_value(val: Any, kind: str, container: str) -> Any:
    """
    按 schema 把 DataFrame 里的值还原成飞书结构。
    """
    if val is None:
        return None

    if kind == "number":
        return val  # int/float

    if kind == "bool":
        return bool(val)

    if kind == "string":
        return str(val)

    if kind == "text":
        # 飞书通常是列表形式，这里统一用列表更稳妥
        s = "" if val is None else str(val)
        return [{"text": s, "type": "text"}] if s != "" else []

    if kind == "text_list":
        # 期望 list[str] -> list[{text,...}]
        if val is None:
            return []
        if isinstance(val, list):
            return [{"text": ("" if x is None else str(x)), "type": "text"} for x in val]
        # 如果列被推断成了标量（比如只有一个值），也做兼容
        return [{"text": str(val), "type": "text"}]

    if kind == "string_list":
        if val is None:
            return []
        return list(val) if isinstance(val, list) else [str(val)]

    if kind == "json":
        # 尝试反序列化回原结构
        try:
            return json.loads(val)
        except Exception:
            return val  # 就留字符串

    # 兜底
    return val


# ---------- records -> Polars（+ schema） ----------

def records_to_polars_generic(records: Sequence[Dict[str, Any]]) -> Tuple[pl.DataFrame, Dict[str, Dict[str, str]]]:
    """
    通用拍平：返回 (DataFrame, schema_meta)
    - 把所有 record['fields'] 的键动态收集成列；
    - 识别并统一处理各种值类型（数字、布尔、字符串、文本数组等）；
    - 附带返回每个列的 schema 信息，用于还原。
    """
    # 收集所有字段名
    all_field_names: List[str] = []
    for rec in records:
        f = rec.get("fields", {}) or {}
        for k in f.keys():
            if k not in all_field_names:
                all_field_names.append(k)
    # 也把 record_id 等顶层有用信息保留（可选）
    meta_cols = []
    if any("record_id" in r for r in records):
        meta_cols.append("record_id")

    rows: List[Dict[str, Any]] = []
    # 记录每列的 schema（以第一条非 None 的值为准；后续遇到冲突尽量兼容）
    schema_meta: Dict[str, Dict[str, str]] = {}

    for rec in records:
        f = rec.get("fields", {}) or {}
        row: Dict[str, Any] = {}
        # 元信息
        if "record_id" in meta_cols:
            row["record_id"] = rec.get("record_id")

        for col in all_field_names:
            raw_val = f.get(col)
            norm_val, meta = _normalize_value(raw_val)

            # 首次建立 schema
            if col not in schema_meta and raw_val is not None:
                schema_meta[col] = meta
            # 若已存在 schema，但当前值是 None，就不动；如果产生冲突，这里你也可以加更复杂的合并策略

            row[col] = norm_val

        rows.append(row)

    # 构造 Polars DataFrame（根据 schema_meta 选择合理 dtype）
    # 数字统一用 Float64（也可细分 Int/Float）；列表用 List(Utf8)；其余 Utf8/Bool
    dtypes: Dict[str, pl.DataType] = {}
    for col in all_field_names:
        meta = schema_meta.get(col, {"kind": "string", "container": "scalar"})
        kind = meta["kind"]
        container = meta["container"]
        if container == "list":
            dtypes[col] = pl.List(pl.Utf8)
        else:
            if kind == "number":
                dtypes[col] = pl.Float64  # 或者 pl.Float64 / pl.Int64 按需切换
            elif kind == "bool":
                dtypes[col] = pl.Boolean
            elif kind in ("string", "text", "json"):
                dtypes[col] = pl.Utf8
            else:
                dtypes[col] = pl.Utf8

    for mc in meta_cols:
        dtypes[mc] = pl.Utf8

    df = pl.DataFrame(rows).with_columns([
        pl.col(c).cast(dtypes[c]) if c in dtypes else pl.col(c) for c in (meta_cols + all_field_names)
    ])
    # return df, {"fields": schema_meta, "meta_cols": {c: "string" for c in meta_cols}}
    return df


# ---------- Polars -> records（用 schema 还原） ----------

def polars_to_records_generic(df: pl.DataFrame, schema: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    通用还原：按 schema 把 DataFrame 还原为飞书 records 结构。
    """
    schema_meta: Dict[str, Dict[str, str]] = schema.get("fields", {})
    meta_cols: Dict[str, str] = schema.get("meta_cols", {})

    field_cols = [c for c in df.columns if c not in meta_cols]  # 除去 meta 列，剩下都是 fields

    out: List[Dict[str, Any]] = []
    for row in df.iter_rows(named=True):
        fields: Dict[str, Any] = {}

        # 还原 fields
        for col in field_cols:
            val = row.get(col)
            meta = schema_meta.get(col, {"kind": "string", "container": "scalar"})
            kind = meta["kind"]
            container = meta["container"]
            denorm = _denormalize_value(val, kind, container)
            # None 的字段建议干脆不放，避免创建“空值”覆盖
            if denorm is not None and denorm != []:
                fields[col] = denorm

        rec: Dict[str, Any] = {"fields": fields}

        # 还原 meta（如 record_id）
        if "record_id" in meta_cols and row.get("record_id"):
            rec["record_id"] = row["record_id"]

        out.append(rec)

    return out
