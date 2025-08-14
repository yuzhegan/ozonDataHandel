
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Import Ozon orders (semicolon-delimited CSV) into MongoDB using polars,
with simple MD5 de-dup on 4 business keys, robust header aliasing,
partial UNIQUE index on MD5, and **preserved original column order**.

- 文档字段顺序严格按 CSV 原列顺序写入；_row_md5 追加在最后。
"""
from __future__ import annotations

import hashlib
from datetime import datetime, date, timedelta
from typing import Any, Optional, List, Dict

import polars as pl
from pymongo import MongoClient, UpdateOne


# ---------------------- helpers ----------------------
_ALIAS_MAP: Dict[str, List[str]] = {
    "订单号码": ["订单号码", "订单号", "订单编号", "订单No", "订单NO", "订单ID", "订单id"],
    "发货号码": ["发货号码", "发货号", "发运号码", "发运号", "运单号", "发货单号"],
    "正在处理中": ["正在处理中", "创建时间", "下单时间", "处理时间", "订单时间"],
    "货号": ["货号", "SKU", "商品SKU", "货品编码", "商品编码", "货品编号"],
}

def _strip_bom(s: str) -> str:
    return s.lstrip("\\ufeff") if isinstance(s, str) else s

def _normalize_header(name: str) -> str:
    return _strip_bom(name).strip()

def _resolve_one(df_cols: List[str], canonical: str) -> Optional[str]:
    cols_norm = { _normalize_header(c): c for c in df_cols }
    for cand in _ALIAS_MAP.get(canonical, [canonical]):
        key = _normalize_header(cand)
        if key in cols_norm:
            return cols_norm[key]
    for cand in _ALIAS_MAP.get(canonical, [canonical]):
        key = _normalize_header(cand)
        for k_norm, orig in cols_norm.items():
            if key in k_norm:
                return orig
    return None

def _resolve_cols(df_cols: List[str], desired: List[str]) -> List[str]:
    resolved = []
    missing = []
    for d in desired:
        r = _resolve_one(df_cols, d)
        if r is None:
            missing.append(d)
        else:
            resolved.append(r)
    if missing:
        raise KeyError(f"缺少用于MD5的列: {missing}；现有列: {df_cols}")
    return resolved

def _parse_ymd(d: str) -> date:
    return datetime.strptime(d, "%Y-%m-%d").date()

def _date_list_from_days_back(n: int, today: date | None = None) -> List[date]:
    if today is None:
        today = date.today()
    return [today - timedelta(days=i) for i in range(1, n + 1)]

def _as_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, date):
        return v.strftime("%Y-%m-%d")
    return str(v).strip()

def _row_md5_simple(row: dict[str, Any], cols: List[str]) -> str:
    raw = "|".join(_as_str(row.get(c)) for c in cols)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()

def _normalize_date_expr(col: str) -> pl.Expr:
    src = pl.col(col).cast(pl.Utf8, strict=False)
    return pl.coalesce(
        [
            src.str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False).dt.date(),
            src.str.strptime(pl.Datetime, "%Y-%m-%d %H:%M", strict=False).dt.date(),
            src.str.strptime(pl.Date, "%Y-%m-%d", strict=False),
        ]
    )


# ---------------------- main ----------------------
def import_ozon_orders_polars(
    csv_path: str,
    mongo_uri: str = "mongodb://localhost:27017",
    db_name: str = "ozondatas",
    coll_name: str = "order_info",
    date_field: str = "正在处理中",
    days_back: int | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    include_dates: List[str] | None = None,
    hash_field: str = "_row_md5",
    hash_cols: List[str] | None = None,
    batch_size: int = 1000,
) -> int:
    # 1) 读 CSV（Polars 类型推断）
    df = pl.read_csv(
        csv_path,
        separator=";",
        quote_char='"',
        encoding="utf8",
        infer_schema_length=0,
        null_values=["", "NULL", "null"],
        truncate_ragged_lines=True,
    )
    # 去除首列可能的 BOM
    if df.columns and df.columns[0].startswith("\\ufeff"):
        df = df.rename({df.columns[0]: df.columns[0].lstrip("\\ufeff")})

    # 保留原始列顺序（稍后构建文档时按此顺序写入）
    original_col_order: List[str] = list(df.columns)

    # 2) 日期列解析（支持别名）
    if date_field not in df.columns:
        resolved = _resolve_one(df.columns, date_field)
        if resolved is None:
            raise KeyError(f"date_field '{date_field}' 不在列中，且未找到可用别名；现有列: {df.columns}")
        date_field = resolved

    df = df.with_columns(_normalize_date_expr(date_field).alias("_filter_date"))
    fil = None
    if include_dates:
        target = [_parse_ymd(d) for d in include_dates]
        fil = pl.col("_filter_date").is_in(target)
    elif start_date and end_date:
        sd, ed = _parse_ymd(start_date), _parse_ymd(end_date)
        if sd > ed:
            sd, ed = ed, sd
        fil = pl.col("_filter_date").is_between(sd, ed, closed="both")
    elif isinstance(days_back, int) and days_back > 0:
        target = _date_list_from_days_back(days_back, date.today())
        fil = pl.col("_filter_date").is_in(target)
    if fil is not None:
        df = df.filter(fil)
    df = df.drop("_filter_date")

    # 3) 计算 MD5（仅四列，支持别名/自定义）
    desired = hash_cols or ["订单号码", "发货号码", "正在处理中", "货号"]
    key_cols = _resolve_cols(df.columns, desired)
    md5_list = []
    for row in df.iter_rows(named=True):
        md5_list.append(_row_md5_simple(row, key_cols))
    df = df.with_columns(pl.Series(hash_field, md5_list))

    # 4) 写 Mongo：部分唯一索引（仅字符串）
    client = MongoClient(mongo_uri)
    coll = client[db_name][coll_name]

    index_name = f"uniq_{hash_field}_str"
    existing = coll.index_information()
    if index_name not in existing:
        coll.create_index(
            [(hash_field, 1)],
            name=index_name,
            unique=True,
            partialFilterExpression={hash_field: {"$type": "string"}},
        )

    inserted = 0
    seen = set()
    from pymongo import UpdateOne  # re-import to satisfy static tools
    ops: List[UpdateOne] = []

    def flush():
        nonlocal inserted, ops
        if not ops:
            return
        res = coll.bulk_write(ops, ordered=False)
        inserted += getattr(res, "upserted_count", 0) or 0
        ops = []

    for row in df.iter_rows(named=True):
        md5v = row.get(hash_field)
        if not isinstance(md5v, str) or md5v in seen:
            continue
        seen.add(md5v)

        # 严格按 original_col_order 构建文档；然后把 hash_field 追加到最后
        doc = {}
        for col in original_col_order:
            # 某些列可能在过滤/转换后不在行中（极少数情况），做下保护
            if col in row:
                doc[col] = row[col]
        doc[hash_field] = md5v  # 最后追加

        ops.append(UpdateOne({hash_field: md5v}, {'$setOnInsert': doc}, upsert=True))

        if len(ops) >= batch_size:
            flush()

    flush()
    client.close()
    return inserted


if __name__ == "__main__":
    # n = import_ozon_orders_polars("./orders.csv", days_back=3)
    n = import_ozon_orders_polars("./orders.csv")
    print("inserted:", n)
    pass
    '''from import_ozon_orders_polars import import_ozon_orders_polars

    # 1) 导入昨天的数据（不含今天）
    n = import_ozon_orders_polars("./orders.csv", days_back=1)

    # 2) 导入昨天和前天
    n = import_ozon_orders_polars("./orders.csv", days_back=2)

    # 3) 按区间导入（含边界）
    n = import_ozon_orders_polars("./orders.csv",
                                  start_date="2025-06-06",
                                  end_date="2025-06-11")

    # 4) 只导入给定两天
    n = import_ozon_orders_polars("./orders.csv",
                                  include_dates=["2025-06-06", "2025-06-11"])

    # 5) 指定用“发运日期”作为筛选字段
    n = import_ozon_orders_polars("./orders.csv",
                                  date_field="发运日期",
                                  days_back=1)

    print("inserted:", n)'''
