#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
聚合 MongoDB 广告数据（opcampaign + mbcampagin），支持：
- 指定日期列表（自动兼容 2025-08-20 / 2025/8/20 等写法，聚合键统一为 YYYY/M/D）
- opcampaign：聚合 orders, ordersFromCPC, ordersMoney, ordersMoneyFromCPC, moneySpent, moneySpentFromCPC
- mbcampagin：聚合 orders, models, ordersMoney, modelsMoney, moneySpent
- 处理 "5 782,00" / "5782,00" / "1,234.56" 等数值字符串为 float
- 对 mbcampagin 支持“按字段排除值列表”的过滤（可选大小写不敏感，支持别名与数组字段）
- 可作为库函数调用 main(...)；也可命令行运行：`python -m mongo_ads_aggregator ...`
"""
from __future__ import annotations

import re
from typing import Any, Iterable, List, Tuple

from pymongo import MongoClient
import polars as pl


# ---------------------- 工具函数：数字解析 & 日期格式 ---------------------- #
def to_float_safe(x: Any) -> float:
    """
    将各种字符串/数字形式稳健转为 float：
      - "5782,00"（逗号小数）
      - "1 234,56" / 含不换行空格 \u00A0
      - "1,234.56"（逗号千分、点小数）
      - 空/破折号视为 0
    """
    if x is None:
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)

    s = str(x).strip()
    if s in {"", "-", "—", "NaN", "None", "null"}:
        return 0.0

    s = s.replace("\u00A0", "").replace(" ", "")
    s = re.sub(r"[^0-9,.\-]", "", s)

    if "," in s and "." in s:
        last_comma = s.rfind(",")
        last_dot = s.rfind(".")
        if last_comma > last_dot:
            s = s.replace(".", "")
            s = s.replace(",", ".")
        else:
            s = s.replace(",", "")
    else:
        if "," in s:
            s = s.replace(",", ".")

    try:
        return float(s)
    except ValueError:
        return 0.0


def norm_date_variants(date_str: str) -> List[str]:
    """
    接受 'YYYY-MM-DD' 或 'YYYY/M/D' 等，产出多种可能写法，便于 $in 命中：
      - YYYY/M/D, YYYY/MM/DD, YYYY-M-D, YYYY-MM-DD
    """
    date_str = date_str.strip()
    m = re.match(r"^\s*(\d{4})[/-](\d{1,2})[/-](\d{1,2})\s*$", date_str)
    if not m:
        raise ValueError(f"无法解析日期: {date_str}")
    y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
    return [
        f"{y}/{mo}/{d}",
        f"{y}/{mo:02d}/{d:02d}",
        f"{y}-{mo}-{d}",
        f"{y}-{mo:02d}-{d:02d}",
    ]


def norm_date_for_key(date_str: str) -> str:
    """统一 key 的日期格式：YYYY/M/D（无前导零）。"""
    m = re.match(r"^\s*(\d{4})[/-](\d{1,2})[/-](\d{1,2})\s*$", date_str.strip())
    if not m:
        return date_str.strip()
    y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
    return f"{y}/{mo}/{d}"


def get_any(d: dict, *keys: str) -> Any:
    """依次尝试多个字段名，返回首个存在的值。"""
    for k in keys:
        if k in d:
            return d[k]
    return None


# ---------------------- 排除过滤辅助 ---------------------- #
def _should_exclude(val: Any, excludes: list[Any], case_insensitive: bool) -> bool:
    """Python 侧兜底过滤：支持标量/数组；可选大小写不敏感。"""
    def norm(x):
        return x.lower() if case_insensitive and isinstance(x, str) else x
    exset = set(norm(v) for v in (excludes or []))
    if isinstance(val, list):
        return any(norm(x) in exset for x in val)
    return norm(val) in exset


# ---------------------- 数据抓取与聚合 ---------------------- #
def fetch_and_aggregate_opcampaign(
    coll, dates_variants: List[str]
) -> pl.DataFrame:
    """
    从 opcampaign 抓取指定日期的文档，字段名大小写/新老命名兼容，转 float 后按 (date, sku) 聚合。
    """
    cursor = coll.find({"date": {"$in": dates_variants}}, {"_id": 0})
    rows: List[dict[str, Any]] = []
    for doc in cursor:
        raw_date = get_any(doc, "date", "Date")
        if not raw_date:
            continue
        date_key = norm_date_for_key(str(raw_date))

        sku = get_any(doc, "sku", "SKU")
        if sku is None:
            continue
        sku = str(sku)

        rows.append(
            {
                "date": date_key,
                "sku": sku,
                "op_orders": to_float_safe(get_any(doc, "orders", "Orders")),
                "op_ordersFromCPC": to_float_safe(get_any(doc, "ordersFromCPC", "OrdersFromCPC")),
                "op_ordersMoney": to_float_safe(get_any(doc, "ordersMoney", "OrdersMoney")),
                "op_ordersMoneyFromCPC": to_float_safe(get_any(doc, "ordersMoneyFromCPC", "OrdersMoneyFromCPC")),
                "op_moneySpent": to_float_safe(get_any(doc, "moneySpent", "MoneySpent")),
                "op_moneySpentFromCPC": to_float_safe(get_any(doc, "moneySpentFromCPC", "MoneySpentFromCPC")),
            }
        )

    if not rows:
        return pl.DataFrame(
            {
                "date": pl.String,
                "sku": pl.String,
                "op_orders": pl.Float64,
                "op_ordersFromCPC": pl.Float64,
                "op_ordersMoney": pl.Float64,
                "op_ordersMoneyFromCPC": pl.Float64,
                "op_moneySpent": pl.Float64,
                "op_moneySpentFromCPC": pl.Float64,
            }
        )

    df = pl.DataFrame(rows)
    return (
        df.group_by(["date", "sku"])
        .agg(
            pl.col("op_orders").sum(),
            pl.col("op_ordersFromCPC").sum(),
            pl.col("op_ordersMoney").sum(),
            pl.col("op_ordersMoneyFromCPC").sum(),
            pl.col("op_moneySpent").sum(),
            pl.col("op_moneySpentFromCPC").sum(),
        )
        .sort(["date", "sku"])
    )


def fetch_and_aggregate_mbcampagin(
    coll,
    dates_variants: list[str],
    exclude_field: str | None = None,
    exclude_values: list[Any] | None = None,
    exclude_case_insensitive: bool = False,
    exclude_field_aliases: list[str] | None = None,
) -> pl.DataFrame:
    """
    从 mbcampagin 抓取指定日期的文档，转 float 后按 (date, sku) 聚合。
    额外支持：在查询阶段与Python阶段对 exclude_field 做排除过滤。
    - exclude_field: 需要过滤的字段名（如 "placement"）
    - exclude_values: 要排除的值列表（如 ["PLACEMENT_SEARCH_AND_CATEGORY","PLACEMENT_SMART"]）
    - exclude_case_insensitive: 排除时对字符串大小写不敏感
    - exclude_field_aliases: 字段别名兜底（如 ["Placement"]）
    """
    base_filter = {"date": {"$in": dates_variants}}

    # 若大小写敏感，则尽量在 Mongo 端做 $nin 过滤；大小写不敏感时，放到 Python 侧兜底
    mongo_filter = dict(base_filter)
    if exclude_field and exclude_values and not exclude_case_insensitive:
        mongo_filter[exclude_field] = {"$nin": exclude_values}

    cursor = coll.find(mongo_filter, {"_id": 0})

    rows: list[dict[str, Any]] = []
    for doc in cursor:
        # Python 侧兜底：处理字段别名、数组类型、大小写不敏感等复杂情况
        if exclude_field and exclude_values:
            val = get_any(doc, exclude_field, *(exclude_field_aliases or []))
            if val is not None and _should_exclude(val, exclude_values, exclude_case_insensitive):
                continue

        raw_date = get_any(doc, "date", "Date")
        if not raw_date:
            continue
        date_key = norm_date_for_key(str(raw_date))

        sku = get_any(doc, "sku", "SKU")
        if sku is None:
            continue
        sku = str(sku)

        rows.append(
            {
                "date": date_key,
                "sku": sku,
                "mb_orders": to_float_safe(get_any(doc, "orders", "Orders")),
                "mb_models": to_float_safe(get_any(doc, "models", "Models")),
                "mb_ordersMoney": to_float_safe(get_any(doc, "ordersMoney", "OrdersMoney")),
                "mb_modelsMoney": to_float_safe(get_any(doc, "modelsMoney", "ModelsMoney")),
                "mb_moneySpent": to_float_safe(get_any(doc, "moneySpent", "MoneySpent")),
            }
        )

    if not rows:
        return pl.DataFrame(
            {
                "date": pl.String,
                "sku": pl.String,
                "mb_orders": pl.Float64,
                "mb_models": pl.Float64,
                "mb_ordersMoney": pl.Float64,
                "mb_modelsMoney": pl.Float64,
                "mb_moneySpent": pl.Float64,
            }
        )

    df = pl.DataFrame(rows)
    return (
        df.group_by(["date", "sku"])
        .agg(
            pl.col("mb_orders").sum(),
            pl.col("mb_models").sum(),
            pl.col("mb_ordersMoney").sum(),
            pl.col("mb_modelsMoney").sum(),
            pl.col("mb_moneySpent").sum(),
        )
        .sort(["date", "sku"])
    )


# ---------------------- 主流程（可调用） ---------------------- #
def main(
    mongo_uri: str,
    db: str,
    dates: list[str],
    op_coll: str = "opcampaign",
    mb_coll: str = "mbcampagin",
    out_merged: str = "ads/ads_merged.csv",
    out_op: str = "ads/ads_opcampaign.csv",
    out_mb: str = "ads/ads_mbcampagin.csv",
    write_csv: bool = True,
    # ↓↓↓ mbcampagin 排除过滤参数
    mb_exclude_field: str | None = None,
    mb_exclude_values: list[Any] | None = None,
    mb_exclude_case_insensitive: bool = False,
    mb_exclude_field_aliases: list[str] | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    可调用版本：
    - 输入 Mongo 连接、库名、日期列表，及集合名/输出文件名
    - 返回 (op_df, mb_df, merged) 三个 DataFrame
    - write_csv=True 时会写出三个 CSV
    """
    # 组装所有可匹配的日期写法，统一 join key 用 YYYY/M/D
    variants_set = set()
    for ds in dates:
        for v in norm_date_variants(ds):
            variants_set.add(v)
    dates_variants = sorted(variants_set)

    cli = MongoClient(mongo_uri)
    _db = cli[db]
    coll_op = _db[op_coll]
    coll_mb = _db[mb_coll]

    op_df = fetch_and_aggregate_opcampaign(coll_op, dates_variants)
    mb_df = fetch_and_aggregate_mbcampagin(
        coll_mb,
        dates_variants,
        exclude_field=mb_exclude_field,
        exclude_values=mb_exclude_values,
        exclude_case_insensitive=mb_exclude_case_insensitive,
        exclude_field_aliases=mb_exclude_field_aliases,
    )

    # 合并（外连接，保留双方）
    merged = (
        op_df.join(mb_df, on=["date", "sku"], how="outer")
        .fill_null(0.0)
        .sort(["date", "sku"])
    )

    if write_csv:
        (op_df if op_df.height > 0 else pl.DataFrame(schema=op_df.schema)).write_csv(out_op)
        (mb_df if mb_df.height > 0 else pl.DataFrame(schema=mb_df.schema)).write_csv(out_mb)
        merged.write_csv(out_merged)
        print(f"[OK] 已导出：\n - {out_op}\n - {out_mb}\n - {out_merged}")

    return op_df, mb_df, merged


# ---------------------- 命令行入口（保留原用法） ---------------------- #
def _cli():
    import argparse

    parser = argparse.ArgumentParser(description="按日期列表汇总 opcampaign 与 mbcampagin 广告数据")
    parser.add_argument("--mongo-uri", default="mongodb://localhost:27017", help="MongoDB 连接串")
    parser.add_argument("--db", required=True, help="数据库名，如 ozondatas")
    parser.add_argument("--op-coll", default="opcampaign", help="opcampaign 集合名（默认：opcampaign）")
    parser.add_argument("--mb-coll", default="mbcampagin", help="mbcampagin 集合名（默认：mbcampagin）")
    parser.add_argument("--dates", nargs="+", required=True, help='日期列表，例如：--dates 2025-08-20 2025/8/21')
    parser.add_argument("--out-merged", default="ads_merged.csv", help="合并后导出 CSV（默认：ads_merged.csv）")
    parser.add_argument("--out-op", default="ads_opcampaign.csv", help="opcampaign 聚合导出 CSV（默认：ads_opcampaign.csv）")
    parser.add_argument("--out-mb", default="ads_mbcampagin.csv", help="mbcampagin 聚合导出 CSV（默认：ads_mbcampagin.csv）")

    # 新增：mbcampagin 排除过滤参数
    parser.add_argument("--mb-exclude-field", help="mbcampagin 需排除的字段名，如 placement")
    parser.add_argument("--mb-exclude-values", nargs="+", help="需排除的值列表，如：A B C")
    parser.add_argument("--mb-exclude-ci", action="store_true", help="排除值大小写不敏感")
    parser.add_argument("--mb-exclude-aliases", nargs="+", help="字段别名（可选），如 Placement")

    args = parser.parse_args()

    main(
        mongo_uri=args.mongo_uri,
        db=args.db,
        dates=args.dates,
        op_coll=args.op_coll,
        mb_coll=args.mb_coll,
        out_merged=args.out_merged,
        out_op=args.out_op,
        out_mb=args.out_mb,
        write_csv=True,
        mb_exclude_field=args.mb_exclude_field,
        mb_exclude_values=args.mb_exclude_values,
        mb_exclude_case_insensitive=args.mb_exclude_ci,
        mb_exclude_field_aliases=args.mb_exclude_aliases,
    )


if __name__ == "__main__":
    exit()
    # _cli()
    op_df, mb_df, merged = main(
    mongo_uri="mongodb://localhost:27017",
    db="ozondatas",
    dates=["2025-08-20", "2025/8/21"],
    op_coll="opcampaign",
    mb_coll="mbcampagin",
    write_csv=True,
    mb_exclude_field="campaignId",
    mb_exclude_values=['8787692'],
    mb_exclude_case_insensitive=True,
    # mb_exclude_field_aliases=["Placement"],
    )
    print(merged.head())
