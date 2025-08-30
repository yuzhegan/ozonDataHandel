# encoding='utf-8

# @Time: 2025-08-28
# @File: %
#!/usr/bin/env
from icecream import ic
import os
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
汇总指定日期列表的广告数据（MongoDB）：
- opcampaign：按 (date, sku) 聚合字段 = orders, ordersFromCPC, ordersMoney, ordersMoneyFromCPC, moneySpent, moneySpentFromCPC
- mbcampagin：按 (date, sku) 聚合字段 = orders, models, ordersMoney, modelsMoney, moneySpent
注意：自动将 "5782,00" / "1 234,56" / "1,234.56" 等字符串稳健转为 float。
"""

import argparse
import re
from typing import Any, Dict, Iterable, List, Tuple

from pymongo import MongoClient
import polars as pl


# ---------------------- 工具函数：数字解析 & 日期格式 ---------------------- #
def to_float_safe(x: Any) -> float:
    """
    将各种字符串/数字形式稳健转为 float：
    支持：
      - "5782,00"（逗号小数）
      - "1 234,56" 或 含不换行空格 \u00A0
      - "1,234.56"（逗号千分、点小数）
      - "—" / "-" / "" 视为 0
    """
    if x is None:
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)

    s = str(x).strip()
    if s in {"", "-", "—", "NaN", "None", "null"}:
        return 0.0

    # 去除空格与不间断空格
    s = s.replace("\u00A0", "").replace(" ", "")

    # 仅保留数字、符号、逗号、小数点
    s = re.sub(r"[^0-9,.\-]", "", s)

    # 同时出现逗号与点：以“最后出现的分隔符”为小数点，另一个视作千分分隔并去掉
    if "," in s and "." in s:
        last_comma = s.rfind(",")
        last_dot = s.rfind(".")
        if last_comma > last_dot:
            # 逗号作小数点，去掉所有点
            s = s.replace(".", "")
            s = s.replace(",", ".")
        else:
            # 点作小数点，去掉所有逗号
            s = s.replace(",", "")
    else:
        # 仅有逗号，视作小数点
        if "," in s:
            s = s.replace(",", ".")

    try:
        return float(s)
    except ValueError:
        return 0.0


def norm_date_variants(date_str: str) -> List[str]:
    """
    接受 'YYYY-MM-DD' 或 'YYYY/M/D' 等，产出多种可能写法，便于 $in 命中：
      - YYYY/M/D
      - YYYY/MM/DD
      - YYYY-M-D
      - YYYY-MM-DD
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
    """
    统一 key 用的日期格式：YYYY/M/D（无前导零），便于 join。
    """
    m = re.match(r"^\s*(\d{4})[/-](\d{1,2})[/-](\d{1,2})\s*$", date_str.strip())
    if not m:
        return date_str.strip()
    y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
    return f"{y}/{mo}/{d}"


def get_any(d: Dict[str, Any], *keys: str) -> Any:
    """
    依次尝试多个字段名，返回首个存在的值。
    """
    for k in keys:
        if k in d:
            return d[k]
    return None


# ---------------------- 数据抓取与聚合 ---------------------- #
def fetch_and_aggregate_opcampaign(
    coll, dates_variants: List[str]
) -> pl.DataFrame:
    """
    从 opcampaign 抓取指定日期的文档，字段名大小写/新老命名兼容，转 float 后按 (date, sku) 聚合。
    """
    cursor = coll.find({"date": {"$in": dates_variants}}, {"_id": 0})
    rows: List[Dict[str, Any]] = []
    for doc in cursor:
        raw_date = get_any(doc, "date", "Date")
        if not raw_date:
            continue
        date_key = norm_date_for_key(str(raw_date))

        # sku 兼容大小写
        sku = get_any(doc, "sku", "SKU")
        if sku is None:
            # 部分数据可能没有 sku，不参与
            continue
        sku = str(sku)

        row = {
            "date": date_key,
            "sku": sku,
            "op_orders": to_float_safe(get_any(doc, "orders", "Orders")),
            "op_ordersFromCPC": to_float_safe(
                get_any(doc, "ordersFromCPC", "OrdersFromCPC")
            ),
            "op_ordersMoney": to_float_safe(
                get_any(doc, "ordersMoney", "OrdersMoney")
            ),
            "op_ordersMoneyFromCPC": to_float_safe(
                get_any(doc, "ordersMoneyFromCPC", "OrdersMoneyFromCPC")
            ),
            "op_moneySpent": to_float_safe(
                get_any(doc, "moneySpent", "MoneySpent")
            ),
            "op_moneySpentFromCPC": to_float_safe(
                get_any(doc, "moneySpentFromCPC", "MoneySpentFromCPC")
            ),
        }
        rows.append(row)

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
    agg = (
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
    return agg


def fetch_and_aggregate_mbcampagin(
    coll, dates_variants: List[str]
) -> pl.DataFrame:
    """
    从 mbcampagin 抓取指定日期的文档，字段名兼容，转 float 后按 (date, sku) 聚合。
    需要聚合字段：orders, models, ordersMoney, modelsMoney, moneySpent
    """
    cursor = coll.find({"date": {"$in": dates_variants}}, {"_id": 0})
    rows: List[Dict[str, Any]] = []
    for doc in cursor:
        raw_date = get_any(doc, "date", "Date")
        if not raw_date:
            continue
        date_key = norm_date_for_key(str(raw_date))

        sku = get_any(doc, "sku", "SKU")
        if sku is None:
            continue
        sku = str(sku)

        row = {
            "date": date_key,
            "sku": sku,
            "mb_orders": to_float_safe(get_any(doc, "orders", "Orders")),
            "mb_models": to_float_safe(get_any(doc, "models", "Models")),
            "mb_ordersMoney": to_float_safe(
                get_any(doc, "ordersMoney", "OrdersMoney")
            ),
            "mb_modelsMoney": to_float_safe(
                get_any(doc, "modelsMoney", "ModelsMoney")
            ),
            "mb_moneySpent": to_float_safe(
                get_any(doc, "moneySpent", "MoneySpent")
            ),
        }
        rows.append(row)

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
    agg = (
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
    return agg

# ---------------------- 主流程（可调用） ---------------------- #
from typing import Tuple

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
    mb_df = fetch_and_aggregate_mbcampagin(coll_mb, dates_variants)

    # 合并（外连接，保留双方）
    merged = (
        op_df.join(mb_df, on=["date", "sku"], how="outer_coalesce")
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
    )


if __name__ == "__main__":
    # _cli()
    
    op_df, mb_df, merged = main(
    mongo_uri="mongodb://localhost:27017",
    db="ozondatas",
    dates=["2025-08-20", "2025/8/21"],
    op_coll="opcampaign",
    mb_coll="mbcampagin",
    # write_csv=False,   # 只想拿 DataFrame，不落盘
    write_csv=True,   # 只想拿 DataFrame，不落盘
    )
    print(merged.head())



