# -*- coding: utf-8 -*-
"""
Summarise the highest sales within a lookback window from MongoDB.

This script provides a helper function ``summarize_max_sales`` that
connects to a MongoDB collection, filters records within the last
``days_back`` days based on a timestamp field, aggregates sales
(using either sum or max) over specified grouping fields, and returns
the group(s) with the highest aggregated sales.  The result is
returned as a Polars DataFrame for further analysis.

Example usage::

    from summarize_max_sales_polars import summarize_max_sales

    # Find the group with the highest total sales in the last 28 days,
    # grouping by SKU and Ozon ID
    df = summarize_max_sales(
        mongo_uri="mongodb://localhost:27017",
        db_name="ozondatas",
        coll_name="order_info",
        date_field="正在处理中",
        group_fields=["货号", "Ozon ID"],
        agg_field="数量",
        days_back=28,
        date_format="%Y-%m-%d %H:%M:%S",
        timezone="Asia/Seoul",
        summary_mode="sum",
        return_all=False
    )
    print(df)

    # Find all groups and their total sales in the last 28 days, sorted
    # descending by total sales
    df_all = summarize_max_sales(
        mongo_uri="mongodb://localhost:27017",
        db_name="ozondatas",
        coll_name="order_info",
        date_field="正在处理中",
        group_fields=["货号", "Ozon ID", "配送集群"],
        agg_field="数量",
        days_back=28,
        summary_mode="sum",
        return_all=True
    )
    print(df_all.head())

``summary_mode`` controls whether the aggregation uses a sum or
a maximum.  ``return_all=False`` returns only the single group with
the highest aggregated value; ``return_all=True`` returns all groups
sorted descending.

"""

from __future__ import annotations

import datetime as _dt
from typing import List, Dict, Sequence, Optional, Any

from zoneinfo import ZoneInfo
import polars as pl
from pymongo import MongoClient

__all__ = ["summarize_max_sales"]


def summarize_max_sales(
    *,
    mongo_uri: str = "mongodb://localhost:27017",
    db_name: str = "ozondatas",
    coll_name: str = "order_info",
    date_field: str = "正在处理中",
    group_fields: Sequence[str] = ("货号", "Ozon ID"),
    agg_field: str = "数量",
    days_back: int = 28,
    date_format: str = "%Y-%m-%d %H:%M:%S",
    timezone: str = "Asia/Seoul",
    summary_mode: str = "sum",
    return_all: bool = False,
) -> pl.DataFrame:
    """Summarise the highest sales in a lookback window.

    Parameters
    ----------
    mongo_uri:
        Connection URI for MongoDB.  Defaults to a local instance.
    db_name:
        Name of the database.
    coll_name:
        Name of the collection containing order documents.
    date_field:
        Name of the timestamp field storing the processing date/time
        as a string.  This is parsed using ``date_format`` and
        interpreted in the given ``timezone``.
    group_fields:
        Sequence of field names used to group records.  The
        aggregated result will contain one row per unique
        combination of these fields.
    agg_field:
        Name of the numeric field whose values represent sales
        quantities.  Values are coerced to doubles.
    days_back:
        Number of days to look back from the reference (current)
        date.  For example, ``28`` filters records to the last
        28 days including today.
    date_format:
        Format string for parsing ``date_field`` values to
        datetimes.  Defaults to ``"%Y-%m-%d %H:%M:%S"``.
    timezone:
        IANA timezone identifier used when interpreting timestamps
        and computing the lookback range.  For example,
        ``"Asia/Seoul"`` or ``"Europe/Moscow"``.
    summary_mode:
        Either ``"sum"`` or ``"max"``.  ``"sum"`` aggregates
        sales values using a sum; ``"max"`` takes the maximum
        single sale value within each group.  Defaults to sum.
    return_all:
        If ``False`` (default), return only the group(s) with the
        highest aggregated sales.  If ``True``, return all groups
        sorted descending by aggregated sales.

    Returns
    -------
    pl.DataFrame
        A Polars DataFrame containing the grouped fields and a
        column named ``total_sales``.  If ``return_all`` is
        ``False``, the DataFrame will contain the top group(s)
        only; otherwise, it contains all groups sorted by
        ``total_sales`` descending.
    """
    if summary_mode not in {"sum", "max"}:
        raise ValueError("summary_mode must be 'sum' or 'max'")
    # Connect to MongoDB
    client = MongoClient(mongo_uri)
    coll = client[db_name][coll_name]

    # Determine the lookback range in the given timezone
    now = _dt.datetime.now(ZoneInfo(timezone))
    # start date inclusive: now - (days_back - 1) days
    start_dt = now - _dt.timedelta(days=days_back - 1)
    # end date exclusive: the start of tomorrow
    end_dt_exclusive = (now + _dt.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

    # Build the aggregation pipeline
    pipeline: List[Dict[str, Any]] = []
    # Stage 1: parse the date string to a datetime
    pipeline.append(
        {
            "$addFields": {
                "_proc_datetime": {
                    "$dateFromString": {
                        "dateString": f"${date_field}",
                        "format": date_format,
                        "timezone": timezone,
                    }
                }
            }
        }
    )
    # Stage 2: match records within the lookback window
    pipeline.append(
        {
            "$match": {
                "_proc_datetime": {
                    "$gte": start_dt,
                    "$lt": end_dt_exclusive,
                }
            }
        }
    )
    # Stage 3: group by the specified fields and aggregate sales
    group_id: Dict[str, Any] = {fld: f"${fld}" for fld in group_fields}
    if summary_mode == "sum":
        agg_op = {"$sum": {"$toDouble": f"${agg_field}"}}
    else:
        # summary_mode == "max"
        agg_op = {"$max": {"$toDouble": f"${agg_field}"}}
    pipeline.append(
        {
            "$group": {
                "_id": group_id,
                "total_sales": agg_op,
            }
        }
    )
    # Stage 4: sort descending by total_sales
    pipeline.append({"$sort": {"total_sales": -1}})
    # Stage 5: limit if only returning top group(s)
    if not return_all:
        pipeline.append({"$limit": 1})

    # Execute the pipeline
    results = list(coll.aggregate(pipeline))
    client.close()
    if not results:
        # Return empty DataFrame with appropriate columns
        cols = list(group_fields) + ["total_sales"]
        return pl.DataFrame({col: [] for col in cols})
    # Convert results to Polars DataFrame
    # Flatten the _id document into separate columns
    flattened = []
    for doc in results:
        row: Dict[str, Any] = {}
        for fld in group_fields:
            row[fld] = doc["_id"].get(fld)
        row["total_sales"] = doc["total_sales"]
        flattened.append(row)
    df = pl.DataFrame(flattened)
    return df
if __name__ == "__main__":
    # 查询近 28 天内，按货号和 Ozon ID 分组的销量最高值（求和）
    df_sum = summarize_max_sales(
        mongo_uri="mongodb://localhost:27017",
        db_name="ozondatas",
        coll_name="order_info",
        date_field="正在处理中",
        group_fields=["货号", "Ozon ID"],
        agg_field="数量",
        days_back=28,
        summary_mode="sum",   # 用 sum 求总销量
        return_all=False      # 只返回销量最高的分组
    )
    print(df_sum)
    exit()

    # 查询近 28 天内每个 SKU + Ozon ID + 配送集群组合的单次最大销量
    df_max = summarize_max_sales(
        mongo_uri="mongodb://localhost:27017",
        db_name="ozondatas",
        coll_name="order_info",
        date_field="正在处理中",
        group_fields=["货号", "Ozon ID", "配送集群"],
        agg_field="数量",
        days_back=28,
        summary_mode="max",   # 用 max 取单次最大销量
        return_all=True       # 返回所有组合，并按最大销量降序
    )
    print(df_max.head())
