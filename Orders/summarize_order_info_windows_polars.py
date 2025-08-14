# -*- coding: utf-8 -*-
"""
Aggregate order quantities over multiple lookback windows for one or more
grouping schemas.  This module defines a helper function,
``summarize_order_windows``, which, given a reference date and a list of
time windows (in days), computes the sum of a numeric field for each
window and grouping of interest.  All operations are performed via
MongoDB's aggregation framework to minimise data transfer, and the
results are returned as a Polars ``DataFrame``.

Two typical use cases supported by this script are:

* Summarising by ``货号`` (SKU) and ``Ozon ID`` across multiple
  time windows; and
* Summarising by ``货号``, ``Ozon ID`` and ``配送集群`` (delivery
  cluster) across multiple windows.

The caller can specify an arbitrary ``target_date`` (as a
``YYYY-MM-DD`` string).  For each ``window`` in ``windows`` the
function will sum all records where the timestamp field falls
between ``target_date - (window - 1)`` and ``target_date``, inclusive.

Example usage::

    from summarize_order_info_windows_polars import summarize_order_windows

    # Summarise by SKU and Ozon ID over 7,14,28,60,90 day windows
    df1 = summarize_order_windows(
        target_date="2025-08-10",
        mongo_uri="mongodb://localhost:27017",
        db_name="ozondatas",
        coll_name="order_info",
        date_field="正在处理中",
        group_fields=["货号", "Ozon ID"],
        agg_field="数量",
        windows=[7,14,28,60,90],
        date_format="%Y-%m-%d %H:%M:%S",
        timezone="Asia/Seoul"
    )
    print(df1.head())

    # Summarise by SKU, Ozon ID and delivery cluster over the same windows
    df2 = summarize_order_windows(
        target_date="2025-08-10",
        mongo_uri="mongodb://localhost:27017",
        db_name="ozondatas",
        coll_name="order_info",
        date_field="正在处理中",
        group_fields=["货号", "Ozon ID", "配送集群"],
        agg_field="数量",
        windows=[7,14,28,60,90],
        date_format="%Y-%m-%d %H:%M:%S",
        timezone="Asia/Seoul"
    )
    print(df2.head())

The implementation relies on MongoDB's ``$dateFromString`` to parse
timestamps and ``$cond`` expressions within a ``$group`` stage to
compute each window's sum in a single pass.  The final results are
returned as a Polars DataFrame for further analysis.

"""

from __future__ import annotations

import datetime as _dt
from typing import Dict, List, Sequence, Optional, Any

from zoneinfo import ZoneInfo
import polars as pl
from pymongo import MongoClient

__all__ = ["summarize_order_windows"]


def summarize_order_windows(
    *,
    target_date: str,
    mongo_uri: str = "mongodb://localhost:27017",
    db_name: str = "ozondatas",
    coll_name: str = "order_info",
    date_field: str = "正在处理中",
    group_fields: Sequence[str] = ("货号", "Ozon ID"),
    agg_field: str = "数量",
    windows: Sequence[int] = (7, 14, 28, 60, 90),
    date_format: str = "%Y-%m-%d %H:%M:%S",
    timezone: str = "Asia/Seoul",
    return_stats: bool = False,
) -> pl.DataFrame | Dict[str, Any]:
    """Aggregate order quantities over multiple lookback windows.

    Parameters
    ----------
    target_date:
        Reference date in ``YYYY-MM-DD`` format.  For each window
        ``w``, the function sums records whose timestamp lies between
        ``target_date - (w - 1)`` and ``target_date`` (inclusive of the
        entire day of ``target_date``).  Internally this is
        implemented as a half‑open interval
        ``[target_date - (w - 1), target_date + 1 day)`` so that
        records at 23:59 on the reference day are included.
    mongo_uri:
        Connection URI for MongoDB.  Defaults to a local instance.
    db_name:
        Database name containing the collection.
    coll_name:
        Collection name containing order documents.
    date_field:
        Field storing the processing timestamp as a string.  The
        function uses ``date_format`` and ``timezone`` to parse this
        field via MongoDB's ``$dateFromString``.  Records with
        unparseable dates are ignored.
    group_fields:
        One or more fields to group by.  A row will be produced for
        each unique combination of these fields.
    agg_field:
        Name of the numeric field to sum.  Values are coerced to
        double via ``$toDouble``.
    windows:
        Sequence of integers representing lookback windows in days.
        The function will compute a sum for each window.  Each window
        covers the inclusive range from ``target_date - (w - 1)``
        through ``target_date`` (for example, a 7‑day window
        anchored on 2025‑08‑07 aggregates data from 2025‑08‑01
        through 2025‑08‑07, inclusive).  By default it uses
        ``(7, 14, 28, 60, 90)``.
    date_format:
        Format string describing how to parse ``date_field`` into a
        datetime.  Defaults to ``"%Y-%m-%d %H:%M:%S"``.  Adjust this
        if your timestamp strings use a different format.
    timezone:
        IANA timezone identifier used to interpret both the
        timestamp strings and the provided ``target_date``.  For
        example, ``"Asia/Seoul"`` or ``"Europe/Moscow"``.
    return_stats:
        If ``True``, return a dict containing the resulting DataFrame
        and diagnostic counts (number of groups).  Otherwise only
        return the DataFrame.

    Returns
    -------
    pl.DataFrame or Dict[str, Any]
        DataFrame with columns equal to ``group_fields`` followed by
        ``sum_<window>`` for each window.  If ``return_stats`` is
        ``True``, the function returns a dict with keys ``data`` and
        ``groups_count``.
    """
    if not windows:
        raise ValueError("windows must be a non-empty sequence of positive integers")

    # Convert the target date to a timezone-aware datetime.  We
    # interpret the given date as midnight in the specified timezone.
    try:
        base_date = _dt.datetime.fromisoformat(target_date)
    except Exception as exc:
        raise ValueError(f"Invalid target_date {target_date!r}: {exc}") from exc
    # Attach timezone information
    tz = ZoneInfo(timezone)
    target_dt = base_date.replace(tzinfo=tz)

    # Compute start and end datetimes for the largest window.  We
    # subtract (max_window - 1) days to include the target date itself
    # and add one day to the end to include the entire day of
    # ``target_date``.  Without this adjustment, a target date at
    # 00:00 would exclude the majority of that day's records.  See
    # user request for inclusive ranges (e.g. 2025-08-01 to 2025-08-07).
    max_window = max(windows)
    start_dt_max = target_dt - _dt.timedelta(days=max_window - 1)
    # Add one day to include the full target date.  MongoDB will
    # compare using < end_dt_exclusive, so that all times up to
    # 23:59:59.999 are included.
    end_dt_exclusive = target_dt + _dt.timedelta(days=1)

    # Build the $group expression with conditional sums for each window.
    # We precompute the start datetime for each window.
    window_start_dates: Dict[int, _dt.datetime] = {
        w: target_dt - _dt.timedelta(days=w - 1) for w in windows
    }
    # In the group stage we will build sum expressions keyed by
    # e.g. "sum_7", "sum_14", ...  We also include sum of the
    # largest window implicitly (90 days) in case it helps debug.
    sum_exprs: Dict[str, Any] = {}
    for w in windows:
        field_name = f"sum_{w}"
        # Each sum is conditionally accumulated: if _proc_datetime >=
        # start_dt_w then we add toDouble(agg_field), else we add 0.
        # Note that records outside the max window will already be
        # filtered by the $match stage.
        sum_exprs[field_name] = {
            "$sum": {
                "$cond": [
                    {"$gte": ["$_proc_datetime", window_start_dates[w]]},
                    {"$toDouble": f"${agg_field}"},
                    0
                ]
            }
        }

    # Build the aggregation pipeline.
    pipeline: List[Dict] = []
    # Stage 1: parse the date string into a datetime using the
    # provided format and timezone.  Unparseable strings yield null.
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
    # Stage 2: restrict to records within the maximum window.  This
    # dramatically reduces the number of documents flowing into the
    # group stage.  Any records with null _proc_datetime will be
    # filtered out.  We use a half‑open interval [start_dt_max,
    # end_dt_exclusive) so that the entire day of the target_date is
    # included.  Without adding a day to end_dt_exclusive, a target
    # date at 00:00 would exclude records later on that same date.
    pipeline.append(
        {
            "$match": {
                "_proc_datetime": {
                    "$gte": start_dt_max,
                    "$lt": end_dt_exclusive,
                }
            }
        }
    )
    print(f"Processing records from {start_dt_max} to {end_dt_exclusive} "
          f"for target date {target_date} with windows {windows}")
    # Stage 3: group by the specified fields and compute sums for
    # each window.  Build the _id document from the group fields.
    group_id: Dict[str, str] = {gf: f"${gf}" for gf in group_fields}
    group_stage: Dict[str, Any] = {
        "_id": group_id,
    }
    group_stage.update(sum_exprs)
    pipeline.append({"$group": group_stage})
    # Stage 4: project the result into a flat document.  Copy each
    # group field from the _id document back to the top level and
    # include the sum fields.  Do not include the _id field.
    project_fields: Dict[str, Any] = {gf: f"$_id.{gf}" for gf in group_fields}
    for w in windows:
        project_fields[f"sum_{w}"] = f"$sum_{w}"
    pipeline.append({"$project": {"_id": 0, **project_fields}})

    # Connect to MongoDB and execute the aggregation
    client = MongoClient(mongo_uri)
    coll = client[db_name][coll_name]
    results = list(coll.aggregate(pipeline))
    client.close()

    # Convert results to Polars DataFrame
    df = pl.DataFrame(results) if results else pl.DataFrame({})
    if return_stats:
        return {"data": df, "groups_count": df.height}
    return df




if __name__ == "__main__":
    exit()
    # Example usage
    from calculate_daily_weight_polars import calculate_weighted_daily_sales, calculate_dynamic_daily_sales
    # 从mongodb中按货号和Ozon ID聚合订单信息，并计算窗口销量
    df1 = summarize_order_windows(
        target_date="2025-08-07",
        mongo_uri="mongodb://localhost:27017",
        db_name="ozondatas",
        coll_name="order_info",
        date_field="正在处理中",
        group_fields=["货号", "Ozon ID"],
        agg_field="数量",
        windows=[7, 14, 28, 60, 90],
        # windows=[7],
        date_format="%Y-%m-%d %H:%M:%S",
        timezone="Asia/Seoul"
    )
    # df 是包含 sum_7、sum_14、... 等列的数据框，按 Ozon ID 聚合各 SKU 各窗口销量占比权重 后再计算动态日销量
    df_daily = calculate_dynamic_daily_sales(
        df1,
        windows=[7, 14, 28, 60, 90],
        group_fields=['Ozon ID'],
        top_k=3, #取多少个销量靠前的窗口
    )
    print(df_daily)
    df_daily.write_csv("a.csv")

    # exit()
    # 从mongodb中按货号、Ozon ID和配送集群聚合订单信息，并计算窗口销量
    
    df2 = summarize_order_windows(
        target_date="2025-08-07",
        mongo_uri="mongodb://localhost:27017",
        db_name="ozondatas",
        coll_name="order_info",
        date_field="正在处理中",
        group_fields=["货号", "Ozon ID", "配送集群"],
        agg_field="数量",
        windows=[7, 14, 28, 60, 90],
        # windows=[7],
        date_format="%Y-%m-%d %H:%M:%S",
        timezone="Asia/Seoul"
    )
    # print(df2.head())
    # df2.write_csv("order_summary_window_by_sku_ozon_id_and_delivery_cluster.csv")

    # df 是包含 sum_7、sum_14、... 等列的数据框，按 Ozon ID 聚合各 SKU 各窗口销量占比权重后再计算动态日销量
    df_daily2 = calculate_dynamic_daily_sales(
        df2,
        windows=[7, 14, 28, 60, 90],
        group_fields=['Ozon ID', '配送集群'],
        top_k=3 # 取多少个销量靠前的窗口
    )
    print(df_daily2)
    df_daily2.write_csv("b.csv")
