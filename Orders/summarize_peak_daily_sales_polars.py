# -*- coding: utf-8 -*-
"""
Summarise peak daily sales within a lookback window from MongoDB.

This module defines a helper function ``summarize_peak_daily_sales`` that
connects to a MongoDB collection, filters records within a recent window
(e.g. the last 28 days), groups orders by a set of business keys and
calendar day, sums the sales per day for each group, and then computes
the maximum of these daily sums.  The result is a Polars DataFrame
containing each unique combination of grouping fields along with its
``max_daily_sales`` – the highest single‐day total within the lookback
period.

Unlike a simple ``sum`` or ``max`` across the entire window, this
function first aggregates sales on a per‐day basis.  It is useful for
identifying the busiest day for each SKU, Ozon ID, or any other set of
fields.  Internally it uses MongoDB's aggregation pipeline to push
date parsing, filtering, grouping and maximum calculation onto the
database server.  The final results are returned as a Polars
DataFrame for further analysis in Python.

Example usage::

    from summarize_peak_daily_sales_polars import summarize_peak_daily_sales

    # Find the peak daily sales for each SKU in the last 28 days
    df = summarize_peak_daily_sales(
        mongo_uri="mongodb://localhost:27017",
        db_name="ozondatas",
        coll_name="order_info",
        date_field="正在处理中",
        group_fields=["货号"],
        agg_field="数量",
        days_back=28,
        date_format="%Y-%m-%d %H:%M:%S",
        timezone="Asia/Seoul"
    )
    print(df)

    # Find the peak daily sales for each SKU and Ozon ID in the last 14 days
    df2 = summarize_peak_daily_sales(
        mongo_uri="mongodb://localhost:27017",
        db_name="ozondatas",
        coll_name="order_info",
        date_field="正在处理中",
        group_fields=["货号", "Ozon ID"],
        agg_field="数量",
        days_back=14,
        timezone="Europe/Moscow"
    )
    print(df2)

The lookback range is inclusive of the starting day and includes the
entire final day (i.e. [target_date-(days_back-1), target_date] in
the specified timezone).  The ``date_field`` should contain a
timestamp in the provided ``date_format``; timestamps are parsed
using ``$dateFromString`` and truncated to date strings when
grouping per day.  Non‐numeric values of ``agg_field`` are coerced
to doubles before summation.  For more details about the $max
accumulator used to derive the peak value see the MongoDB
documentation, which notes that ``$max`` returns the highest
expression value for each group【659827914736873†L535-L538】 while
``$sum`` adds numeric values and ignores non‐numeric inputs【659827914736873†L616-L620】.
"""

from __future__ import annotations

import datetime as _dt
from typing import Sequence, List, Dict, Any

from zoneinfo import ZoneInfo
import polars as pl
from pymongo import MongoClient

__all__ = ["summarize_peak_daily_sales"]


def summarize_peak_daily_sales(
    *,
    mongo_uri: str = "mongodb://localhost:27017",
    db_name: str = "ozondatas",
    coll_name: str = "order_info",
    date_field: str = "正在处理中",
    group_fields: Sequence[str] = ("货号",),
    agg_field: str = "数量",
    days_back: int = 28,
    date_format: str = "%Y-%m-%d %H:%M:%S",
    timezone: str = "Asia/Seoul",
    return_all: bool = True,
    target_date: str | None = None,
) -> pl.DataFrame:
    """Compute the peak daily sales for each group within a recent window.

    For each unique combination of the ``group_fields`` values, this
    function calculates the sum of ``agg_field`` for each day in the
    lookback window and then returns the maximum of these daily sums.

    Parameters
    ----------
    mongo_uri:
        MongoDB connection URI.  Defaults to ``mongodb://localhost:27017``.
    db_name:
        Name of the database containing the target collection.
    coll_name:
        Name of the collection with order documents.
    date_field:
        Name of the timestamp field (string) representing the processing
        time of each order.  Values are parsed using ``date_format`` and
        interpreted according to ``timezone``.
    group_fields:
        List of field names that define the grouping key.  Peak values
        are computed separately for each distinct combination of these
        fields.
    agg_field:
        Name of the numeric field to be summed per day.  Non‐numeric
        inputs are coerced to doubles.
    days_back:
        Length of the lookback window (in days).  A value of ``28``
        includes today and the previous 27 days.  Must be positive.
    date_format:
        Format string used to parse ``date_field`` values (see
        :class:`datetime.datetime.strptime`).  Defaults to ``"%Y-%m-%d %H:%M:%S"``.
    timezone:
        IANA timezone name used for interpreting ``date_field`` values
        and computing the lookback window.  Common examples include
        ``"Asia/Seoul"`` or ``"Europe/Moscow"``.
    return_all:
        If ``True`` (default), return peak values for all groups
        sorted descending.  If ``False``, return only the single group
        with the highest peak daily sales.
    target_date:
        Optional anchor date (``YYYY-MM-DD`` or ISO timestamp) used
        as the reference day for the lookback window.  If provided,
        the lookback period ends at ``target_date`` (inclusive) and
        begins ``days_back-1`` days earlier.  If omitted, the
        current date/time in ``timezone`` is used.

    Returns
    -------
    pl.DataFrame
        A Polars DataFrame with one row per group (or one row if
        ``return_all`` is ``False``) and columns for each grouping
        field plus ``max_daily_sales``.

    Notes
    -----
    The lookback window is inclusive on the lower bound and exclusive
    on the upper bound.  Specifically, if ``days_back`` is 28,
    records whose ``date_field`` falls between ``(now - 27 days)``
    (inclusive) and the start of tomorrow (exclusive) are included.
    The maximum value is determined via the ``$max`` accumulator in
    MongoDB, which returns the highest expression value per group
    【659827914736873†L535-L538】.  Summation of daily quantities uses
    ``$sum``, which adds numeric values and ignores non‐numeric
    values【659827914736873†L616-L620】.
    """
    if days_back <= 0:
        raise ValueError("days_back must be a positive integer")

    # Connect to MongoDB
    client = MongoClient(mongo_uri)
    coll = client[db_name][coll_name]

    # Determine the anchor date for the lookback window.
    # If target_date is provided (e.g. '2025-08-07'), interpret it in the
    # specified timezone.  Otherwise, use the current date/time.
    now = _dt.datetime.now(ZoneInfo(timezone))
    if target_date:
        try:
            # Attempt to parse as ISO datetime or date.  If only a date is
            # provided, fromisoformat returns a naive datetime at 00:00.
            parsed_dt = _dt.datetime.fromisoformat(target_date)
        except ValueError:
            # Fallback: attempt to parse as date only
            parsed_dt = _dt.datetime.combine(_dt.date.fromisoformat(target_date), _dt.time(0, 0))
        # Attach timezone information if absent
        if parsed_dt.tzinfo is None:
            target_dt = parsed_dt.replace(tzinfo=ZoneInfo(timezone))
        else:
            target_dt = parsed_dt.astimezone(ZoneInfo(timezone))
    else:
        target_dt = now
    # Compute the inclusive start of the window and the exclusive end.
    # The window covers [target_dt - (days_back - 1) days, target_dt]
    # inclusive, so we subtract days_back - 1 for the start.  The end is
    # the beginning of the day following target_dt.
    start_dt = (target_dt - _dt.timedelta(days=days_back - 1)).replace(hour=0, minute=0, second=0, microsecond=0)
    end_dt_exclusive = (target_dt + _dt.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    # print(f"Lookback window: {start_dt} to {end_dt_exclusive} (exclusive)")

    # Build aggregation pipeline
    pipeline: List[Dict[str, Any]] = []
    # Stage 1: parse date strings into datetime objects
    pipeline.append({
        "$addFields": {
            "_proc_datetime": {
                "$dateFromString": {
                    "dateString": f"${date_field}",
                    "format": date_format,
                    "timezone": timezone,
                }
            }
        }
    })
    # Stage 2: filter records within the lookback window
    pipeline.append({
        "$match": {
            "_proc_datetime": {
                "$gte": start_dt,
                "$lt": end_dt_exclusive,
            }
        }
    })
    # Stage 3: group by group_fields and day, summing agg_field per day
    # Create _id for first grouping: combine original fields and a date string for the day
    first_id: Dict[str, Any] = {fld: f"${fld}" for fld in group_fields}
    # Add day field derived from _proc_datetime
    first_id["day"] = {
        "$dateToString": {
            "date": "$_proc_datetime",
            "format": "%Y-%m-%d",
            "timezone": timezone,
        }
    }
    pipeline.append({
        "$group": {
            "_id": first_id,
            "daily_sum": {"$sum": {"$toDouble": f"${agg_field}"}},
        }
    })
    # Stage 4: group again by only group_fields, taking max of daily_sum
    second_id = {fld: f"$_id.{fld}" for fld in group_fields}
    pipeline.append({
        "$group": {
            "_id": second_id,
            "max_daily_sales": {"$max": "$daily_sum"},
        }
    })
    # Stage 5: sort descending by max_daily_sales
    pipeline.append({"$sort": {"max_daily_sales": -1}})
    # Stage 6: limit results if not returning all
    if not return_all:
        pipeline.append({"$limit": 1})

    # Execute aggregation
    results = list(coll.aggregate(pipeline))
    client.close()

    # Convert to Polars DataFrame
    if not results:
        # Create DataFrame with appropriate columns but no rows
        cols = list(group_fields) + ["max_daily_sales"]
        return pl.DataFrame({col: [] for col in cols})

    # Flatten _id and build list of dicts
    flattened: List[Dict[str, Any]] = []
    for doc in results:
        row: Dict[str, Any] = {}
        for fld in group_fields:
            row[fld] = doc["_id"].get(fld)
        row["max_daily_sales"] = doc["max_daily_sales"]
        flattened.append(row)
    df = pl.DataFrame(flattened)
    if 'max_daily_sales' in df.columns:
        df = df.with_columns(pl.col("max_daily_sales").fill_null(0).cast(pl.Int64))
    return df

if __name__ == "__main__":
    exit()

    # 指定结束日期为 2025-08-07，向前 7 天
    df_peak = summarize_peak_daily_sales(
        mongo_uri="mongodb://localhost:27017",
        db_name="ozondatas",
        coll_name="order_info",
        date_field="正在处理中",
        group_fields=["货号"],
        agg_field="数量",
        days_back=28,
        target_date="2025-08-07",
        timezone="Asia/Seoul"
    )
    print(df_peak)
