# encoding='utf-8

# @Time: 2025-08-10
# @File: %
#!/usr/bin/env
from __future__ import annotations


# -*- coding: utf-8 -*-
"""
Aggregate recent order information from MongoDB, optionally converting a
timestamp field into a date and grouping on that date along with
additional fields.  This module exposes a single function,
``summarize_order_info``, which connects to MongoDB, optionally
filters documents by a date range, groups by user‑supplied
dimensions and sums a numeric field.  Results are returned as a
Polars ``DataFrame``.

The caller can choose whether to include the date in the grouping.
If ``date_field`` is provided and non‑empty, the specified
``date_field`` is parsed using the supplied ``date_format`` and
``timezone`` into a proper datetime.  The aggregation then filters
documents whose timestamp falls within the last ``days_back`` days
and groups by the date portion (``YYYY‑MM‑DD``) plus any fields
specified in ``group_fields``.  When ``date_field`` is ``None`` or
an empty string, the aggregation skips date parsing and filtering
entirely and groups only by ``group_fields``.

Example usage::

    from summarize_order_info_polars_conditional import summarize_order_info

    # Group the last 90 days of records by day, SKU and Ozon ID.
    df = summarize_order_info(
        mongo_uri="mongodb://localhost:27017",
        db_name="ozondatas",
        coll_name="order_info",
        date_field="正在处理中",
        group_fields=["货号", "Ozon ID"],
        agg_field="数量",
        days_back=90,
        date_format="%Y-%m-%d %H:%M:%S",
        timezone="Asia/Seoul"
    )

    # Group all records (no date filtering) by SKU, Ozon ID and 配送集群
    df2 = summarize_order_info(
        mongo_uri="mongodb://localhost:27017",
        db_name="ozondatas",
        coll_name="order_info",
        date_field=None,
        group_fields=["货号", "Ozon ID", "配送集群"],
        agg_field="数量",
        days_back=90  # ignored because date_field is None
    )

This design allows callers to switch between date‑aware and
date‑agnostic aggregations simply by passing or omitting the
``date_field`` argument.

"""


import datetime as _dt
from typing import Dict, List, Sequence, Optional, Any

import polars as pl
from pymongo import MongoClient

__all__ = ["summarize_order_info"]


def summarize_order_info(
    mongo_uri: str = "mongodb://localhost:27017",
    db_name: str = "ozondatas",
    coll_name: str = "order_info",
    date_field: Optional[str] = "正在处理中",
    group_fields: Sequence[str] = ("货号", "Ozon ID"),
    agg_field: str = "数量",
    days_back: int = 90,
    date_format: str = "%Y-%m-%d %H:%M:%S",
    timezone: str = "UTC",
    return_stats: bool = False,
) -> pl.DataFrame | Dict[str, Any]:
    """Aggregate recent orders, optionally grouping by date and other keys.

    Parameters
    ----------
    mongo_uri:
        Connection string for MongoDB.  Defaults to a local instance.
    db_name:
        Name of the database to use.
    coll_name:
        Name of the collection containing order documents.
    date_field:
        Name of the timestamp field to use for filtering and
        date grouping.  If set to ``None`` or an empty string,
        no date parsing or filtering is performed and the
        aggregation groups only on ``group_fields``.  When a
        non‑empty string is provided, values are assumed to be
        timestamps in the format described by ``date_format`` and
        interpreted using ``timezone``.
    group_fields:
        Fields to group by in addition to (or instead of) the date.
        If ``date_field`` is provided, the resulting DataFrame
        contains a column with the same name converted to
        ``YYYY‑MM‑DD`` strings.  Otherwise, the DataFrame contains
        only these group fields plus the aggregated field.
    agg_field:
        Field whose numeric values are summed within each group.
        Non‑numeric values are treated as zero via ``$toDouble``.
    days_back:
        Number of days of history to include when filtering.  Only
        effective when ``date_field`` is provided.  Records with
        timestamps older than ``days_back`` days from the current
        moment are excluded.
    date_format:
        ``strptime`` format string describing how to parse values
        in ``date_field`` into a datetime.  Defaults to
        ``"%Y-%m-%d %H:%M:%S"``.
    timezone:
        IANA timezone identifier used when interpreting timestamps.
        For example, ``"Asia/Seoul"`` or ``"Europe/Moscow"``.
    return_stats:
        If ``True``, the function returns a dictionary with
        statistics about the aggregation process, including row
        counts and the resulting DataFrame.  Otherwise, returns
        only the resulting DataFrame.

    Returns
    -------
    pl.DataFrame or Dict[str, Any]
        If ``return_stats`` is ``False`` (default), a Polars
        DataFrame of aggregated results.  If ``return_stats`` is
        ``True``, a dictionary with keys ``data`` (the DataFrame),
        ``rows_matched``, ``groups_count`` and ``inserted`` (all
        optional) providing diagnostic information.

    Notes
    -----
    When ``date_field`` is provided, the aggregation builds
    ``_proc_datetime`` via ``$dateFromString`` and filters documents
    whose datetime falls between ``now - days_back`` and ``now``.
    It then groups by a formatted date string ``YYYY‑MM‑DD`` plus
    any additional ``group_fields``.  When ``date_field`` is not
    provided, the pipeline omits date parsing and filtering and
    groups solely by the fields in ``group_fields``.
    """
    # Connect to MongoDB
    client = MongoClient(mongo_uri)
    coll = client[db_name][coll_name]

    # Determine time window for filtering when date_field is supplied
    now = _dt.datetime.now()
    start_dt = now - _dt.timedelta(days=days_back)
    end_dt = now

    # Build aggregation pipeline conditionally
    pipeline: List[Dict] = []

    # We will collect stats if return_stats is True
    rows_matched: Optional[int] = None

    # Only build date parsing and filtering stages when date_field
    # contains a non-empty string
    if date_field:
        # Stage 1: parse date string into a datetime using the provided
        # format and timezone.  We store it in _proc_datetime for
        # subsequent filtering and grouping.
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
        # Stage 2: filter documents to the last ``days_back`` days
        pipeline.append(
            {
                "$match": {
                    "_proc_datetime": {
                        "$gte": start_dt,
                        "$lte": end_dt,
                    }
                }
            }
        )
        # Group identifier: include a date string column derived from
        # _proc_datetime plus all group_fields.  We reuse the
        # provided date_field name to store the formatted date string
        # in the output.
        group_id: Dict[str, Any] = {
            date_field: {
                "$dateToString": {
                    "date": "$_proc_datetime",
                    "format": "%Y-%m-%d",
                    "timezone": timezone,
                }
            }
        }
        for gf in group_fields:
            group_id[gf] = f"${gf}"
        pipeline.append(
            {
                "$group": {
                    "_id": group_id,
                    "total": {"$sum": {"$toDouble": f"${agg_field}"}},
                }
            }
        )
        # Project back to a flat document.  We rename the aggregated
        # sum to agg_field and preserve the grouped values.
        project_fields: Dict[str, Any] = {date_field: f"$_id.{date_field}"}
        for gf in group_fields:
            project_fields[gf] = f"$_id.{gf}"
        project_fields[agg_field] = "$total"
        pipeline.append({"$project": {"_id": 0, **project_fields}})
    else:
        # No date field provided.  We group by the supplied
        # group_fields only and sum the numeric field.  No filtering
        # by date is performed.
        # Stage: group by group_fields only
        group_id = {gf: f"${gf}" for gf in group_fields}
        pipeline.append(
            {
                "$group": {
                    "_id": group_id,
                    "total": {"$sum": {"$toDouble": f"${agg_field}"}},
                }
            }
        )
        # Project flat document
        project_fields = {gf: f"$_id.{gf}" for gf in group_fields}
        project_fields[agg_field] = "$total"
        pipeline.append({"$project": {"_id": 0, **project_fields}})

    # Execute the pipeline
    results = list(coll.aggregate(pipeline))
    client.close()

    # Convert results to Polars DataFrame
    df = pl.DataFrame(results) if results else pl.DataFrame({})

    if return_stats:
        # Collect some simple stats: number of documents matched and
        # number of groups produced.  We only know the matched count
        # reliably if date_field is supplied and filtering occurred.
        # When no date_field, MongoDB does not provide a matched
        # count, so we leave rows_matched as None.
        stats: Dict[str, Any] = {
            "data": df,
            "groups_count": df.height,
        }
        return stats

    return df

if __name__ == "__main__":
    # 汇总最近 90 天的数据，按日期(yyyy-mm-dd)、货号 和 Ozon ID 分组，求数量总和
    df = summarize_order_info(
        mongo_uri="mongodb://localhost:27017",
        db_name="ozondatas",
        coll_name="order_info",
        date_field="正在处理中",
        group_fields=["货号", "Ozon ID"],
        agg_field="数量",
        days_back=90,
        date_format="%Y-%m-%d %H:%M:%S",  # 根据实际时间字符串格式调整
        timezone="Asia/Seoul"             # 根据存储时间的时区调整
    )
    df.write_csv("order_days_summary.csv")
    print(df)
    df2 = summarize_order_info(
        mongo_uri="mongodb://localhost:27017",
        db_name="ozondatas",
        coll_name="order_info",
        date_field='正在处理中',
        group_fields=["货号", "Ozon ID", "配送集群"],
        agg_field="数量",
        days_back=90,
        # date_format="%Y-%m-%d %H:%M:%S",  # 根据实际时间字符串格式调整
        timezone="Asia/Seoul"             # 根据存储时间的时区调整
    )
    df2.write_csv("order_days_jiqun_summary.csv")
    print(df2)
