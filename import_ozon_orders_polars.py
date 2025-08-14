"""
High‑throughput CSV import into MongoDB using Polars.

This module exposes a single function, ``import_ozon_orders_polars``, that can
read an Ozon order CSV, perform optional date filtering and deduplication,
compute a deterministic MD5 hash across selected key columns and bulk insert
new records into a MongoDB collection.  The implementation is tuned for
performance: it uses Polars' lazy API to push filters down to the CSV
scanner, batches inserts to reduce round‑trips, and avoids building
multi‑megabyte queries that trigger MongoDB's BSON size limit.  A partial
unique index is created on the hash field to enforce uniqueness without
interfering with legacy documents where the hash field is missing or null.

Key features
------------

* **Date filtering**: you can choose to import only recent days, a fixed
  date range, or an explicit list of dates.  Dates are parsed from the
  specified ``date_field`` column.  Filtering is applied lazily to avoid
  loading the full CSV when only a subset of rows are needed.

* **MD5 deduplication**: by default the hash is computed from the four
  business keys ``订单号码``, ``发货号码``, ``正在处理中`` and ``货号`` (with
  alias fallback for different column names).  You can override this list
  via ``hash_cols``.  The computed hash is stored in the Mongo document
  under ``hash_field`` (default ``_row_md5``), and a partial unique index is
  maintained on that field.  Existing hashes are looked up in chunks to
  avoid the BSON document size limitation.

* **Preservation of column order**: the documents inserted into MongoDB
  preserve the original column ordering from the CSV.  The hash field is
  appended as the last key.

* **Performance**: batch inserts and chunked duplicate checks reduce
  overhead and keep memory usage bounded.  The function returns the
  number of new documents inserted.

Example usage::

    from import_ozon_orders_polars_fast import import_ozon_orders_polars

    # import everything
    n = import_ozon_orders_polars("orders.csv")
    print(f"Inserted {n} new documents")

    # import just yesterday and the day before
    n = import_ozon_orders_polars("orders.csv", days_back=2)
    print(f"Inserted {n} new documents in the last two days")

    # import a fixed date range
    n = import_ozon_orders_polars(
        "orders.csv", start_date="2025-06-01", end_date="2025-06-10")
    print(f"Inserted {n} new documents in the date window")

    # import only specific dates
    n = import_ozon_orders_polars(
        "orders.csv", include_dates=["2025-06-01", "2025-06-03"])
    print(f"Inserted {n} new documents for specific dates")

Note that you must have a running MongoDB instance accessible via
``mongo_uri``.  The default connection string points at localhost.
"""

from __future__ import annotations

import datetime as _dt
import hashlib
from typing import Iterable, List, Optional, Sequence, Tuple, Dict, Any

import polars as pl
from pymongo import MongoClient, errors

__all__ = ["import_ozon_orders_polars"]


# Mapping of business key groups to possible aliases.  When ``hash_cols`` is
# ``None``, the function will attempt to locate these columns in the CSV by
# testing each list of aliases in order.  The first match found for each
# group is used.  If a required group cannot be resolved, an exception is
# raised.  You can pass your own list of column names via ``hash_cols`` to
# bypass this lookup entirely.
_DEFAULT_HASH_ALIAS: Dict[str, List[str]] = {
    "订单号码": ["订单号码", "订单号", "订单编号", "订单ID", "订单No", "order", "order id"],
    "发货号码": ["发货号码", "发货号", "发运号码", "运单号", "物流号", "shipment", "tracking id"],
    "正在处理中": ["正在处理中", "创建时间", "下单时间", "处理时间", "订单时间", "processing time"],
    "货号": ["货号", "SKU", "sku", "货品编码", "商品编码", "product code"],
}


def _resolve_hash_columns(
    columns: Sequence[str],
    hash_cols: Optional[Sequence[str]] = None,
) -> List[str]:
    """Resolve which columns to use for MD5 based on the provided list or
    alias mapping.

    Parameters
    ----------
    columns:
        The list of column names from the CSV.
    hash_cols:
        Optional explicit list of columns to use for MD5.  If provided,
        these names must exist exactly in ``columns``; otherwise a
        ``KeyError`` is raised.  If ``None``, the function will attempt to
        match each default group by checking its aliases.

    Returns
    -------
    list[str]
        The resolved list of column names.

    Raises
    ------
    KeyError
        If a required column cannot be resolved.
    """
    resolved: List[str] = []
    columns_set = {c.lower(): c for c in columns}
    if hash_cols:
        # Validate explicit columns
        missing = [c for c in hash_cols if c not in columns]
        if missing:
            raise KeyError(f"Missing hash columns: {missing!r}")
        return list(hash_cols)
    # Attempt alias resolution
    for group, aliases in _DEFAULT_HASH_ALIAS.items():
        found: Optional[str] = None
        for alias in aliases:
            key = alias.lower()
            if key in columns_set:
                found = columns_set[key]
                break
        if not found:
            raise KeyError(f"无法在CSV列中找到用于MD5的列: {group}")
        resolved.append(found)
    return resolved


def _parse_date_list(date_strs: Iterable[str]) -> List[_dt.date]:
    """Parse a sequence of ``YYYY-MM-DD`` strings into ``date`` objects.

    Invalid or empty strings are ignored.
    """
    dates: List[_dt.date] = []
    for s in date_strs:
        if not s:
            continue
        try:
            dates.append(_dt.date.fromisoformat(str(s)))
        except ValueError:
            # Ignore badly formatted dates; let upstream handle if needed
            continue
    return dates


def import_ozon_orders_polars(
    csv_path: str,
    *,
    mongo_uri: str = "mongodb://localhost:27017",
    db_name: str = "ozondatas",
    coll_name: str = "order_info",
    date_field: str = "正在处理中",
    days_back: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    include_dates: Optional[Sequence[str]] = None,
    hash_cols: Optional[Sequence[str]] = None,
    hash_field: str = "_row_md5",
    batch_size: int = 20000,
    dedup_chunk_size: int = 5000,
    return_stats: bool = False,
) -> int | Dict[str, Any]:
    """Import an Ozon orders CSV into MongoDB with deduplication.

    Parameters
    ----------
    csv_path:
        Path to the semicolon‑separated CSV file.  The file is expected to
        have a header row.  All data rows will be read lazily to enable
        pushdown filtering.
    mongo_uri:
        Connection string for MongoDB.  Defaults to a local instance.
    db_name:
        Name of the target database.
    coll_name:
        Name of the target collection.
    date_field:
        Name of the column containing the date or datetime used for
        filtering.  The function attempts to parse this column to
        ``datetime`` and then extracts the date component for comparison.
    days_back:
        If provided, import only the last ``days_back`` days excluding
        today.  For example, ``days_back=1`` imports only yesterday.  A
        value of ``2`` imports yesterday and the day before yesterday.  If
        supplied alongside ``start_date``/``end_date`` or ``include_dates``,
        it is ignored.
    start_date, end_date:
        Inclusive start and end dates in ``YYYY-MM-DD`` format.  When both
        are provided, import only rows whose date falls within the range.
        These parameters override ``days_back``.  If only one of them is
        provided, it is ignored.
    include_dates:
        A list of exact dates (``YYYY-MM-DD``) to include.  When present,
        only rows matching these dates are imported.  This parameter takes
        precedence over ``days_back`` and the start/end window.
    hash_cols:
        Explicit list of column names to join together for the MD5 hash.
        If omitted, the function attempts to resolve ``订单号码``, ``发货号码``,
        ``正在处理中`` and ``货号`` via the alias mapping.  Aliases are
        case‑insensitive.  Passing this parameter disables the alias
        resolution.
    hash_field:
        Name of the field in the Mongo document where the MD5 hash will be
        stored.
    batch_size:
        Number of documents to insert per batch.  Larger batches reduce
        round‑trips but require more memory.  Adjust based on your
        environment.
    dedup_chunk_size:
        When checking for existing hashes, how many hash values to include in
        each `$in` query.  MongoDB has a BSON document size limit of 16MB;
        large ``$in`` lists can exceed this limit.  Adjust this to a value
        that comfortably fits within your server's limits.

    Returns
    -------
    int
        The number of newly inserted documents.

    Raises
    ------
    KeyError
        If any of the required hash columns cannot be resolved.
    """
    # ------------------------------------------------------------------
    # Stats initialisation
    # ------------------------------------------------------------------
    # When return_stats is True we compute the total number of rows in the
    # CSV prior to filtering.  Counting is done via a lazy scan so that
    # memory usage remains bounded.  If any error occurs during
    # counting, the value will be left as None.
    rows_read: Optional[int] = None
    if return_stats:
        try:
            # Use a minimal schema inference to scan the file and count
            # the number of rows.  The count aggregator returns a one‑row
            # DataFrame; extract the value at position (0,0).
            count_df = (
                pl.scan_csv(
                    csv_path,
                    separator=';',
                    has_header=True,
                    skip_rows=0,
                    infer_schema_length=0,
                ).select(pl.count())
            ).collect()
            rows_read = int(count_df[0, 0])
        except Exception:
            # If counting fails (e.g. due to unreadable file), leave as None
            rows_read = None
    # ---------------------------------------------------------------------
    # 1. Determine date filters
    # ---------------------------------------------------------------------
    include_dates_parsed: List[_dt.date] = []
    start_dt: Optional[_dt.date] = None
    end_dt: Optional[_dt.date] = None
    # Precedence: include_dates > start/end > days_back
    if include_dates:
        include_dates_parsed = _parse_date_list(include_dates)
    elif start_date and end_date:
        try:
            start_dt = _dt.date.fromisoformat(str(start_date))
            end_dt = _dt.date.fromisoformat(str(end_date))
        except ValueError:
            # Invalid date strings; ignore window
            start_dt = None
            end_dt = None
    elif days_back and days_back > 0:
        today = _dt.date.today()
        # Exclude today: days_back=1 means yesterday only
        start_dt = today - _dt.timedelta(days=days_back)
        end_dt = today - _dt.timedelta(days=1)

    # ---------------------------------------------------------------------
    # 2. Lazily read the CSV and push down date filtering
    # ---------------------------------------------------------------------
    # We allow Polars to infer the schema.  Using scan_csv avoids loading
    # the entire file into memory when we only need a subset of rows.
    lf = pl.scan_csv(
        csv_path,
        separator=';',
        has_header=True,
        skip_rows=0,
        infer_schema_length=0,  # read entire file for schema inference
    )
    # Resolve the columns now without fully collecting the data.  This
    # triggers a schema evaluation but not a complete read of the CSV.
    columns = lf.collect_schema().names()
    if date_field not in columns:
        raise KeyError(f"日期字段 {date_field!r} 在CSV中不存在；现有列为 {columns}")

    # Build a date column for filtering.  Try to parse strings into
    # timestamps, then extract only the date component.  Invalid parses
    # produce nulls, which are handled below.
    lf = lf.with_columns(
        pl.col(date_field)
        .str.strip_chars()
        .str.replace_all('\r', '')
        .str.replace_all('\n', '')
        .str.strptime(pl.Datetime, strict=False, format=None)
        .dt.date()
        .alias("_flt_date")
    )

    # Apply the filter condition if necessary
    if include_dates_parsed:
        lf = lf.filter(pl.col("_flt_date").is_in(include_dates_parsed))
    elif start_dt and end_dt:
        lf = lf.filter((pl.col("_flt_date") >= start_dt) & (pl.col("_flt_date") <= end_dt))
    elif days_back and days_back > 0:
        lf = lf.filter((pl.col("_flt_date") >= start_dt) & (pl.col("_flt_date") <= end_dt))

    # Drop the temporary filter date column and collect the resulting frame
    lf = lf.drop("_flt_date")
    df = lf.collect()

    if df.height == 0:
        # Nothing to insert
        return 0

    # ---------------------------------------------------------------------
    # 3. Determine which columns to join for the hash
    # ---------------------------------------------------------------------
    resolved_hash_cols = _resolve_hash_columns(df.columns, hash_cols)

    # ---------------------------------------------------------------------
    # 4. Compute the hash for each row
    # ---------------------------------------------------------------------
    # Create a concatenated string from the selected columns.  We cast to
    # UTF‑8 and fill nulls with empty strings to ensure a stable input to
    # MD5.  Using a seldom‑used character (ASCII unit separator) as the
    # delimiter prevents accidental collisions if values themselves contain
    # pipes.
    delim = "\x1f"
    df = df.with_columns(
        pl.concat_str([
            pl.col(col).cast(pl.Utf8, strict=False).fill_null("") for col in resolved_hash_cols
        ], separator=delim).alias("__hash_key")
    )
    # Compute MD5 hashes outside of Polars' expression system.  Some
    # distributions of Polars do not expose an ``apply`` method on an
    # expression, leading to an AttributeError.  To maintain broad
    # compatibility and keep performance high, we materialise the
    # intermediate concatenated strings in Python and compute the hash
    # directly.  This also avoids repeated calls into Python when using
    # ``apply`` inside Polars.
    # Obtain the list of concatenated strings
    concat_list = df["__hash_key"].to_list()
    hashes = [hashlib.md5(str(s).encode("utf-8")).hexdigest() for s in concat_list]
    # Append the hashes as a new column
    df = df.with_columns(pl.Series(name=hash_field, values=hashes))
    # Drop the temporary key column
    df = df.drop("__hash_key")

    # ------------------------------------------------------------------
    # Stats: capture row and hash counts prior to deduplication
    # ------------------------------------------------------------------
    # We record the number of rows after date filtering and the number of
    # unique hashes in the current batch.  These values are used only
    # when return_stats is True and are otherwise ignored.
    if return_stats:
        rows_after_filter = df.height
        # Extract the hash column as a Python list.  Using .to_list()
        # forces materialisation but avoids overhead of dict conversions.
        md5_list_local = df[hash_field].to_list()
        unique_md5_count = len(set(md5_list_local))

    # ---------------------------------------------------------------------
    # 5. Connect to MongoDB and ensure the partial unique index exists
    # ---------------------------------------------------------------------
    client = MongoClient(mongo_uri)
    coll = client[db_name][coll_name]
    try:
        coll.create_index(
            [(hash_field, 1)],
            name=f"uniq_{hash_field}",
            unique=True,
            partialFilterExpression={hash_field: {"$type": "string"}},
        )
    except Exception:
        # Ignore index creation errors; the index may already exist with
        # incompatible options.  In that case, MongoDB will not create a
        # duplicate index and returns an error.  It's safe to proceed.
        pass

    # ---------------------------------------------------------------------
    # 6. Build the documents preserving original order
    # ---------------------------------------------------------------------
    original_cols = [col for col in columns]
    records: List[Dict[str, object]] = []
    # Convert the dataframe to a list of dicts.  This is done row by row
    # because Polars does not guarantee field order when converting to
    # dictionaries.  Explicitly build the dict using the original column
    # ordering and append the hash field at the end.
    rows = df.to_dicts()
    for row in rows:
        doc = {col: row.get(col) for col in original_cols}
        doc[hash_field] = row[hash_field]
        records.append(doc)

    # ---------------------------------------------------------------------
    # 7. Determine which records are new by querying existing hashes in
    #    manageable chunks.  This prevents the $in list from exceeding
    #    MongoDB's BSON document size limit.
    # ---------------------------------------------------------------------
    all_hashes: List[str] = [rec[hash_field] for rec in records]
    existing_hashes: set[str] = set()
    if dedup_chunk_size > 0:
        for i in range(0, len(all_hashes), dedup_chunk_size):
            chunk = all_hashes[i : i + dedup_chunk_size]
            # Find only the hash field to reduce network overhead
            try:
                for doc in coll.find({hash_field: {"$in": chunk}}, {hash_field: 1}):
                    h = doc.get(hash_field)
                    if h is not None:
                        existing_hashes.add(h)
            except errors.OperationFailure:
                # If the $in query fails (e.g. index not ready), skip dedup
                existing_hashes.clear()
                break

    # Stats: record how many hashes already exist in the database
    if return_stats:
        existing_count = len(existing_hashes)

    # Filter out records whose hashes already exist
    new_records: List[Dict[str, object]] = []
    if existing_hashes:
        for rec in records:
            if rec[hash_field] not in existing_hashes:
                new_records.append(rec)
    else:
        # If we couldn't detect existing hashes or dedup is disabled, attempt
        # to insert all records.  Duplicate keys will be handled by the
        # unique index on MongoDB.
        new_records = records

    # Stats: number of records prepared for insertion after deduplication
    to_insert_count = len(new_records)
    if not new_records:
        client.close()
        if return_stats:
            return {
                "rows_read": rows_read,
                "rows_after_filter": rows_after_filter,
                "unique_md5": unique_md5_count,
                "existing_in_db": existing_count,
                "to_insert": to_insert_count,
                "inserted": 0,
            }
        return 0

    # ---------------------------------------------------------------------
    # 8. Insert the new records in batches.  We use ordered=False to allow
    #    MongoDB to continue processing after encountering duplicate key
    #    errors.  The number of inserted documents is approximated by
    #    summing the reported nInserted values from any BulkWriteError.
    # ---------------------------------------------------------------------
    inserted_count = 0
    for i in range(0, len(new_records), batch_size):
        batch = new_records[i : i + batch_size]
        try:
            result = coll.insert_many(batch, ordered=False, bypass_document_validation=True)
            # PyMongo returns inserted_ids for successful inserts
            inserted_count += len(result.inserted_ids)
        except errors.BulkWriteError as bwe:
            # Some inserts failed due to duplicates; count the successful ones
            details = bwe.details
            inserted_count += details.get('nInserted', 0)
    if return_stats:
        return {
            "rows_read": rows_read,
            "rows_after_filter": rows_after_filter,
            "unique_md5": unique_md5_count,
            "existing_in_db": existing_count,
            "to_insert": to_insert_count,
            "inserted": inserted_count,
        }
    return inserted_count
if __name__ == "__main__":
    # 普通用法：返回插入的行数
    # n = import_ozon_orders_polars("orders.csv", days_back=5)
    n = import_ozon_orders_polars("orders.csv")
    print("inserted:", n)

    # 返回统计信息
    # stats = import_ozon_orders_polars(
    #     "orders.csv",
    #     start_date="2025-06-01",
    #     end_date="2025-06-10",
    #     return_stats=True
    # )
    # print(stats)

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


    

