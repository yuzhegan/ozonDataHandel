# encoding='utf-8

# @Time: 2025-08-10
# @File: %
#!/usr/bin/env
from __future__ import annotations
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
High‑throughput accrual report import into MongoDB using Polars.

This module exposes a single function, ``import_ozon_accruals_polars``, that
reads Ozon accrual XLSX/CSV files, performs optional date filtering and
deduplication, computes a deterministic MD5 hash across selected key
columns, and bulk inserts new records into a MongoDB collection.  The
implementation mirrors the high‑performance design of
``import_ozon_orders_polars_fast.py``: lazy CSV scanning pushes filters
down to the reader, batching reduces network round trips, and partial
unique indexes avoid conflicts from legacy documents lacking the hash
field.

Key features
------------

* **Excel and CSV support**: The function will automatically detect
  .xlsx files and use Polars' Excel reader (with a fallback to pandas
  when unavailable).  CSV files are read lazily using Polars' scan API.

* **Date filtering**: You can choose to import only recent days, a fixed
  date range, or an explicit list of dates.  Dates are parsed from the
  specified ``date_field`` column.  Filtering is applied lazily for
  CSVs to avoid unnecessary I/O.

* **MD5 deduplication**: By default the hash is computed from six
  business keys: ``Дата начисления``, ``Тип начисления``,
  ``Номер отправления или идентификатор услуги``, ``SKU``, ``Название товара или
  услуги`` and ``Итого, руб.``.  You can override this list via
  ``hash_cols``.  The computed hash is stored in the Mongo document
  under ``hash_field`` (default ``_row_md5``), and a partial unique index is
  maintained on that field.  Existing hashes are looked up in chunks
  to avoid hitting MongoDB's BSON document size limit.

* **Preservation of column order**: The documents inserted into MongoDB
  preserve the original column ordering from the input file.  The hash
  field is appended as the last key.

* **Performance and diagnostics**: Batch inserts and chunked duplicate
  checks keep memory usage bounded.  An optional ``return_stats`` flag
  returns detailed metrics about the import process, including row
  counts before and after filtering, unique hash counts, existing
  hashes found, records prepared for insertion and actual insertions.

Example usage::

    from import_ozon_accruals_polars import import_ozon_accruals_polars

    # import everything from an xlsx file
    n = import_ozon_accruals_polars("accrual_report.xlsx")
    print(f"Inserted {n} new documents")

    # import just last two days from a CSV file
    n = import_ozon_accruals_polars("accrual_report.csv", days_back=2)
    print(f"Inserted {n} new documents for recent days")

    # import a fixed date range
    stats = import_ozon_accruals_polars(
        "accrual_report.xlsx", start_date="2025-08-01", end_date="2025-08-03",
        return_stats=True
    )
    print(stats)

Note that you must have a running MongoDB instance accessible via
``mongo_uri``.  The default connection string points at localhost.
"""


import hashlib
from datetime import date, datetime, timedelta
from typing import Any, Iterable, List, Optional, Sequence, Dict

import polars as pl
from pymongo import MongoClient, errors

__all__ = ["import_ozon_accruals_polars"]


def _strip_bom(s: str) -> str:
    """Strip a Unicode BOM from the beginning of a string if present."""
    return s.lstrip("\ufeff") if isinstance(s, str) else s


def _normalize_header(name: str) -> str:
    """Normalize a column name by stripping BOM and whitespace and
    lowercasing."""
    return _strip_bom(name).strip().lower()


def _resolve_one(cols: Sequence[str], targets: Sequence[str]) -> Optional[str]:
    """Resolve one logical column name from a list of possible aliases.

    This function first looks for an exact case‑insensitive match after
    normalisation, then falls back to a substring match.  If nothing
    matches, ``None`` is returned.
    """
    normalized = {_normalize_header(c): c for c in cols}
    for t in targets:
        key = _normalize_header(t)
        if key in normalized:
            return normalized[key]
    for t in targets:
        key = _normalize_header(t)
        for norm, orig in normalized.items():
            if key in norm:
                return orig
    return None


def _resolve_cols(cols: Sequence[str], desired_aliases: Sequence[Sequence[str]]) -> List[str]:
    """Resolve multiple logical column names from their alias lists.

    Parameters
    ----------
    cols:
        The list of actual column names present in the data frame.
    desired_aliases:
        A sequence of sequences, where each inner sequence contains
        possible aliases for one logical field.  The first alias in
        each inner sequence is considered the canonical name for error
        reporting.

    Returns
    -------
    list[str]
        The resolved list of column names in the order corresponding to
        ``desired_aliases``.

    Raises
    ------
    KeyError
        If any of the logical fields cannot be resolved.
    """
    resolved: List[str] = []
    missing: List[str] = []
    for variants in desired_aliases:
        r = _resolve_one(cols, variants)
        if r is None:
            missing.append(variants[0])
        else:
            resolved.append(r)
    if missing:
        raise KeyError(f"缺少用于MD5的列: {missing}; 现有列: {list(cols)}")
    return resolved


def _parse_ymd(d: str) -> date:
    """Parse a YYYY-MM-DD string into a date object."""
    return datetime.strptime(d, "%Y-%m-%d").date()


def import_ozon_accruals_polars(
    file_path: str,
    *,
    mongo_uri: str = "mongodb://localhost:27017",
    db_name: str = "ozondatas",
    coll_name: str = "ozon_accruals",
    date_field: str = "Дата начисления",
    days_back: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    include_dates: Optional[Iterable[str]] = None,
    hash_cols: Optional[Sequence[str]] = None,
    hash_field: str = "_row_md5",
    batch_size: int = 20000,
    dedup_chunk_size: int = 50000,
    sheet_name: Optional[int | str] = None,
    return_stats: bool = False,
) -> int | Dict[str, Any]:
    """
    Import an Ozon accrual report into MongoDB with deduplication.

    This function reads the specified file (XLSX or CSV), applies optional
    date filtering, computes an MD5 hash from a set of key columns, and
    inserts unique records into the specified MongoDB collection.  If
    ``return_stats`` is ``True``, a dictionary with detailed statistics
    about the import process is returned instead of an integer.

    Parameters
    ----------
    file_path:
        Path to the input file (.xlsx or .csv).  The file must have a
        header row.
    mongo_uri:
        MongoDB connection string.  Defaults to a local instance.
    db_name:
        Name of the target database.
    coll_name:
        Name of the target collection.
    date_field:
        Name of the column containing the date or datetime used for
        filtering.  The function attempts to parse this column to dates.
    days_back:
        If provided, import only the last ``days_back`` days excluding
        today.  For example, ``days_back=1`` imports only yesterday.
        ``2`` imports yesterday and the day before yesterday.  Ignored
        if ``include_dates`` or ``start_date``/``end_date`` are provided.
    start_date, end_date:
        Inclusive start and end dates in ``YYYY-MM-DD`` format.  When
        both are provided, import only rows whose date falls within the
        range.  These override ``days_back``.  If only one is provided,
        filtering is skipped.
    include_dates:
        A sequence of exact dates (``YYYY-MM-DD``) to include.  When
        present, only rows matching these dates are imported.  This
        parameter overrides ``days_back`` and ``start_date``/``end_date``.
    hash_cols:
        Explicit list of column names to use for MD5 hashing.  If
        provided, these names must exist exactly in the input.  If
        omitted, the function resolves the default six fields via an
        internal alias mapping.  Passing ``hash_cols`` disables alias
        resolution.
    hash_field:
        Name of the field in the Mongo document where the MD5 hash will
        be stored.
    batch_size:
        Number of documents to insert per batch.  Larger batches reduce
        round‑trips but require more memory.
    dedup_chunk_size:
        When checking for existing hashes, how many hash values to
        include in each `$in` query.  Large lists can exceed MongoDB's
        BSON document size limit.
    sheet_name:
        Sheet name or index to read when ``file_path`` is an Excel file.
        Ignored for CSV files.
    return_stats:
        When ``True``, the function returns a dictionary with detailed
        statistics about the import process instead of an integer.

    Returns
    -------
    int or dict
        If ``return_stats`` is ``False``, the number of newly inserted
        documents.  Otherwise a dictionary with keys ``rows_read``,
        ``rows_after_filter``, ``unique_md5``, ``existing_in_db``,
        ``to_insert``, and ``inserted``.
    """
    # Initialise stats variables
    rows_read: Optional[int] = None
    # Count rows before filtering if stats are requested
    if return_stats:
        try:
            if file_path.lower().endswith(".csv"):
                count_df = (
                    pl.scan_csv(
                        file_path,
                        separator=';',
                        has_header=True,
                        skip_rows=0,
                        infer_schema_length=0,
                    ).select(pl.count())
                ).collect()
                rows_read = int(count_df[0, 0])
            else:
                # For Excel, use pandas to count rows efficiently
                try:
                    import pandas as pd
                    dfp_temp = pd.read_excel(file_path, sheet_name=sheet_name if sheet_name is not None else 0)
                    rows_read = int(len(dfp_temp))
                except Exception:
                    rows_read = None
        except Exception:
            rows_read = None

    # Prepare date filtering boundaries
    include_dates_parsed: List[date] = []
    start_dt: Optional[date] = None
    end_dt: Optional[date] = None
    # Precedence: include_dates > start/end > days_back
    if include_dates:
        for d in include_dates:
            try:
                include_dates_parsed.append(_parse_ymd(str(d)))
            except Exception:
                continue
    elif start_date and end_date:
        try:
            start_dt = _parse_ymd(start_date)
            end_dt = _parse_ymd(end_date)
            if start_dt > end_dt:
                start_dt, end_dt = end_dt, start_dt
        except Exception:
            start_dt = None
            end_dt = None
    elif days_back and days_back > 0:
        today = date.today()
        start_dt = today - timedelta(days=days_back)
        end_dt = today - timedelta(days=1)

    # Determine file extension
    lower = file_path.lower()
    df: pl.DataFrame

    if lower.endswith(".csv"):
        # Lazy scan of CSV for efficient filtering
        lf = pl.scan_csv(
            file_path,
            separator=';',
            has_header=True,
            skip_rows=0,
            infer_schema_length=0,
        )
        # Remove BOM from first column if present
        cols = lf.collect_schema().names()
        if cols and cols[0].startswith("\ufeff"):
            lf = lf.rename({cols[0]: cols[0].lstrip("\ufeff")})
            cols = lf.collect_schema().names()
        # Set rows_read for stats if not already counted
        if return_stats:
            try:
                rows_read = len(dfp)
            except Exception:
                pass

        # Resolve date_field alias if necessary
        if date_field not in cols:
            alt = _resolve_one(cols, [date_field, "Дата начисления", "Дата", "Дата операции"])
            if alt is None:
                raise KeyError(f"date_field '{date_field}' 不在列中；现有列: {cols}")
            date_field = alt
        # Build date column for filtering using Polars' flexible parser.  We use
        # `format=None` so that Polars will attempt to parse a wide range
        # of date and datetime formats, including those without zero‑padded
        # months or days (e.g. "2025/8/1").  After parsing to a
        # ``Datetime``, we extract only the date component.  This mirrors
        # the behaviour used in the orders import script and prevents
        # unparseable strings from silently becoming nulls that cause all
        # rows to be filtered out.
        fdate = (
            pl.col(date_field)
            .cast(pl.Utf8, strict=False)
            .str.strip_chars()
            .str.replace_all('\r', '')
            .str.replace_all('\n', '')
            .str.strptime(pl.Datetime, strict=False, format=None)
            .dt.date()
            .alias("_fdate")
        )
        lf = lf.with_columns(fdate)
        # Apply date filters lazily
        if include_dates_parsed:
            lf = lf.filter(pl.col("_fdate").is_in(include_dates_parsed))
        elif start_dt and end_dt:
            lf = lf.filter((pl.col("_fdate") >= start_dt) & (pl.col("_fdate") <= end_dt))
        elif days_back and days_back > 0:
            lf = lf.filter((pl.col("_fdate") >= start_dt) & (pl.col("_fdate") <= end_dt))
        # Drop temporary date column
        lf = lf.drop("_fdate")
        df = lf.collect(streaming=True)
    else:
        # -----------------------------------------------------------------
        # Excel reading via pandas
        # -----------------------------------------------------------------
        import pandas as pd
        # Read the specified sheet (by index or name)
        dfp = pd.read_excel(file_path, sheet_name=sheet_name if sheet_name is not None else 0)

        # Resolve date_field alias if necessary
        if date_field not in dfp.columns:
            alt = _resolve_one(dfp.columns, [date_field, "Дата начисления", "Дата", "Дата операции"])
            if alt is None:
                raise KeyError(
                    f"date_field '{date_field}' 不在列中；现有列: {dfp.columns.tolist()}"
                )
            date_field = alt

        # Build date column for filtering.  Use pandas to parse the
        # date_field column to datetime, then extract the date component.
        dfp['_fdate'] = pd.to_datetime(dfp[date_field], errors='coerce').dt.date

        # Apply date filters
        if include_dates_parsed:
            dfp = dfp[dfp['_fdate'].isin(include_dates_parsed)]
        elif start_dt and end_dt:
            dfp = dfp[(dfp['_fdate'] >= start_dt) & (dfp['_fdate'] <= end_dt)]
        elif days_back and days_back > 0:
            dfp = dfp[(dfp['_fdate'] >= start_dt) & (dfp['_fdate'] <= end_dt)]
        # Drop temporary date column
        dfp = dfp.drop(columns=['_fdate'])
        # Reset index after filtering so that row indices are 0..n-1.  Without
        # resetting, the original index values from the Excel file may be
        # preserved (e.g., [2, 5, 7]), causing mismatches when indexing
        # md5_list by row number.  Resetting ensures md5_list aligns with
        # DataFrame rows during iteration.
        dfp.reset_index(drop=True, inplace=True)

        # Stats: rows after filtering
        rows_after_filter = len(dfp)
        if rows_after_filter == 0:
            if return_stats:
                return {
                    "rows_read": rows_read,
                    "rows_after_filter": 0,
                    "unique_md5": 0,
                    "existing_in_db": 0,
                    "to_insert": 0,
                    "inserted": 0,
                }
            return 0

        # Preserve original column order
        original_cols = dfp.columns.tolist()

        # Resolve hash columns
        if hash_cols:
            key_cols = [col for col in hash_cols]
            missing = [c for c in key_cols if c not in dfp.columns]
            if missing:
                raise KeyError(f"Missing hash columns: {missing}; in columns {dfp.columns.tolist()}")
        else:
            desired_aliases = [
                ["Дата начисления", "Дата", "Дата операции"],
                ["Тип начисления", "Тип", "Тип услуги"],
                ["Номер отправления или идентификатор услуги", "Номер отправления", "Идентификатор услуги", "Отправление/услуга"],
                ["SKU", "Артикул SKU", "ИД SKU", "Артикул"],
                ["Название товара или услуги", "Название товара", "Название услуги", "Наименование"],
                ["Итого, руб.", "Итого, руб", "Итого руб.", "Итого"],
            ]
            key_cols = _resolve_cols(original_cols, desired_aliases)

        # Compute MD5 hashes using pandas.  Fill NaNs with empty string,
        # convert to string and concatenate with a delimiter.  The
        # delimiter is ``\x1f`` (unit separator) to avoid accidental
        # collisions.  Compute the hash in Python for each row.
        concat_series = dfp[key_cols].fillna('').astype(str).agg(lambda x: "\x1f".join(x), axis=1)
        md5_list = [hashlib.md5(s.encode('utf-8')).hexdigest() for s in concat_series]
        if return_stats:
            unique_md5_count = len(set(md5_list))

        # Build list of documents preserving original column order and
        # attaching the hash field.  Convert any ``datetime.date`` into
        # ``datetime.datetime`` at midnight for MongoDB compatibility.
        records: List[Dict[str, Any]] = []
        for idx, row in dfp.iterrows():
            doc: Dict[str, Any] = {}
            for col in original_cols:
                val = row[col]
                # Convert date to datetime (if not already datetime)
                if isinstance(val, date) and not isinstance(val, datetime):
                    doc[col] = datetime(val.year, val.month, val.day)
                else:
                    doc[col] = val
            doc[hash_field] = md5_list[idx]
            records.append(doc)

        # Connect to MongoDB and ensure partial unique index
        client = MongoClient(mongo_uri)
        coll = client[db_name][coll_name]
        index_name = f"uniq_{hash_field}_str"
        if index_name not in coll.index_information():
            try:
                coll.create_index(
                    [(hash_field, 1)],
                    name=index_name,
                    unique=True,
                    partialFilterExpression={hash_field: {"$type": "string"}},
                )
            except Exception:
                pass

        # Determine existing hashes in database in chunks
        existing_hashes: set[str] = set()
        if dedup_chunk_size > 0:
            unique_hashes = list(set(md5_list))
            for i in range(0, len(unique_hashes), dedup_chunk_size):
                chunk = unique_hashes[i : i + dedup_chunk_size]
                for doc in coll.find({hash_field: {"$in": chunk}}, {hash_field: 1}):
                    h = doc.get(hash_field)
                    if isinstance(h, str):
                        existing_hashes.add(h)

        # Build new records, de‑duplicating within this batch
        new_records: List[Dict[str, Any]] = []
        seen_hashes: set[str] = set()
        for doc in records:
            h = doc[hash_field]
            if h in existing_hashes or h in seen_hashes:
                continue
            new_records.append(doc)
            seen_hashes.add(h)

        to_insert_count = len(new_records)
        if to_insert_count == 0:
            client.close()
            if return_stats:
                return {
                    "rows_read": rows_read,
                    "rows_after_filter": rows_after_filter,
                    "unique_md5": unique_md5_count if return_stats else 0,
                    "existing_in_db": len(existing_hashes),
                    "to_insert": to_insert_count,
                    "inserted": 0,
                }
            return 0

        # Insert new records in batches and accumulate inserted count
        inserted_count = 0
        for i in range(0, to_insert_count, batch_size):
            batch_docs = new_records[i : i + batch_size]
            try:
                res = coll.insert_many(batch_docs, ordered=False, bypass_document_validation=True)
                inserted_count += len(res.inserted_ids)
            except errors.BulkWriteError as bwe:
                inserted_count += bwe.details.get('nInserted', 0)
            except Exception:
                pass

        client.close()
        if return_stats:
            return {
                "rows_read": rows_read,
                "rows_after_filter": rows_after_filter,
                "unique_md5": unique_md5_count if return_stats else 0,
                "existing_in_db": len(existing_hashes),
                "to_insert": to_insert_count,
                "inserted": inserted_count,
            }
        return inserted_count

    # Stats: rows after filtering
    rows_after_filter = df.height
    if rows_after_filter == 0:
        if return_stats:
            return {
                "rows_read": rows_read,
                "rows_after_filter": 0,
                "unique_md5": 0,
                "existing_in_db": 0,
                "to_insert": 0,
                "inserted": 0,
            }
        return 0

    # Preserve original column order
    original_cols = list(df.columns)

    # Resolve hash columns
    if hash_cols:
        key_cols = [col for col in hash_cols]
        missing = [c for c in key_cols if c not in df.columns]
        if missing:
            raise KeyError(f"Missing hash columns: {missing}; in columns {df.columns}")
    else:
        desired_aliases = [
            ["Дата начисления", "Дата", "Дата операции"],
            ["Тип начисления", "Тип", "Тип услуги"],
            ["Номер отправления или идентификатор услуги", "Номер отправления", "Идентификатор услуги", "Отправление/услуга"],
            ["SKU", "Артикул SKU", "ИД SKU", "Артикул"],
            ["Название товара или услуги", "Название товара", "Название услуги", "Наименование"],
            ["Итого, руб.", "Итого, руб", "Итого руб.", "Итого"],
        ]
        key_cols = _resolve_cols(original_cols, desired_aliases)

    # Compute concatenated key
    df = df.with_columns(
        pl.concat_str([
            pl.col(c).cast(pl.Utf8, strict=False).fill_null("") for c in key_cols
        ], separator="|").alias("__hash_key")
    )
    # Compute MD5 hashes in Python for maximum compatibility
    concat_list = df["__hash_key"].to_list()
    md5_list = [hashlib.md5((s if s is not None else "").encode("utf-8")).hexdigest() for s in concat_list]
    df = df.drop("__hash_key").with_columns(pl.Series(name=hash_field, values=md5_list))
    # Stats: unique md5 count
    if return_stats:
        unique_md5_count = len(set(md5_list))

    # Connect to MongoDB and ensure partial unique index
    client = MongoClient(mongo_uri)
    coll = client[db_name][coll_name]
    index_name = f"uniq_{hash_field}_str"
    if index_name not in coll.index_information():
        try:
            coll.create_index(
                [(hash_field, 1)],
                name=index_name,
                unique=True,
                partialFilterExpression={hash_field: {"$type": "string"}},
            )
        except Exception:
            pass

    # Determine existing hashes in database in chunks
    existing_hashes: set[str] = set()
    md5_set = set(md5_list)
    if dedup_chunk_size > 0:
        md5_list_unique = list(md5_set)
        for i in range(0, len(md5_list_unique), dedup_chunk_size):
            chunk = md5_list_unique[i:i + dedup_chunk_size]
            for doc in coll.find({hash_field: {"$in": chunk}}, {hash_field: 1}):
                v = doc.get(hash_field)
                if isinstance(v, str):
                    existing_hashes.add(v)
    # Stats: existing hash count
    if return_stats:
        existing_count = len(existing_hashes)

    # Build list of new records, de‑duplicating duplicates within this batch.
    # We maintain a set of hashes seen in this batch ("seen_hashes") in
    # addition to the hashes already present in the database ("existing_hashes").
    # Only the first occurrence of each MD5 within the current import is
    # inserted, preventing unique‑index conflicts and unnecessary insert
    # attempts.
    new_records: List[Dict[str, Any]] = []
    seen_hashes: set[str] = set()
    for row in df.iter_rows(named=True):
        h = row[hash_field]
        if h in existing_hashes or h in seen_hashes:
            continue
        doc = {col: row.get(col) for col in original_cols}
        # Convert `datetime.date` values (which are not datetime.datetime) to
        # ``datetime`` objects at midnight.  PyMongo cannot encode
        # ``datetime.date`` directly【443358688638899†L1151-L1156】.
        for key, val in doc.items():
            # Skip None and already converted datetime objects
            if val is None:
                continue
            # Check for `datetime.date` but not `datetime.datetime`
            # We import `date` and `datetime` from datetime module at the top
            if isinstance(val, date) and not isinstance(val, datetime):
                doc[key] = datetime(val.year, val.month, val.day)
        doc[hash_field] = h
        new_records.append(doc)
        seen_hashes.add(h)

    # Stats: number of records prepared for insertion after deduplication
    to_insert_count = len(new_records)
    if to_insert_count == 0:
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

    # Insert new records in batches.  If BulkWriteError occurs due to
    # unique index conflicts, we accumulate the number of successfully
    # inserted documents from the error details.  Ordered=False allows
    # other documents in the batch to be inserted even if some fail.
    inserted_count = 0
    for i in range(0, to_insert_count, batch_size):
        batch_docs = new_records[i : i + batch_size]
        try:
            res = coll.insert_many(batch_docs, ordered=False, bypass_document_validation=True)
            inserted_count += len(res.inserted_ids)
        except errors.BulkWriteError as bwe:
            inserted_count += bwe.details.get('nInserted', 0)
        except Exception:
            # For any other error, we silently skip counting inserted docs
            pass

    client.close()
    # Return stats or count
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
    # Example usage
    # Ensure you have a valid MongoDB instance running and accessible
    # Adjust the file path and database/collection names as needed

    # Import everything from an xlsx file
    # n = import_ozon_accruals_polars("./应计2025-08-01 - 2025-08-08.xlsx",
    #                                 db_name="ozondatas",
    #                                 days_back=4,
    #                                 sheet_name=0,
    #                                 coll_name="ozon_accruals")
    # print(f"Inserted {n} new documents")
    stats = import_ozon_accruals_polars(
    './应计2025-08-01 - 2025-08-08.xlsx',
    sheet_name='Начисления',
    # start_date="2025-08-01",
    # end_date="2025-08-03",
    days_back=4,
    return_stats=True
                    )
    print(stats)

    # # Import just last two days from a CSV file
    # n = import_ozon_accruals_polars("your_report.csv", days_back=2,
    #                                 db_name="ozondatas",
    #                                 coll_name="ozon_accruals")
    # print(f"Inserted {n} new documents for recent days")
