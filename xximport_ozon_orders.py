#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Import Ozon orders (semicolon-delimited CSV) into MongoDB.

Usage:
  pip install pymongo
  python import_ozon_orders.py --csv /path/to/orders.csv --db ozondatas --collection order_info

Notes:
- Defaults: mongodb://localhost:27017, db=ozondatas, collection=order_info
- Empty strings -> None
- Dates like "YYYY-MM-DD HH:MM[:SS]" -> datetime
- Numbers (including percentages like "85%") -> numeric (percent stored as 85.0)
- Currency suffixes "RUB" / "₽" are stripped before numeric parsing
"""
import argparse
import csv
import re
from datetime import datetime
from typing import Any, Optional

from pymongo import MongoClient


def _to_datetime(s: str) -> Optional[datetime]:
    s = s.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None


_NUM_RE = re.compile(
    r"^-?\s*\d{1,3}(?:[ ,]\d{3})*(?:[.,]\d+)?$|^-?\s*\d+(?:[.,]\d+)?$"
)


def _to_number(s: str) -> Any:
    """Parse common number formats. Keeps original if parsing fails."""
    s = s.strip().replace("\xa0", " ")
    # remove common currency tails
    s = re.sub(r"(RUB|₽)\s*$", "", s, flags=re.IGNORECASE).strip()

    # percentages -> float value w/o % (e.g., "85%" -> 85.0)
    if s.endswith("%"):
        v = s[:-1].strip()
        v = v.replace(" ", "").replace(",", ".")
        try:
            return float(v)
        except ValueError:
            return s

    if not _NUM_RE.match(s):
        return s

    t = s.replace(" ", "")
    if "," in t and "." in t:
        # assume comma as thousands sep
        t = t.replace(",", "")
    elif "," in t and "." not in t:
        # assume comma decimal
        t = t.replace(",", ".")

    try:
        if "." in t:
            return float(t)
        return int(t)
    except ValueError:
        return s


def _transform(value: Optional[str]) -> Any:
    if value is None:
        return None
    v = value.strip()
    if v == "":
        return None

    dt = _to_datetime(v)
    if dt is not None:
        return dt

    num = _to_number(v)
    return num


def import_csv(csv_path: str, mongo_uri: str, db_name: str, coll_name: str) -> int:
    client = MongoClient(mongo_uri)
    db = client[db_name]
    coll = db[coll_name]

    inserted = 0
    # newline='' is important so csv handles embedded newlines correctly
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";", quotechar='"')
        batch = []
        for row in reader:
            # Keep original Chinese headers as keys
            doc = { (k.strip() if isinstance(k, str) else k): _transform(v) for k, v in row.items() }
            batch.append(doc)
            if len(batch) >= 1000:
                coll.insert_many(batch)
                inserted += len(batch)
                batch.clear()
        if batch:
            coll.insert_many(batch)
            inserted += len(batch)

    client.close()
    return inserted


def main():
    parser = argparse.ArgumentParser(description="Import Ozon orders CSV into MongoDB")
    parser.add_argument("--csv", required=True, help="Path to CSV file (semicolon-delimited)")
    parser.add_argument("--mongo-uri", default="mongodb://localhost:27017", help="MongoDB URI")
    parser.add_argument("--db", default="ozondatas", help="Database name (default: ozondatas)")
    parser.add_argument("--collection", default="order_info", help='Collection name (default: "order_info")')
    args = parser.parse_args()

    n = import_csv(args.csv, args.mongo_uri, args.db, args.collection)
    print(f"Inserted {n} documents into {args.db}.{args.collection}")


if __name__ == "__main__":
    main()
