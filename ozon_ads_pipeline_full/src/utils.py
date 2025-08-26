from __future__ import annotations
import re
import hashlib
from datetime import date, timedelta
from typing import Iterable, List, Dict, Any

def parse_number(s):
    if s is None:
        return None
    if isinstance(s, (int, float)):
        return float(s)
    s = str(s).replace(' ', '').replace(',', '.')
    if not re.match(r'^-?\d+(\.\d+)?$', s):
        return None
    try:
        return float(s)
    except Exception:
        return None

def date_range_yyyymmdd(date_from: str, date_to: str) -> List[str]:
    y1, m1, d1 = map(int, date_from.split('-'))
    y2, m2, d2 = map(int, date_to.split('-'))
    start = date(y1, m1, d1)
    end = date(y2, m2, d2)
    out = []
    cur = start
    while cur <= end:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out

def assign_dates_evenly(rows: List[Dict[str, Any]], date_from: str, date_to: str) -> None:
    days = date_range_yyyymmdd(date_from, date_to)
    n = len(rows)
    d = len(days)
    if n == 0 or d == 0:
        return
    base = n // d
    rem = n % d
    idx = 0
    for i, the_day in enumerate(days):
        take = base + (1 if i < rem else 0)
        for _ in range(take):
            if idx >= n:
                return
            row = rows[idx]
            if not row.get("date"):
                row["date"] = the_day
            idx += 1

def md5_of_fields(*parts: Iterable[str]) -> str:
    m = hashlib.md5()
    for p in parts:
        m.update(str(p).encode("utf-8"))
    return m.hexdigest()
