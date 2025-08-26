from __future__ import annotations
from typing import Dict, Any, List
from datetime import date as _date, timedelta
from icecream import ic
from .products_daily_pipeline import run_products_daily

def _days_inclusive(date_from: str, date_to: str):
    d1 = _date.fromisoformat(date_from)
    d2 = _date.fromisoformat(date_to)
    cur = d1
    while cur <= d2:
        yield cur.isoformat()
        cur += timedelta(days=1)

def run_products_range(date_from: str, date_to: str, delay_seconds: float = 0.0, stop_on_error: bool = False) -> Dict[str, Any]:
    import time
    results: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    totals = {"rows": 0, "matched": 0, "modified": 0, "upserted": 0}
    for day in _days_inclusive(date_from, date_to):
        try:
            res = run_products_daily(day)
            results.append({"day": day, "result": res})
            totals["rows"] += int(res.get("rows", 0) or 0)
            totals["matched"] += int(res.get("matched", 0) or 0)
            totals["modified"] += int(res.get("modified", 0) or 0)
            totals["upserted"] += int(res.get("upserted", 0) or 0)
        except Exception as e:
            info = {"day": day, "error": str(e)}
            failures.append(info)
            ic(f"day {day} failed: {e}")
            if stop_on_error:
                return {"message": "FAILED", "first_error": info, "totals": totals, "ok_days": len(results), "failures": failures}
        if delay_seconds > 0:
            time.sleep(delay_seconds)
    return {"message": "OK", "date_from": date_from, "date_to": date_to, "totals": totals, "ok_days": len(results), "failures": failures}
