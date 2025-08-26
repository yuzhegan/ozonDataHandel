from __future__ import annotations
import argparse, json
from src.products_range_runner import run_products_range

def parse_args():
    p = argparse.ArgumentParser(description="Ozon Products Daily Reports (range) → Mongo(opcampaign)")
    p.add_argument("--date-from", required=True, help="YYYY-MM-DD")
    p.add_argument("--date-to", required=True, help="YYYY-MM-DD")
    p.add_argument("--delay-seconds", type=float, default=0.0, help="两个日期之间的请求间隔（秒）")
    p.add_argument("--stop-on-error", action="store_true", help="遇到错误立即停止")
    return p.parse_args()

def main():
    a = parse_args()
    res = run_products_range(a.date_from, a.date_to, a.delay_seconds, a.stop_on_error)
    print(json.dumps(res, ensure_ascii=False))

if __name__ == "__main__":
    main()
