from __future__ import annotations
import argparse
from src.products_daily_pipeline import run_products_daily

def parse_args():
    p = argparse.ArgumentParser(description="Ozon Products Daily Report → Mongo(opcampaign)")
    p.add_argument("--day", required=True, help="YYYY-MM-DD（单日）")
    return p.parse_args()

def main():
    a = parse_args()
    res = run_products_daily(a.day)
    print(res)

if __name__ == "__main__":
    main()
