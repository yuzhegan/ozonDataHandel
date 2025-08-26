from __future__ import annotations
import argparse
from typing import List, Optional
from src.report_pipeline import run

def parse_args():
    p = argparse.ArgumentParser(description="Ozon Performance Ads (campaign) → MongoDB(mbcampagin)")
    p.add_argument("--date-from", required=True, help="YYYY-MM-DD")
    p.add_argument("--date-to", required=True, help="YYYY-MM-DD")
    p.add_argument("--states", nargs="*", default=[], help="按状态筛选，如：CAMPAIGN_STATE_RUNNING CAMPAIGN_STATE_PAUSED")
    p.add_argument("--campaign-ids", nargs="*", default=[], help="仅处理指定活动 ID 列表")
    return p.parse_args()

def main():
    a = parse_args()
    res = run(a.date_from, a.date_to, a.states or None, a.campaign_ids or None)
    print(res)

if __name__ == "__main__":
    main()
