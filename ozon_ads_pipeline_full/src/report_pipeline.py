from __future__ import annotations
from typing import Any, Dict, List, Optional
from icecream import ic
from .config import OzonConfig, MongoConfig
from .ozon_client import OzonClient
from .utils import assign_dates_evenly, parse_number
from .mongo_utils import upsert_docs
import time

SUCCESS_STATES = {"SUCCESS", "READY", "DONE", "OK", 'ok'}

def filter_by_states(camps: List[Dict[str, Any]], keep_states: Optional[List[str]]) -> List[Dict[str, Any]]:
    if not keep_states:
        return camps
    keep = set(keep_states)
    return [c for c in camps if c.get("state") in keep]

def wait_until_ready(client: OzonClient, uuid: str, timeout_sec: int = 180, poll: float = 2.0) -> Dict[str, Any]:
    deadline = time.time() + timeout_sec
    last = {}
    while time.time() < deadline:
        st = client.report_status(uuid)
        last = st
        state = (st.get("state") or st.get("Status") or st.get("status") or "").upper()
        if state in SUCCESS_STATES:
            return st
        if "ERROR" in state or "FAIL" in state:
            raise RuntimeError(f"UUID {uuid} 生成失败: {st}")
        time.sleep(poll)
    raise TimeoutError(f"等待 UUID {uuid} 超时，最后状态: {last}")

def flatten_rows(campaign_meta: Dict[str, Any], rows: List[Dict[str, Any]], date_from: str, date_to: str) -> List[Dict[str, Any]]:
    # 缺失日期则均匀补齐
    if any(not r.get("date") for r in rows):
        assign_dates_evenly(rows, date_from, date_to)

    base = {
        "campaignId": campaign_meta.get("id") or campaign_meta.get("campaignId"),
        "campaignTitle": campaign_meta.get("title"),
        "state": campaign_meta.get("state"),
        "advObjectType": campaign_meta.get("advObjectType"),
        "productCampaignMode": campaign_meta.get("productCampaignMode"),
        "placement": campaign_meta.get("placement"),
        "budget": campaign_meta.get("budget"),
        "budgetType": campaign_meta.get("budgetType"),
        "weeklyBudget": campaign_meta.get("weeklyBudget"),
        "startWeekDay": campaign_meta.get("startWeekDay"),
        "endWeekDay": campaign_meta.get("endWeekDay"),
        "createdAt": campaign_meta.get("createdAt"),
        "updatedAt": campaign_meta.get("updatedAt"),
    }
    out: List[Dict[str, Any]] = []
    for r in rows:
        row = dict(base)
        d = r.get("date")
        if d and "." in d and "-" not in d:  # 20.08.2025 → 2025-08-20
            dd, mm, yyyy = d.split(".")
            d = f"{yyyy}-{mm}-{dd}"
        row["date"] = d
        row["sku"] = r.get("sku")
        row["title_item"] = r.get("title") or r.get("search_query")
        for k in ("views","clicks","ctr","orders","ordersMoney","moneySpent","avgBid","price","toCart","models","modelsMoney","drr"):
            row[k] = parse_number(r.get(k))
        out.append(row)
    return out

def run(date_from: str, date_to: str, keep_states: Optional[List[str]] = None, campaign_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    ocfg = OzonConfig()
    mcfg = MongoConfig()
    client = OzonClient(ocfg)

    camps = client.campaigns()
    if campaign_ids:
        idset = set(map(str, campaign_ids))
        camps = [c for c in camps if str(c.get("id") or c.get("campaignId")) in idset]
    camps = filter_by_states(camps, keep_states)

    if not camps:
        return {"message": "无匹配活动", "selected": 0, "docs": 0, "upserted": 0}

    all_docs: List[Dict[str, Any]] = []

    for c in camps:
        cid = str(c.get("id") or c.get("campaignId"))
        try:
            uuids = client.request_statistics_json([cid], date_from, date_to, group_by="DATE")
            for u in uuids:
                _ = wait_until_ready(client, u, timeout_sec=180, poll=2.0)
                rep = client.download_report(u)

                rows: List[Dict[str, Any]] = []
                if isinstance(rep, dict):
                    if cid in rep and isinstance(rep[cid], dict):
                        rr = rep[cid].get("report", {})
                        rows = rr.get("rows", [])
                    if not rows:
                        maybe = rep.get("report")
                        if isinstance(maybe, dict):
                            rows = maybe.get("rows", [])

                docs = flatten_rows(c, rows or [], date_from, date_to)
                all_docs.extend(docs)
        except Exception as e:
            ic(f"活动 {cid} 处理失败: {e}")
            continue

    if all_docs:
        res = upsert_docs(mcfg, all_docs)
        return {"message": "OK", "selected": len(camps), "docs": len(all_docs), **res}
    return {"message": "没有可写入的数据", "selected": len(camps), "docs": 0, "upserted": 0}
