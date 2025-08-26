from __future__ import annotations
from typing import Dict, Any, List
from datetime import date as _date, timedelta
from icecream import ic
from .config import OzonConfig, MongoConfig
from .ozon_client import OzonClient
from .report_pipeline import wait_until_ready
from .utils import parse_number
from .op_mongo_utils import upsert_docs

GENERATE_PATH = "/api/client/statistic/products/generate/json"

def _iso_utc_day_bounds(day: str) -> (str, str):
    d = _date.fromisoformat(day)
    # nxt = d + timedelta(days=1)
    nxt = d
    return f"{d.isoformat()}T00:00:00Z", f"{nxt.isoformat()}T00:00:00Z"

def _flatten_rows(rows: List[Dict[str, Any]], day: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows:
        doc = dict(r)               # 保留原字段
        doc["date"] = day           # 注入请求日期
        doc["sku"] = r.get("SKU") or r.get("sku")
        doc["offerId"] = r.get("OfferID") or r.get("offerId")
        doc["title"] = r.get("Title") or r.get("title")
        doc["category"] = r.get("Category") or r.get("category")

        for k_src, k_dst in [
            ("Views","views"),("Clicks","clicks"),("CTR","ctr"),
            ("Orders","orders"),("OrdersMoney","ordersMoney"),
            ("MoneySpent","moneySpent"),("MoneySpentFromCPC","moneySpentFromCPC"),
            ("Bid","bid"),("BidValue","bidValue"),("Price","price"),
            ("ToCart","toCart"),("DRR","drr"),
            ("ordersFromCPC","ordersFromCPC"),("ordersMoneyFromCPC","ordersMoneyFromCPC"),
        ]:
            if k_src in r:
                doc[k_dst] = parse_number(r.get(k_src))

        out.append(doc)
    return out

def run_products_daily(day: str) -> Dict[str, Any]:
    ocfg = OzonConfig()
    client = OzonClient(ocfg)

    # 1) 生成 UUID（半开区间 [day, day+1)）
    ts_from, ts_to = _iso_utc_day_bounds(day)
    payload = {"from": ts_from, "to": ts_to}
    data = client._post(GENERATE_PATH, json_data=payload)

    # 2) 抽取 UUID（健壮解析）
    uuids: List[str] = []
    def walk(o):
        if isinstance(o, dict):
            for vv in o.values():
                yield from walk(vv)
        elif isinstance(o, list):
            for vv in o:
                yield from walk(vv)
        elif isinstance(o, str) and len(o) >= 36 and "-" in o:
            yield o
    uuids = list(dict.fromkeys(walk(data)))
    if not uuids:
        raise RuntimeError(f"未从生成接口拿到 UUID：{data}")

    all_rows: List[Dict[str, Any]] = []

    # 3) 逐个 UUID 轮询并下载
    for u in uuids:
        _ = wait_until_ready(client, u, timeout_sec=180, poll=2.0)
        rep = client.download_report(u)
        rows = []
        if isinstance(rep, dict):
            if "rows" in rep and isinstance(rep["rows"], list):
                rows = rep["rows"]
            else:
                maybe = rep.get("report")
                if isinstance(maybe, dict) and isinstance(maybe.get("rows"), list):
                    rows = maybe["rows"]
        rows = rows or []
        all_rows.extend(_flatten_rows(rows, day))

    # 4) 入库
    mcfg = MongoConfig()
    res = upsert_docs(mcfg, all_rows)
    return {"message": "OK", "day": day, "rows": len(all_rows), **res}
