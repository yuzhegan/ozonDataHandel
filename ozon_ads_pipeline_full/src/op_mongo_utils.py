from __future__ import annotations
from typing import List, Dict, Any
from pymongo import MongoClient, UpdateOne
from .config import MongoConfig
from .utils import md5_of_fields

COLLECTION_NAME = "opcampaign"

def get_collection(cfg: MongoConfig):
    cli = MongoClient(cfg.uri)
    db = cli[cfg.db]
    coll = db[COLLECTION_NAME]
    coll.create_index([("dedup_md5", 1)], unique=True, background=True)
    coll.create_index([("date", 1), ("sku", 1)], background=True)
    coll.create_index([("PromotionStatus", 1)], background=True)
    return coll

def upsert_docs(cfg: MongoConfig, docs: List[Dict[str, Any]]) -> Dict[str, Any]:
    coll = get_collection(cfg)
    ops = []
    for d in docs:
        key = md5_of_fields(d.get("date"), d.get("sku"))
        d["dedup_md5"] = key
        ops.append(UpdateOne({"dedup_md5": key}, {"$set": d}, upsert=True))
    if not ops:
        return {"matched": 0, "modified": 0, "upserted": 0}
    res = coll.bulk_write(ops, ordered=False)
    return {"matched": res.matched_count, "modified": res.modified_count, "upserted": len(res.upserted_ids)}
