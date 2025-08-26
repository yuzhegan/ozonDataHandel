import os
from typing import Dict, Any, List
from pymongo import MongoClient

MONGO_URI=os.getenv("MONGO_URI","mongodb://localhost:27017")
MONGO_DB=os.getenv("MONGO_DB","ozondatas")
ALLOWED=[s.strip() for s in os.getenv("ALLOWED_COLLECTIONS","mbcampagin,opcampaign").split(",") if s.strip()]

_client=MongoClient(MONGO_URI)
_db=_client[MONGO_DB]

def list_collections()->List[str]:
    return ALLOWED

def get_collection(name:str):
    if name not in ALLOWED:
        raise ValueError(f"Collection {name} not allowed")
    return _db[name]

def sample_fields(name:str, sample:int=200)->Dict[str,str]:
    coll=get_collection(name)
    fields={}
    for doc in coll.find({}, limit=sample):
        for k,v in doc.items():
            fields.setdefault(k, type(v).__name__)
    return fields
