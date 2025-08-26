import os
from typing import Any, Dict, List, Optional
from pymongo import MongoClient, ASCENDING

MONGO_URI=os.getenv("MONGO_URI","mongodb://localhost:27017")
MONGO_DB=os.getenv("MONGO_DB","ozondatas")
COLL=os.getenv("PIVOT_PREFS_COLLECTION","pivot_prefs")

_client=MongoClient(MONGO_URI)
_db=_client[MONGO_DB]
_coll=_db[COLL]
_coll.create_index([("collection",ASCENDING),("name",ASCENDING)], unique=True)

class PrefsStore:
    def list(self, collection:str)->List[Dict[str,Any]]:
        return list(_coll.find({"collection":collection},{"_id":0,"collection":1,"name":1,"updatedAt":1}).sort("updatedAt",-1))
    
    def list_all(self)->List[Dict[str,Any]]:
        return list(_coll.find({},{"_id":0,"collection":1,"name":1,"updatedAt":1}).sort("updatedAt",-1))
    
    def get(self, collection:str, name:str)->Optional[Dict[str,Any]]:
        return _coll.find_one({"collection":collection,"name":name},{"_id":0})
    def save(self, body:Dict[str,Any])->bool:
        col=body.get("collection"); name=body.get("name")
        if not col or not name: return False
        body=dict(body)
        body["updatedAt"]=__import__('datetime').datetime.utcnow().isoformat()+"Z"
        _coll.update_one({"collection":col,"name":name},{"$set":body}, upsert=True)
        return True
    def delete(self, collection:str, name:str)->bool:
        _coll.delete_one({"collection":collection,"name":name}); return True
