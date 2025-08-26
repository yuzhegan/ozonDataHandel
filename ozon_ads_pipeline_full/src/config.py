from __future__ import annotations
import os, json
from dataclasses import dataclass, field
from typing import List, Optional
from dotenv import load_dotenv

load_dotenv()

@dataclass
class OzonCredential:
    client_id: str
    client_secret: str

@dataclass
class OzonConfig:
    base_url: str = os.getenv("OZON_BASE_URL", "https://api-performance.ozon.ru")
    # 兼容单凭证
    client_id: Optional[str] = os.getenv("OZON_CLIENT_ID")
    client_secret: Optional[str] = os.getenv("OZON_CLIENT_SECRET")
    # 多凭证（优先）
    clients: List[OzonCredential] = field(default_factory=list)
    timeout: int = int(os.getenv("HTTP_TIMEOUT", "30"))

    def __post_init__(self):
        clients_json = os.getenv("OZON_CLIENTS_JSON")
        id_list = os.getenv("OZON_CLIENT_ID_LIST")
        secret_list = os.getenv("OZON_CLIENT_SECRET_LIST")

        parsed: List[OzonCredential] = []
        if clients_json:
            try:
                arr = json.loads(clients_json)
                for item in arr:
                    cid = item.get("client_id")
                    sec = item.get("client_secret")
                    if cid and sec:
                        parsed.append(OzonCredential(client_id=cid, client_secret=sec))
            except Exception:
                pass
        elif id_list and secret_list:
            ids = [x.strip() for x in id_list.split(",") if x.strip()]
            secs = [x.strip() for x in secret_list.split(",") if x.strip()]
            for i, cid in enumerate(ids):
                if i < len(secs):
                    parsed.append(OzonCredential(client_id=cid, client_secret=secs[i]))

        if not parsed and self.client_id and self.client_secret:
            parsed = [OzonCredential(client_id=self.client_id, client_secret=self.client_secret)]

        if not parsed:
            raise RuntimeError("请在 .env 中配置 OZON_CLIENTS_JSON 或 OZON_CLIENT_ID/_SECRET（单个或列表）")

        self.clients = parsed

@dataclass
class MongoConfig:
    uri: str = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    db: str = os.getenv("MONGO_DB", "ozondatas")
    coll: str = os.getenv("MONGO_COLL", "mbcampagin")
    # coll2: str = os.getenv("MONGO_COLL", "mbcampagin")
