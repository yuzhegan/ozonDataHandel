from pydantic import BaseModel, Field
from typing import Any, Dict, Optional

class QueryRequest(BaseModel):
    collection: str
    filters: Optional[Dict[str, Any]] = Field(default_factory=dict)
    limit: int = 5000
    skip: int = 0
    projection: Optional[Dict[str,int]] = Field(default_factory=lambda: {"_id":0})
