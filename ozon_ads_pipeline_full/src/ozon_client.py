from __future__ import annotations
import time, re, requests
from typing import Any, Dict, List, Optional
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from .config import OzonConfig, OzonCredential

UUID_RE = re.compile(r'^[0-9a-fA-F-]{36}$')

class RateLimitError(Exception):
    pass

class OzonClient:
    """
    支持多组 client_id/client_secret 轮换：
    - 初始化时按顺序尝试获取 token；
    - 请求遇到 429 时，切换到下一组凭证，获取新 token 后重试；
    - 同步更新 'client-id' 头与 Authorization；
    - 401 时对当前凭证刷新 token 并重试一次；
    - 网络异常走 tenacity 自动重试（不改变凭证）。
    """
    def __init__(self, cfg: OzonConfig):
        self.cfg = cfg
        self.creds: List[OzonCredential] = cfg.clients
        self.cred_idx: int = 0
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "ozon-performance-client/0.4-rotating",
        })
        self.access_token: Optional[str] = None
        self.token_type: str = "Bearer"
        self.expires_at: Optional[float] = None
        ok = self._switch_to_first_working_credential()
        if not ok:
            raise RuntimeError("所有凭证都无法获取 token，请检查 .env 配置或网络")

    # ===== credentials =====
    def _current_cred(self) -> OzonCredential:
        return self.creds[self.cred_idx]

    def _advance_cred(self):
        self.cred_idx = (self.cred_idx + 1) % len(self.creds)

    def _switch_to_first_working_credential(self) -> bool:
        tried = 0
        start_idx = self.cred_idx
        while tried < len(self.creds):
            if self._fetch_token_for_current(rotate_header=True, suppress_429=True):
                return True
            self._advance_cred()
            tried += 1
        self.cred_idx = start_idx
        return False

    def _fetch_token_for_current(self, rotate_header: bool = True, suppress_429: bool = False) -> bool:
        cred = self._current_cred()
        if rotate_header:
            self.session.headers["client-id"] = cred.client_id
        url = self.cfg.base_url.rstrip('/') + "/api/client/token"
        payload = {"client_id": cred.client_id, "client_secret": cred.client_secret, "grant_type": "client_credentials"}
        try:
            r = requests.post(url, json=payload, timeout=self.cfg.timeout)
            if r.status_code == 429 and suppress_429:
                return False
            r.raise_for_status()
            data = r.json()
            token = data.get("access_token")
            if not token:
                return False
            self.token_type = data.get("token_type", "Bearer")
            self.access_token = token
            self.session.headers["Authorization"] = f"{self.token_type} {self.access_token}"
            expires_in = data.get("expires_in")
            self.expires_at = (time.time() + float(expires_in) - 30) if isinstance(expires_in, (int, float)) else None
            return True
        except requests.RequestException:
            return False

    def _rotate_credential_and_fetch_token(self) -> bool:
        self._advance_cred()
        return self._fetch_token_for_current(rotate_header=True, suppress_429=False)

    def _ensure_token(self):
        if self.expires_at and time.time() >= self.expires_at:
            if not self._fetch_token_for_current(rotate_header=True, suppress_429=False):
                if not self._rotate_credential_and_fetch_token():
                    raise RuntimeError("无法刷新或轮换凭证获取 token")

    # ===== request layer =====
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=6),
           retry=retry_if_exception_type(requests.RequestException))
    def _request_json(self, method: str, path: str, *, json_data: Dict[str, Any] | None = None,
                      params: Dict[str, Any] | None = None) -> Dict[str, Any]:
        self._ensure_token()
        url = self.cfg.base_url.rstrip('/') + path

        tried_creds = 0
        while tried_creds < len(self.creds):
            resp = self.session.request(method, url, json=json_data, params=params, timeout=self.cfg.timeout)
            print(f"请求头 {self.session.headers}, 请求方法 {method}, URL {url}, JSON数据 {json_data}, 参数 {params}")
            if resp.status_code == 401:
                if self._fetch_token_for_current(rotate_header=True, suppress_429=False):
                    resp = self.session.request(method, url, json=json_data, params=params, timeout=self.cfg.timeout)
                else:
                    if not self._rotate_credential_and_fetch_token():
                        resp.raise_for_status()
                        return resp.json()
                    resp = self.session.request(method, url, json=json_data, params=params, timeout=self.cfg.timeout)

            if resp.status_code == 429:
                tried_creds += 1
                if not self._rotate_credential_and_fetch_token():
                    continue
                # 简单退避
                time.sleep(0.8 * tried_creds)
                continue

            resp.raise_for_status()
            try:
                return resp.json()
            except ValueError:
                return {"raw": resp.text}

        raise RateLimitError("所有凭证均被限流（429），请增加更多凭证或延长时间间隔")

    def _get(self, path: str, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
        return self._request_json("GET", path, params=params)

    def _post(self, path: str, json_data: Dict[str, Any] | None = None, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
        return self._request_json("POST", path, json_data=json_data, params=params)

    # ===== business wrappers =====
    def campaigns(self) -> List[Dict[str, Any]]:
        data = self._get("/api/client/campaign")
        if isinstance(data, dict) and isinstance(data.get("list"), list):
            return data["list"]
        if isinstance(data, list):
            return data
        raise RuntimeError(f"未知的 campaigns 响应结构: {data}")

    def request_statistics_json(self, campaign_ids: List[str], date_from: str, date_to: str, group_by: str = "DATE") -> List[str]:
        payload = {"campaigns": [str(c) for c in campaign_ids], "dateFrom": date_from, "dateTo": date_to, "groupBy": group_by}
        data = self._post("/api/client/statistics/json", json_data=payload)
        uuids: List[str] = []
        def walk(o):
            if isinstance(o, dict):
                for vv in o.values():
                    yield from walk(vv)
            elif isinstance(o, list):
                for vv in o:
                    yield from walk(vv)
            elif isinstance(o, str) and UUID_RE.match(o):
                yield o
        uuids = list(dict.fromkeys(walk(data)))
        if not uuids:
            raise RuntimeError(f"未能解析出 UUID：{data}")
        return uuids

    def report_status(self, uuid: str) -> Dict[str, Any]:
        return self._get(f"/api/client/statistics/{uuid}")

    def download_report(self, uuid: str) -> Dict[str, Any]:
        return self._get("/api/client/statistics/report", params={"UUID": uuid})
