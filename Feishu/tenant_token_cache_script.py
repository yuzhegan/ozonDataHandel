# -*- coding: utf-8 -*-
"""
Fetch and cache a Feishu/Lark tenant access token with expiration check.

This script encapsulates the logic for retrieving a tenant access token
(`tenant_access_token`) from the Feishu (或 Lark) API, caching it on disk,
and re‑using the cached token until it expires.  If the cached token is
still valid (i.e. the current time is before its stored ``expire_at``),
the script will simply return the cached token.  Otherwise, it will
request a new token, update the cache, and return the fresh token.

Usage (run from command line)::

    export FEISHU_APP_ID='cli_a819ae6445685013'
    export FEISHU_APP_SECRET='WZVSbtc80PYSJjDr8CHZDgcbILzKgzW0'
    python tenant_token_cache_script.py

The script will print a masked version of the token and its expiration
seconds.  To change the API domain (e.g. for Lark), set
``FEISHU_DOMAIN=https://open.larksuite.com``.

Caching details:
* Cache file name is ``.tenant_token_cache.json`` in the current
  working directory.  This JSON contains ``tenant_access_token``,
  ``expire``, ``expire_at``, ``app_id`` and ``domain``.
* If the cache exists and ``expire_at`` is in the future, the token
  is re‑used.
* Otherwise the script calls the Feishu API to obtain a new token.

Security note:  Do not hardcode your App ID or App Secret in this
file.  Use environment variables or pass them programmatically.  See
the documentation for more details on the API endpoint【812910652656384†L667-L709】.
"""

import json
import os
import time
import requests
from typing import Optional, Dict


CACHE_FILENAME = ".tenant_token_cache.json"


def fetch_tenant_token(app_id: str, app_secret: str, domain: str) -> Dict[str, object]:
    """Call Feishu API to obtain a new tenant access token.

    Parameters
    ----------
    app_id:
        Application ID for your Feishu/Lark internal app.
    app_secret:
        Application secret for your Feishu/Lark internal app.
    domain:
        Base domain for the Open API (e.g. ``https://open.feishu.cn`` or
        ``https://open.larksuite.com``).

    Returns
    -------
    dict
        A dictionary containing the token, expiry seconds, and absolute
        expiry timestamp.
    """
    url = f"{domain}/open-apis/auth/v3/tenant_access_token/internal"
    resp = requests.post(url, json={"app_id": app_id, "app_secret": app_secret})
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"Error from Feishu API: code={data.get('code')} msg={data.get('msg')}")
    expire_seconds = int(data["expire"])
    expire_at = int(time.time()) + expire_seconds
    return {
        "tenant_access_token": data["tenant_access_token"],
        "expire": expire_seconds,
        "expire_at": expire_at,
    }


def load_cache() -> Optional[Dict[str, object]]:
    """Load token data from the cache file, if it exists and is valid JSON."""
    if not os.path.exists(CACHE_FILENAME):
        return None
    try:
        with open(CACHE_FILENAME, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        # Corrupted cache; ignore it
        return None


def save_cache(data: Dict[str, object]) -> None:
    """Save token data to the cache file."""
    try:
        with open(CACHE_FILENAME, "w", encoding="utf-8") as f:
            json.dump(data, f)
    except Exception:
        # Do not propagate cache writing errors
        pass


def get_tenant_token(
    app_id: str,
    app_secret: str,
    domain: str,
) -> Dict[str, object]:
    """Retrieve a tenant access token, using the cache when possible.

    The function checks the cache file (``.tenant_token_cache.json``)
    for an existing token.  If the token exists and has not expired,
    it is returned.  Otherwise, the API is called to obtain a fresh
    token, which is then stored in the cache.

    Parameters
    ----------
    app_id:
        Application ID for the internal app.
    app_secret:
        Application secret for the internal app.
    domain:
        Base domain for the Open API.

    Returns
    -------
    dict
        A dictionary containing the token, expiry seconds, expiry
        timestamp and also includes ``app_id`` and ``domain`` for
        convenience.
    """
    now = int(time.time())
    cached = load_cache()
    # Use cached token if valid and corresponds to same app_id and domain
    if (
        cached
        and cached.get("app_id") == app_id
        and cached.get("domain") == domain
        and isinstance(cached.get("expire_at"), int)
        and cached["expire_at"] > now
    ):
        return cached

    # Otherwise fetch a new token
    token_data = fetch_tenant_token(app_id, app_secret, domain)
    token_data.update({"app_id": app_id, "domain": domain})
    save_cache(token_data)
    return token_data


if __name__ == "__main__":
    # Read credentials from environment variables for security
    exit()
    APP_ID = 'cli_a819ae6445685013'
    APP_SECRET = 'WZVSbtc80PYSJjDr8CHZDgcbILzKgzW0'
    DOMAIN = os.getenv("FEISHU_DOMAIN", "https://open.feishu.cn")
    if not APP_ID or not APP_SECRET:
        raise SystemExit(
            "Please set FEISHU_APP_ID and FEISHU_APP_SECRET environment variables."
        )
    token_info = get_tenant_token(APP_ID, APP_SECRET, DOMAIN)
    token = token_info["tenant_access_token"]
    # Mask token in output to avoid accidental exposure
    print("tenant_access_token:", token)
    print("expires_in_sec:", token_info["expire"])
