# encoding='utf-8

# @Time: 2025-08-20
# @File: %
#!/usr/bin/env
from icecream import ic
import os
import requests
import json

def warehouse2cluster(api_key: str, client_id:str):
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en,en-US;q=0.9",
        "api-key": api_key,
        "cache-control": "no-cache",
        "client-id": client_id,
        "content-type": "application/json",
        "origin": "https://docs.ozon.ru",
        "pragma": "no-cache",
        "priority": "u=1, i",
        "referer": "https://docs.ozon.ru/",
        "sec-ch-ua": "\"Not;A=Brand\";v=\"99\", \"Google Chrome\";v=\"139\", \"Chromium\";v=\"139\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"macOS\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        "x-o3-app-name": "sandbox-doc-api"
    }
    url = "https://api-seller.ozon.ru/v1/cluster/list"
    data = {
        "cluster_ids": [],
        "cluster_type": "CLUSTER_TYPE_OZON"
    }
    data = json.dumps(data, separators=(',', ':'))
    response = requests.post(url, headers=headers, data=data)
    try:
        j = response.json()
        return j.get("clusters", [])
    except json.JSONDecodeError:
        ic(f"Error decoding JSON response: {response.text}")
        return []




