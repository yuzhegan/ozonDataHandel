# encoding='utf-8

# @Time: 2025-08-23
# @File: %
#!/usr/bin/env
from icecream import ic
import os
import requests
import json


headers = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en,en-US;q=0.9",
    "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJhZHZlcnRpc2luZy5wZXJmb3JtYW5jZS5vem9uLnJ1IiwiZXhwIjoxNzU2MDQ5ODUyLCJpYXQiOjE3NTYwNDgwNTIsImlzcyI6InBlcmZvcm1hbmNlLWF1dGgub3pvbi5ydSIsInN1YiI6IjU3ODMxMjAyLTE3NDE1NzMyMTU5MDdAYWR2ZXJ0aXNpbmcucGVyZm9ybWFuY2Uub3pvbi5ydSJ9.xPBbVSQoiITDKg2oSads4CVtC7_BpuvupUQ6mSJoXNQ",
    "cache-control": "no-cache",
    # "client-id": "57831202-1741573215907@advertising.performance.ozon.ru",
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

url = "https://api-performance.ozon.ru/api/client/statistic/products/generate"
# data = {
#     "from": "2025-08-20T14:15:22Z",
#     "to": "2025-08-20T14:15:22Z"
# }
data = {
    "dateFrom": "2025-08-20",
    "dateTo": "2025-08-20"
}
data = json.dumps(data, separators=(',', ':'))
response = requests.post(url, headers=headers, data=data)

print(response.json())
print(response)
