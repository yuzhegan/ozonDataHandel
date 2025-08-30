# encoding='utf-8

# @Time: 2025-08-27
# @File: %
#!/usr/bin/env
from icecream import ic
import os
import requests
import polars as pl
import json
# client_id = os.getenv('OZON_CLIENT_ID')
client_id = '1654428'
api_key = '316033f2-f1c3-4a9e-a61c-6268f8c5b4a5'

def genSizefapi(client_id, api_key):
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
    url = "https://api-seller.ozon.ru/v4/product/info/attributes"
    data = {
        "filter": {
            "sku": [],
            "visibility": "ALL"
        },
        "limit": 200,
        "sort_dir": "ASC"
    }
    data = json.dumps(data, separators=(',', ':'))
    response = requests.post(url, headers=headers, data=data).json()
    result = response.get('result', [])
    size_infos = [{'offer_id':item.get('offer_id'),'sku':item.get('sku', ''),'height': item.get('height', 0), 'depth': item.get('depth', 0), 'width': item.get('width', 0), 'weight': item.get('weight', 0), 'dimension_unit': item.get('dimension_unit',"mm"), "weight_unit": item.get('weight_unit', 'g')} for item in result]
    df = pl.DataFrame(size_infos)
    df = df.with_columns(
            (pl.col('depth')*pl.col('width')*pl.col('height')/1000000).alias('volume_rise').round(2),
            )
    df.write_csv('Finance/ozon_size_full.csv')
    df = df.select(['sku', 'volume_rise'])
    return df
if __name__ == "__main__":
    exit()
    # df = genSizefapi(client_id, api_key)
    # ic(df)


