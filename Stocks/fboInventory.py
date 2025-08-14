# encoding='utf-8

# @Time: 2025-08-14
# @File: %
#!/usr/bin/env
from icecream import ic
import os
import requests
import polars as pl


def genStocksQuanlity(headers):
    json_datas = {
        "limit": 1000,
        "offset": 0,
        "warehouse_type": "ALL"
    }
    response = requests.post(
        'https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses',
        headers=headers,
        json=json_datas,
    ).json()
    # ic(response)
    df = pl.DataFrame(response['result']['rows'])

    ic(df)
    return df

