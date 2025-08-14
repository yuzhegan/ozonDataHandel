# encoding='utf-8

# @Time: 2025-08-12
# @File: %
#!/usr/bin/env
from icecream import ic
import os
from Feishu.feishu import query_records
from utils.feishu_records_polars import *
import polars as pl
import lark_oapi as lark
def genbaseinfo(baseinfo_table_url:str ,datas:list, app_id:str, app_secret:str):
    table_url = baseinfo_table_url
    client = build_client(app_id, app_secret)
    app_token, table_id = parse_app_and_table_from_url(table_url)
    print(f"app_token: {app_token}, table_id: {table_id}")
    field_schema = fetch_field_schema(client, app_token, table_id)
    records = search_records(client, app_token, table_id)  # or view_id=view_id
    new_records = []
    for date in datas:
        record = [ {**r, '日期': date } for r in records]
        new_records.extend(record)
    # print(new_records[:2])
    # exit()
    df = records_to_polars_by_schema(new_records, field_schema)
    # print(df)
    return df



# 从飞书中查询记录成本表记录,获取datafarme的基础信息需要的字段
def xxgenbaseinfo(dates: list, app_id: str='cli_a819ae6445685013', app_secret: str='WZVSbtc80PYSJjDr8CHZDgcbILzKgzW0'):
    assert isinstance(app_id, str) and isinstance(app_secret, str)
    records = query_records(
            table_url="https://xcn114pn5b7h.feishu.cn/base/N5cFb1e6Za8gShsw6gnc7FWYnod?table=tblz6DaA0SMDT69k&view=vew3Gt8Jkc",
            page_size=5000,
            app_id=app_id,
            app_secret= app_secret,
        )
    # 
    # print(records)
    # 处理记录，添加日期字段
    new_records = []
    for date in dates:
        record = [ {**r, '日期': date } for r in records] #每个记录添加日期字段
        new_records.extend(record)
    # print(new_records)
    # print(records)

    # print(f"Total records: {len(records)}")
    df = records_to_polars_by_schema(new_records)
    # df.write_csv('./utils/stock_fill.csv')
    # print(df.head())
    return df

# 为基础数据匹配FBO的库存上架库存,在途库存
import configparser
from Stocks.fboInventory import genStocksQuanlity

def fboInventory():
    cfg = configparser.ConfigParser()
    cfg.read('config.ini')

    client_id = cfg['ozon']['client_id']
    api_key = cfg['ozon']['api_key']
    headers = {
        "Api-Key": api_key,
        "Client-Id": client_id,
    }
    fboinventory = genStocksQuanlity(headers)
    return fboinventory

def overseasInventory(table_url, app_id: str, app_secret: str, list_text_strategy:''):
    # table_url = "https://xcn114pn5b7h.feishu.cn/base/N5cFb1e6Za8gShsw6gnc7FWYnod?table=tbl2O9SNHUeHmPfE&view=vew4IHfvZH"

    client = build_client(app_id, app_secret)
    app_token, table_id = parse_app_and_table_from_url(table_url)
    print(f"app_token: {app_token}, table_id: {table_id}")

    field_schema = fetch_field_schema(client, app_token, table_id)
    # print(f"field_schema: {field_schema}")

    # 如果你想按视图拉取，可以把 view_id 也解析出来传进去
    # view_id = parse_view_id_from_url(table_url)  # 可选
    records = search_records(client, app_token, table_id)  # or view_id=view_id
    print(records)

    df = records_to_polars_by_schema(records, field_schema)
    print(df)
    return df

if __name__ == "__main__":
    cfg = configparser.ConfigParser()
    cfg.read('config.ini')
    client_id = cfg['ozon']['client_id']
    api_key = cfg['ozon']['api_key']
    baseinfo_table_url = cfg['feishu']['baseinfo_table_url'].strip()
    print(baseinfo_table_url)
    # 1. 获取配置文件中的app_id和app_secret (https://open.feishu.cn/app/cli_a819ae6445685013/baseinfo)
    app_id=cfg['feishu']['app_id']  # 'cli_a819ae6445685013'
    app_secret=cfg['feishu']['app_secret']  # 'WZVSbtc80PYSJjDr8CHZDgcbILzKgzW0'
    # 1. 获取基础信息
    dates = ['2025-08-01', '2025-08-02'] 
    basicinfo = genbaseinfo(baseinfo_table_url, dates, app_id, app_secret).with_columns(
            pl.col('Ozon ID').cast(pl.Utf8),
            )
    # exit()
    # print(basicinfo['Ozon ID'].head(10))
    # print(basicinfo.schema)
    # 2. 获取FBO的库存信息 
    fboinventory1 = fboInventory()
    fboinventory1 = fboinventory1.with_columns(
            pl.col("sku").cast(pl.Utf8)
            )
    # print(fboinventory1.schema)
    # print(fboinventory1['sku'].head(10))
    # exit()
    fboinventory1.write_csv('./Stocks/fbo_inventory.csv')
    exit()
    promised_amount = fboinventory1.group_by('sku').agg(
         pl.col('free_to_sell_amount').sum().alias('FBO上架数量(万)'),
         pl.col('promised_amount').sum().alias('FBO越库在途数量(万)'),
            ).fill_null(0)
    print(promised_amount.head(10))
    # exit()
    # 3. 将FBO的库存信息与基础信息进行匹配
    basic_stock_fill = basicinfo.join(promised_amount, how='left', left_on='Ozon ID', right_on='sku').fill_null(0)
    # basic_stock_fill.write_csv('./Stocks/basic_stock_fill.csv')
    # exit()
    # print(basic_stock_fill.head(10))
    # 4.获取海外仓的库存
    overseas_table_url = cfg['feishu']['overseas_table_url']
    overseas_inventory = overseasInventory(overseas_table_url, app_id, app_secret, list_text_strategy='')
    # overseas_inventory.write_csv('./Stocks/overseas_inventory.csv')
    # 海外仓库存,聚合按照 sku 聚合, 数量 ,fnsku 是"OZNxxxxx" 去掉ozon
    overseas_inventory = overseas_inventory.with_columns(
        pl.col('fnsku').cast(pl.Utf8).str.replace('OZN', '').replace('"', '').alias('fnsku')
    ).group_by('fnsku').agg(
        pl.col('数量').sum().alias('海外仓在途数量(万)'),
    ).fill_null(0)
    print(overseas_inventory.head(10))


    # 合并basic_stock_fill和overseas_inventory
    basic_stock_fill_seasinventory = basic_stock_fill.join(overseas_inventory, how='left', left_on='Ozon ID', right_on='fnsku').fill_null(0)
    # basic_stock_fill_seasinventory.write_csv('./Stocks/basic_stock_fill_seasinventory.csv')
    # 5. 获取海外仓的在途库存 头程库存
    toucheng_table_url = cfg['feishu']['toucheng_table_url']
    overseas_toucheng_inventory = overseasInventory(toucheng_table_url, app_id, app_secret, list_text_strategy='json')
    overseas_toucheng_inventory = overseas_toucheng_inventory.with_columns(
            pl.col('fnsku').cast(pl.Utf8).str.replace('OZN', '').replace('"', '').alias('fnsku')
            )
    overseas_toucheng_inventory = overseas_toucheng_inventory.with_columns(
        pl.col("数量").cast(pl.Float64, strict=False)
    )

    overseas_toucheng_inventory = overseas_toucheng_inventory.pivot(
        values="数量",
        index=["fnsku"],       # 可以是 str 或 list[str]
        on="渠道更新",     # 这里必须是单个列名（str）
        aggregate_function="sum",   # 聚合函数用这个参数名
    ).fill_null(0)
    # overseas_toucheng_inventory.write_csv('./Stocks/overseas_toucheng_inventory.csv')
    if "7日达" in overseas_toucheng_inventory.columns:
        overseas_toucheng_inventory = overseas_toucheng_inventory.rename(
                {"7日达": '7日达在途数量(万)'}
        )
    elif '普快' in overseas_toucheng_inventory.columns:
        overseas_toucheng_inventory = overseas_toucheng_inventory.rename(
                {"普快": '普快在途数量(万)'}
        )
    elif '普慢' in overseas_toucheng_inventory.columns:
        overseas_toucheng_inventory = overseas_toucheng_inventory.rename(
                {"普慢": '普慢在途数量(万)'}
        )
    # 合并basic_stock_fill_seasinventory和overseas_toucheng_inventory
    basic_stock_fill_seasinventory_toucheng = basic_stock_fill_seasinventory.join(
        overseas_toucheng_inventory, how='left', left_on='Ozon ID', right_on='fnsku'
    ).fill_null(0)
    basic_stock_fill_seasinventory_toucheng.write_csv('./Stocks/basic_stock_fill_seasinventory_toucheng.csv')





