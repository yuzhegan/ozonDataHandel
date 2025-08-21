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
    #取成本明细df表前5列
    return df

# step1.1 获取集群表格信息
def gen_cluster_dict(cluster_dict_table_url, app_id, app_secret):
    """
    获取集群表格信息
    :param cluster_dict_table_url: 集群表格的URL
    :param app_id: 飞书应用的app_id
    :param app_secret: 飞书应用的app_secret
    :return: 返回集群信息的DataFrame
    """
    client = build_client(app_id, app_secret)
    app_token, table_id = parse_app_and_table_from_url(cluster_dict_table_url)
    field_schema = fetch_field_schema(client, app_token, table_id)
    records = search_records(client, app_token, table_id)
    df = records_to_polars_by_schema(records, field_schema)
    try:
        df = df.drop('record_id')
    except Exception as e:
        print(f"Error dropping record_id: {e}")
    return df

# step5 获取集群表格信息
def gen_cluster_safedays(cluster_safe_days_table_url, app_id, app_secret):
    """
    获取集群表格信息
    :param cluster_dict_table_url: 集群表格的URL
    :param app_id: 飞书应用的app_id
    :param app_secret: 飞书应用的app_secret
    :return: 返回集群信息的DataFrame
    """
    client = build_client(app_id, app_secret)
    app_token, table_id = parse_app_and_table_from_url(cluster_safe_days_table_url)
    field_schema = fetch_field_schema(client, app_token, table_id)
    records = search_records(client, app_token, table_id)
    df = records_to_polars_by_schema(records, field_schema)
    df = df.select(["集群中俄", "FBO安全天数"])
    df = df.with_columns(
            pl.col('FBO安全天数').cast(pl.Int64, strict=False).alias('FBO安全天数'),
            )
    try:
        df = df.drop('record_id')
    except Exception as e:
        print(f"Error dropping record_id: {e}")
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

def genfeishu_TableDatas(table_url, app_id: str, app_secret: str, list_text_strategy:''):
    # table_url = "https://xcn114pn5b7h.feishu.cn/base/N5cFb1e6Za8gShsw6gnc7FWYnod?table=tbl2O9SNHUeHmPfE&view=vew4IHfvZH"

    client = build_client(app_id, app_secret)
    app_token, table_id = parse_app_and_table_from_url(table_url)
    print(f"app_token: {app_token}, table_id: {table_id}")

    field_schema = fetch_field_schema(client, app_token, table_id)
    # print(f"field_schema: {field_schema}")

    # 如果你想按视图拉取，可以把 view_id 也解析出来传进去
    # view_id = parse_view_id_from_url(table_url)  # 可选
    records = search_records(client, app_token, table_id)  # or view_id=view_id

    df = records_to_polars_by_schema(records, field_schema)
    return df

# 1. 获取基础信息
def step1_genbasic(dates):
    baseinfo_table_url = _cfg['feishu']['baseinfo_table_url']
    app_id = _cfg['feishu']['app_id']
    app_secret = _cfg['feishu']['app_secret']
    basicinfo = genbaseinfo(baseinfo_table_url, dates, app_id, app_secret).with_columns(
            pl.col('Ozon ID').cast(pl.Utf8),
            )
    return basicinfo
# 2. 获取FBO的库存信息 
def step2_fboInventory(group_by=['sku']):
    api_key = _cfg['ozon']['api_key']
    client_id = _cfg['ozon']['client_id']
    fboinventory1 = fboInventory()
    # print(fboinventory1.head(5))
    fboinventory1 = fboinventory1.with_columns(
            pl.col("sku").cast(pl.Utf8)
            )
    if 'warehouse_name' in group_by:
        from Stocks.warehose2cluster import warehouse2cluster  #ozon官方接口仓库匹配对应的集群
        cluster = warehouse2cluster(api_key, client_id) #json格式的集群信息 \
        #cluster[{'id':154,'logistic_clusters':[], 'name':"Москва, МО и Дальние регионы", "type":'ozon'}]
        ic("f获取到{len(cluster)}条数据")
        from utils.warehouse_name2cluster import add_cluster_name_column_deep
        fboinventory1 = add_cluster_name_column_deep(fboinventory1, cluster)
        # fboinventory1.write_csv('./Stocks/fboinventory1.csv')
        group_by = ['sku','cluster']
    promised_amount = fboinventory1.group_by(group_by).agg(
         pl.col('free_to_sell_amount').sum().alias('FBO上架数量(万)'),
         pl.col('promised_amount').sum().alias('FBO越库在途数量(万)'),
            ).fill_null(0)
    return promised_amount

# 3. 将FBO的库存信息与基础信息进行匹配
def step3_basic_stock_fill(basicinfo, promised_amount, cluster:bool=False):
    if not cluster:
        basic_stock_fill = basicinfo.join(promised_amount, how='left', left_on='Ozon ID', right_on='sku').fill_null(0)
    else:
        basic_stock_fill = basicinfo.join(promised_amount, how='left', left_on=['Ozon ID', '集群'], right_on=['sku', 'cluster']).fill_null(0)
    

    return basic_stock_fill

# 4.获取海外仓的库存
def step4_overseas_inventory():
    overseas_table_url = _cfg['feishu']['overseas_table_url']
    app_id = _cfg['feishu']['app_id']
    app_secret = _cfg['feishu']['app_secret']
    overseas_inventory = genfeishu_TableDatas(overseas_table_url, app_id, app_secret, list_text_strategy='')
    overseas_inventory = overseas_inventory.with_columns(
        pl.col('fnsku').cast(pl.Utf8).str.replace('OZN', '').replace('"', '').alias('fnsku')
    ).group_by('fnsku').agg(
        pl.col('数量').sum().alias('海外仓在库数量(万)'),
    ).fill_null(0)
    return overseas_inventory

# 5.合并basic_stock_fill和overseas_inventory
def step5_basic_stock_fill_seasinventory(basic_stock_fill, overseas_inventory):
    basic_stock_fill_seasinventory = basic_stock_fill.join(overseas_inventory, how='left', left_on='Ozon ID', right_on='fnsku').fill_null(0)
    return basic_stock_fill_seasinventory
# 6. 获取海外仓的在途库存 头程库存
def GenoverseasInventory():
    app_id = _cfg['feishu']['app_id']
    app_secret = _cfg['feishu']['app_secret']
    toucheng_table_url = _cfg['feishu']['toucheng_table_url']
    overseas_toucheng_inventory = genfeishu_TableDatas(toucheng_table_url, app_id, app_secret, list_text_strategy='json')
    overseas_toucheng_inventory = overseas_toucheng_inventory.with_columns(
            pl.col('fnsku').cast(pl.Utf8).str.replace('OZN', '').replace('"', '').alias('fnsku')
            )
    overseas_toucheng_inventory = overseas_toucheng_inventory.with_columns(
        pl.col("数量").cast(pl.Int64, strict=False)
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
    return overseas_toucheng_inventory
# 7.合并basic_stock_fill_seasinventory和overseas_toucheng_inventory
def basic_stock_fill_seasinventory_toucheng(basic_stock_fill_seasinventory, overseas_toucheng_inventory):
    basic_stock_fill_seasinventory_toucheng = basic_stock_fill_seasinventory.join(
        overseas_toucheng_inventory, how='left', left_on='Ozon ID', right_on='fnsku'
    ).fill_null(0)
    return basic_stock_fill_seasinventory_toucheng
# 8. 获取本地仓库存
def local_inventory():
    app_id = _cfg['feishu']['app_id']
    app_secret = _cfg['feishu']['app_secret']
    local_table_url = _cfg['feishu']['local_table_url']
    local_inventory = genfeishu_TableDatas(local_table_url, app_id, app_secret, list_text_strategy='')
    local_inventory = local_inventory.with_columns(
        pl.col('fnsku').cast(pl.Utf8).str.replace('OZN', '').replace('"', '').alias('fnsku')
    ).group_by('fnsku').agg(
        pl.col('数量').sum().alias('本地仓库存数量(万)'),
    ).fill_null(0)
    return local_inventory
# 9.合并basic_stock_fill_seasinventory_toucheng和local_inventory
def basic_stock_fill_seasinventory_toucheng_local(basic_stock_fill_seasinventory_toucheng, local_inventory):
    basic_stock_fill_seasinventory_toucheng_local = basic_stock_fill_seasinventory_toucheng.join(
        local_inventory, how='left', left_on='Ozon ID', right_on='fnsku'
    ).fill_null(0)
    return basic_stock_fill_seasinventory_toucheng_local

# 10. 获取采购在途库存
def purchasedInventory():
    app_id = _cfg['feishu']['app_id']
    app_secret = _cfg['feishu']['app_secret']
    purchased_table_url = _cfg['feishu']['purchased_table_url']
    purchased_inventory = genfeishu_TableDatas(purchased_table_url, app_id, app_secret, list_text_strategy='')
    purchased_inventory = purchased_inventory.with_columns(
        pl.col('fnsku').cast(pl.Utf8).str.replace('OZN', '').replace('"', '').alias('fnsku')
    ).group_by('fnsku').agg(
        pl.col('数量').sum().alias('已采购在途数量(万)'),
    ).fill_null(0)
    return purchased_inventory
# 11.合并basic_stock_fill_seasinventory_toucheng_local和purchase_inventory
def basic_stock_fill_seasinventory_toucheng_local_purchase(basic_stock_fill_seasinventory_toucheng_local, purchased_inventory):
    basic_stock_fill_seasinventory_toucheng_local_purchase = basic_stock_fill_seasinventory_toucheng_local.join(
        purchased_inventory, how='left', left_on='Ozon ID', right_on='fnsku'
    ).fill_null(0)
    return basic_stock_fill_seasinventory_toucheng_local_purchase
# 12.获取各个窗口的销量,计算加权日均销量
def step12_sales_weighted_average(date_field:str="正在处理中", target_dates:list=[], cluster:bool=False):
    from Orders.stock_orders import OrderSummaryGenerator, OrderSummaryConfig
    result = []
    for date in target_dates:
        confg = OrderSummaryConfig(
            mongo_uri=_cfg['mongodb']['mongo_uri'],
            db_name=_cfg['mongodb']['db_name'],
            coll_name=_cfg['mongodb']['coll_name'],
            # date_field="正在处理中",
            date_field=date_field,
            agg_field="数量",
            windows=(7, 14, 28, 60, 90),
            timezone="Asia/Seoul",
            date_format="%Y-%m-%d %H:%M:%S",
            # target_date="2025-08-07",
            target_date=date,
            top_k=3,
            peak_days_back=28, #28天的最高销量
            # 如果将来有别的维度也想在日销/峰值阶段去掉，可在这里加上
            drop_keys_for_daily=("货号"),
        )
        gen = OrderSummaryGenerator(confg)
        # 场景1：按“货号 + Ozon ID”
        if cluster:
            windowsales = gen.generate_by_sku_ozon_id_and_cluster()
        else:
            windowsales = gen.generate_by_sku_and_ozon_id()
        result.append(windowsales)
    df = pl.concat(result)
    import ast
    raw_rename = _cfg['columns']['rename_dict']   # 现在是字符串
    rename_dict = ast.literal_eval(raw_rename)   # 转成真正的 dict
    # 按集群groupby 没有max_daily_sales,需要删除这个元素
    if cluster:
        rename_dict.pop('max_daily_sales', None)
    df = plrenameColumns(df, rename_dict)

    return df

def plrenameColumns(df: pl.DataFrame, rename_dict: dict):
    """
    重命名DataFrame的列
    :param df: 输入的DataFrame
    :param rename_dict: 重命名字典，键为旧列名，值为新列名
    :return: 重命名后的DataFrame
    """
    return df.rename(rename_dict)
from typing import Union
def calcu_available_days(df, safe_days:Union[int, bool]=60, cluster:bool=False):
    """
    计算可用天数
    :param df: 输入的DataFrame 小数向下取整
    :return: 添加了可用天数列的DataFrame
    """
    df = df.with_columns(
        ((pl.col('FBO上架数量(万)') / pl.col('每日销量'))
         .fill_nan(0.0)                          # NaN -> 0
         .cast(pl.Int64, strict=False)           # 非法转换变 null
         .fill_null(0))                           # null -> 0
        .alias('FBO上架可售天数'),
        ((pl.col('FBO越库在途数量(万)') / pl.col('每日销量'))
         .fill_nan(0)
         .cast(pl.Int64, strict=False)           # 非法转换变 null
         .fill_null(0))                           # null -> 0
        .alias('FBO越库可售天数'),
        # ((pl.col('海外仓在库数量(万)') / pl.col('每日销量')).cast(pl.Int64))
        #     .alias('海外仓可售天数'),
        # ((pl.col('7日达在途数量(万)') / pl.col('每日销量')).cast(pl.Int64))
        #     .alias('7日到达可售天数'),
    )
    if cluster: #集群的安全天数是从表里读取,上面函数已经处理,不用在设
        df = df.with_columns(
                (pl.col('FBO越库可售天数') + pl.col('FBO上架可售天数'))
                .cast(pl.Int64, strict=False).fill_null(0).alias('FBO越库可售天数'),
                )
        return df
    if '海外仓在库数量(万)' in df.columns:
        df = df.with_columns(
                (pl.col('海外仓在库数量(万)') / pl.col('每日销量'))
                .fill_nan(0.0)                          # NaN -> 0
                .cast(pl.Int64, strict=False)           # 非法转换变 null
                .fill_null(0)                           # null -> 0
                .alias('海外仓可售天数')
            )
    if '7日达在途数量(万)' in df.columns:
        df = df.with_columns(
            ((pl.col('7日达在途数量(万)') / pl.col('每日销量'))
             .fill_nan(0.0)                          # NaN -> 0
             .cast(pl.Int64, strict=False)           # 非法转换变 null
             .fill_null(0))                           # null -> 0
            .alias('7日到达可售天数'),
        )
    if '普快在途数量(万)' in df.columns:
        df = df.with_columns(
            ((pl.col('普快在途数量(万)') / pl.col('每日销量'))
             .fill_nan(0.0)                          # NaN -> 0
             .cast(pl.Int64, strict=False)           # 非法转换变 null
             .fill_nan(0))                           # null -> 0
            .alias('普快可售天数'),
        )
    if '普慢在途数量(万)' in df.columns:
        print("True")
        df = df.with_columns(
            ((pl.col('普慢在途数量(万)') / pl.col('每日销量'))
             .fill_nan(0.0)                          # NaN -> 0
             .cast(pl.Int64, strict=False)           # 非法转换变 null
             .fill_null(0))                           # null -> 0
                .alias('普慢可售天数'),
        )
    df = df.with_columns(
        ((pl.col('本地仓库存数量(万)') / pl.col('每日销量'))
         .fill_nan(0.0)                          # NaN -> 0
         .cast(pl.Int64, strict=False)           # 非法转换变 null
         .fill_null(0))                           # null -> 0
        .alias('本地库存可售天数'),
        ((pl.col('已采购在途数量(万)') / pl.col('每日销量'))
         .fill_nan(0.0)                          # NaN -> 0
         .cast(pl.Int64, strict=False)           # 非法转换变 null
         .fill_null(0))                           # null -> 0
        .alias('已采购可售天数'),
    )
    #需要累加的列名
    inx_ = df.columns.index('FBO上架可售天数')  # 找到'fbo上架可售天数'的索引
    neds_cumulative_cols =  df.columns[inx_:]
    print("需要累加的列名:", neds_cumulative_cols)
    # 计算累计可售天数
    df = calculate_cumulative_days(df, neds_cumulative_cols)
    if isinstance(safe_days, bool) and safe_days is False: #集群的安全天数是从表里读取,上面函数已经处理,不用在设
        return df
    if isinstance(safe_days, int): #供应链安全天数是一个整数
        df = df.with_columns(
                pl.lit(int(safe_days)).alias('供应链安全天数'),
                )
    return df
def calculate_cumulative_days(df: pl.DataFrame, cols: list) -> pl.DataFrame:
    """
    根据传入的列名列表，计算累计可售天数。
    每一列 = 原列 + 前一列累计值
    :param df: 输入的Polars DataFrame
    :param cols: 需要累加的列名列表，顺序要和逻辑一致
    :return: 返回更新后的DataFrame
    """
    if not cols:
        return df

    # 初始化第一列
    cum_expr = pl.col(cols[0])
    df = df.with_columns(cum_expr.alias(cols[0]))

    # 从第二列开始依次累加
    for i in range(1, len(cols)):
        df = df.with_columns(
            (pl.col(cols[i]) + pl.col(cols[i-1])).alias(cols[i])
        )
    
    return df
def promotable_quantity(df):
    """
    计算可促销数量
    :param df: 输入的DataFrame
    :return: 添加了可上架数量列的DataFrame
    """
    df = df.with_columns(
        (
            pl.when(pl.col('已采购可售天数') - pl.col('供应链安全天数') > 0)
            .then((pl.col('已采购可售天数') - pl.col('供应链安全天数')) * pl.col('每日销量'))
            .otherwise(0)
        ).alias('可促销数量')
    )
    return df

def required_purchase_quantity(df):
    """
    计算需采购数量
    :param df: 输入的DataFrame
    :return: 添加了需采购数量列的DataFrame
    """
    overseas_sales_days = int(_cfg['inventorytable']['overseas_safe_days'])  # 海外仓补货安全天数,最近28天最大销量的多少倍
    toucheng_sales_days = int(_cfg['inventorytable']['toucheng_safe_days'])  # 头程补货安全天数,每日销量的多少倍
    df = df.with_columns(
        (pl.col('28日内销量最大值') * overseas_sales_days).alias('海外仓需备货(万)'), #step1 海外仓需采购数量
        ((pl.col('供应链安全天数') - pl.col('已采购可售天数'))* pl.col('每日销量')).alias('今日FBO需采购(万)'),
    # step2 计算FBO需采购数量
        (pl.col('每日销量') * toucheng_sales_days).alias('在途需采购(万)'), #step3 在途需要采购数量
    )
    # step4 计算总共需要采购的数量
    df = df.with_columns(
        pl.when((pl.col('海外仓需备货(万)') + pl.col('今日FBO需采购(万)') + pl.col('在途需采购(万)')) > 0).then(
            pl.col('海外仓需备货(万)') + pl.col('今日FBO需采购(万)') + pl.col('在途需采购(万)')
            ).otherwise(0).alias('今日需采购数量(万)'),
    )

    return df
def fill_null_with_zero(df: pl.DataFrame) -> pl.DataFrame:
    columns = df.columns
    if "普快在途数量(万)" not in columns:
        df = df.with_columns(
            pl.lit(0).cast(pl.Int64).alias('普快在途数量(万)')
        )
    if "普慢在途数量(万)" not in columns:
        df = df.with_columns(
            pl.lit(0).cast(pl.Int64).alias('普慢在途数量(万)')
        )
    return df

# 计算总货值
def gentotal_value(df):
    """
    计算总货值
    :param df: 输入的DataFrame
    :return: 添加了总货值列的DataFrame
    """
    cost_table_url = (_cfg['feishu']['cost_table_url']) #成本表的url
    app_id = _cfg['feishu']['app_id']
    app_secret = _cfg['feishu']['app_secret']
    rate = float(_cfg['RUB']['rate'])  # 汇率
    import ast
    total_vat_columns = _cfg['columns']['total_vat_columns']   # 现在是字符串
    total_vat_columns = ast.literal_eval(total_vat_columns)   # 转成真正的 列表
    total_value_columns = _cfg['columns']['total_value_columns']   # 现在是字符串
    total_value_columns = ast.literal_eval(total_value_columns)   # 转成真正的 列表
    print(f"total_vat_columns: {total_vat_columns}, total_value_columns: {total_value_columns}")
    cost_df = genfeishu_TableDatas(cost_table_url, app_id, app_secret, list_text_strategy='')
    # 需要计算vat的是产品成本
    cost_vat_df = cost_df.select(['Ozon ID', '成本|卢布'])
    cost_vat_df = cost_vat_df.with_columns(
        pl.col('成本|卢布').cast(pl.Float64)
    )
    # 在路上和已采购的的只计算货值
    cost_value_df = cost_df.select(['Ozon ID', '采购成本|RMB'])
    # 将成本价与主表连接
    df = df.join(cost_vat_df, how='left', on='Ozon ID').fill_null(0)
    df = df.join(cost_value_df, how='left', on='Ozon ID').fill_null(0)
    df = df.with_columns(
            pl.col('成本|卢布').cast(pl.Float64).round(2).alias('成本|卢布'),
            pl.col('采购成本|RMB').cast(pl.Float64).round(2).alias('采购成本|RMB'),
            )
    # 计算总货值
    df = df.with_columns(
        (sum(pl.col(col).cast(pl.Float64) for col in total_vat_columns)*pl.col('成本|卢布')/rate).round(2).alias('含vat货值(万)'),
        (sum(pl.col(col).cast(pl.Float64) for col in total_value_columns)*pl.col('采购成本|RMB')).round(2).alias('不含vat货值(万)'),
        
    )
    df = df.with_columns(
            (pl.col('含vat货值(万)') + pl.col('不含vat货值(万)')).cast(pl.Int64).alias('总货值(万)'),
            )
    df = df.drop('成本|卢布', '采购成本|RMB','不含vat货值(万)', 
                 '含vat货值(万)') # 删除不需要的列

    return df

def records_to_feishu(app_id, app_secret, table_url, records):
    
    data = insert_records_to_feishu(
        app_id = app_id,
        app_secret = app_secret,
        table_url = table_url,
        records=records
    )
    # print(json.dumps(json.loads(lark.JSON.marshal(data, indent=4)), ensure_ascii=False, indent=4))
    return data
# -*- coding: utf-8 -*-
import configparser
import polars as pl

# 你已有的工具函数/步骤函数（此处假定已实现并可直接导入使用）：
# - step1_genbasic, step2_fboInventory, step3_basic_stock_fill
# - step4_overseas_inventory, step5_basic_stock_fill_seasinventory
# - GenoverseasInventory, fill_null_with_zero
# - basic_stock_fill_seasinventory_toucheng, local_inventory
# - basic_stock_fill_seasinventory_toucheng_local, purchasedInventory
# - basic_stock_fill_seasinventory_toucheng_local_purchase
# - step12_sales_weighted_average, calcu_available_days
# - promotable_quantity, required_purchase_quantity, gentotal_value
# - df_to_feishu_records, records_to_feishu
# - gen_cluster_dict, gen_cluster_safedays
# - run_cluster_available_days_to_feishu（若你已按上一条消息添加）

def calculate_purchased_days(df: pl.DataFrame) -> pl.DataFrame:
    """
    计算“今日采购可售天数” = 今日需采购数量(万)/每日销量 + 已采购可售天数
    会向下取整为整数天。
    """
    return df.with_columns(
        (
            pl.col('今日需采购数量(万)') / pl.col('每日销量') + pl.col('已采购可售天数')
        ).cast(pl.Int64).alias('今日采购可售天数')
    )


def run_stock_purchase_to_feishu(
    cfg_path: str = "config.ini",
    dates: list[str] = None,
    dest_table_url: str | None = None,
    write_intermediate_csv: bool = False,
) -> int:
    """
    计算并将“库存采购表（Ozon库存采购）”写入飞书多维表格。

    参数
    ----
    cfg_path : 配置文件路径（读取 Feishu / Ozon 凭据与默认 URL）
    dates : 参与销量窗口计算的日期列表，例如 ["2025-08-01"]
    dest_table_url : 目标写入表 URL（不传则从 config.ini 的 [feishu].ozon_stock_purchase_table_url 读取）
    write_intermediate_csv : 调试用，是否落盘中间结果

    返回
    ----
    成功写入记录条数
    """
    if dates is None:
        dates = ["2025-08-01"]

    # 1) 读取配置
    cfg = configparser.ConfigParser()
    cfg.read(cfg_path)

    app_id = cfg["feishu"]["app_id"]
    app_secret = cfg["feishu"]["app_secret"]


    safe_days_raw = cfg.get("inventorytable", "safe_days", fallback="0")
    try:
        safe_days = int(safe_days_raw)
    except Exception:
        safe_days = 45

    # 目标表 URL 优先取函数参数，其次取配置
    dest_table_url = dest_table_url or cfg["feishu"].get("ozon_stock_purchase_table_url", "").strip()
    if not dest_table_url:
        raise ValueError("未提供库存采购目标表 URL：请传入 dest_table_url 或在 config.ini 的 [feishu] 中设置 ozon_stock_purchase_table_url")

    # 2) 基础信息
    basicinfo_df = step1_genbasic(dates)

    # 3) FBO库存信息
    fbo_inventory_df = step2_fboInventory()

    # 4) 匹配基础信息与FBO库存
    basic_stock_fill_df = step3_basic_stock_fill(basicinfo_df, fbo_inventory_df)

    # 5) 海外仓库存
    overseas_inventory_df = step4_overseas_inventory()

    # 6) 合并 basic_stock_fill 与 overseas_inventory
    basic_stock_fill_seasinventory_df = step5_basic_stock_fill_seasinventory(
        basic_stock_fill_df, overseas_inventory_df
    )

    # 7) 头程库存（普快/普慢），并填0
    overseas_toucheng_inventory_df = GenoverseasInventory()
    overseas_toucheng_inventory_df = fill_null_with_zero(overseas_toucheng_inventory_df)

    # 8) 合并头程库存
    basic_stock_fill_seasinventory_toucheng_df = basic_stock_fill_seasinventory_toucheng(
        basic_stock_fill_seasinventory_df, overseas_toucheng_inventory_df
    )

    # 9) 本地仓库存
    local_inventory_df = local_inventory()

    # 10) 合并本地仓库存
    basic_stock_fill_seasinventory_toucheng_local_df = basic_stock_fill_seasinventory_toucheng_local(
        basic_stock_fill_seasinventory_toucheng_df, local_inventory_df
    )

    # 11) 采购在途库存
    purchased_inventory_df = purchasedInventory()

    # 12) 合并采购在途
    basic_stock_fill_seasinventory_toucheng_local_purchase_df = basic_stock_fill_seasinventory_toucheng_local_purchase(
        basic_stock_fill_seasinventory_toucheng_local_df, purchased_inventory_df
    )

    # 13) 各窗口销量加权日均
    windows_saels_average_df = step12_sales_weighted_average(target_dates=dates)
    if "货号" in windows_saels_average_df.columns:
        windows_saels_average_df = windows_saels_average_df.drop("货号")

    # 14) 合并销量（按 Ozon ID, 日期）
    merged_df = basic_stock_fill_seasinventory_toucheng_local_purchase_df.join(
        windows_saels_average_df, how='left', on=['Ozon ID', '日期']
    ).fill_null(0)

    # 15) 可用天数 + 供应链安全天数
    available_days_df = calcu_available_days(merged_df, safe_days=safe_days)

    # 16) 可促销数量
    promotable_quantity_df = promotable_quantity(available_days_df)

    # 17) 需采购数量
    required_purchase_quantity_df = required_purchase_quantity(promotable_quantity_df)
    if "record_id" in required_purchase_quantity_df.columns:
        required_purchase_quantity_df = required_purchase_quantity_df.drop('record_id')

    # 18) 计算库存总货值
    total_value_df = gentotal_value(required_purchase_quantity_df)

    # 19) 加上今日采购的可售天数
    final_df = calculate_purchased_days(total_value_df)

    # 调试落盘
    if write_intermediate_csv:
        final_df.write_csv("./Orders/ozon_stock_purchase_final.csv")

    # 20) 转飞书 records 并写入
    feishu_records = df_to_feishu_records(final_df)
    _ = records_to_feishu(app_id, app_secret, dest_table_url, feishu_records)

    print("数据已成功插入飞书 — Ozon库存采购表。总记录数 =", len(feishu_records))
    return len(feishu_records)


def run_cluster_available_days_to_feishu(
    cfg_path: str = "config.ini",
    dates: list[str] = None,
    dest_table_url: str | None = None,
    write_intermediate_csv: bool = False,
) -> int:
    """
    计算各集群可售/安全/补货等指标，并将结果写入飞书多维表格。

    参数
    ----
    cfg_path : str
        配置文件路径（读取 Ozon/Feishu 凭据与数据表 URL）。
    dates : list[str]
        参与销量窗口计算的日期列表，例如 ["2025-08-01"]。
    dest_table_url : str | None
        结果要写入的目标表 URL。若不提供，则尝试从 config.ini 里读取
        `feishu.ozon_cluster_stock_table_url`；若也不存在，将回退使用
        `feishu.cluster_dict_table_url`（与您示例保持一致）。
    write_intermediate_csv : bool
        是否将中间结果落盘到 ./Stocks / ./Orders 目录，便于调试。

    返回
    ----
    int
        成功写入飞书的记录条数（由 df_to_feishu_records 的结果长度决定）。
    """

    if dates is None:
        dates = ["2025-08-01"]

    # ===== 1) 读取配置 =====
    cfg = configparser.ConfigParser()
    cfg.read(cfg_path)

    app_id = cfg["feishu"]["app_id"]
    app_secret = cfg["feishu"]["app_secret"]

    # 源数据相关
    # safe_days = cfg["inventorytable"]["safe_days"]  # 如需全局安全天数可取用
    # client_id = cfg["ozon"]["client_id"]
    # api_key = cfg["ozon"]["api_key"]

    # 读取飞书里“集群字典表URL”“各集群安全天数表URL”
    cluster_dict_table_url_cfg = cfg["feishu"].get("cluster_dict_table_url", "").strip()
    cluster_safe_days_table_url = cfg["feishu"].get("cluster_safe_days_table_url", "").strip()

    # 目标写入表 URL 优先级：函数参数 > config.ini(ozon_cluster_stock_table_url) > config.ini(cluster_dict_table_url)
    dest_table_url = (
        dest_table_url
        or cfg["feishu"].get("ozon_cluster_stock_table_url", "").strip()
        or cluster_dict_table_url_cfg
    )
    if not dest_table_url:
        raise ValueError("未提供目标写入表 URL：请传入 dest_table_url，或在 config.ini 的 [feishu] 中设置 ozon_cluster_stock_table_url / cluster_dict_table_url")

    # ===== 2) 计算流程 =====
    # 2.1 基础信息
    basicinfo = step1_genbasic(dates)

    # 2.2 拉取“集群字典表”，并与基础信息笛卡尔 JOIN（每个 SKU × 集群）
    cluster_dict_df = gen_cluster_dict(cluster_dict_table_url_cfg, app_id, app_secret)
    basicinfo = basicinfo.join(cluster_dict_df, how="cross").fill_null(0)

    if write_intermediate_csv:
        basicinfo.write_csv("./Stocks/basicinfo_cluster.csv")

    # 2.3 FBO 库存信息（按 sku × warehouse 聚合）并匹配
    promised_amount = step2_fboInventory(group_by=["sku", "warehouse_name"])
    if write_intermediate_csv:
        promised_amount.write_csv("./Stocks/promised_amount.csv")

    basic_stock_fill = step3_basic_stock_fill(basicinfo, promised_amount, cluster=True)
    if write_intermediate_csv:
        basic_stock_fill.write_csv("./Stocks/basicinfo_cluster_stock_fill.csv")

    # 2.4 销量窗口 & 加权日均销量
    windows_saels_average = step12_sales_weighted_average(target_dates=dates, cluster=True)
    # 删除“货号”列避免重复
    if "货号" in windows_saels_average.columns:
        windows_saels_average = windows_saels_average.drop("货号")

    # 2.5 合并销量到基础表（按 Ozon ID + 日期 + 集群 对齐）
    basic_stock_sales = basic_stock_fill.join(
        windows_saels_average,
        how="left",
        left_on=["Ozon ID", "日期", "集群"],
        right_on=["Ozon ID", "日期", "配送集群"],
    ).fill_null(0)
    if write_intermediate_csv:
        basic_stock_sales.write_csv("./Stocks/basic_stock_sales.csv")

    # 2.6 各集群安全天数表
    cluster_safe_days_df = gen_cluster_safedays(cluster_safe_days_table_url, app_id, app_secret)

    # 2.7 合并安全天数（按“集群中俄”匹配）
    basic_stock_fill_windows = basic_stock_sales.join(
        cluster_safe_days_df, how="left", on="集群中俄"
    ).fill_null(0)
    if write_intermediate_csv:
        basic_stock_fill_windows.write_csv("./Stocks/basic_stock_fill_windows.csv")

    # 2.8 计算可用天数
    available_days_df = calcu_available_days(
        basic_stock_fill_windows, safe_days=False, cluster=True
    ).fill_nan(0)

    # 2.9 计算“集群需补货数量”“可促销数量”
    available_days_df = available_days_df.with_columns(
        pl.when(pl.col("FBO安全天数") - pl.col("FBO越库可售天数") > 0)
        .then((pl.col("FBO安全天数") - pl.col("FBO越库可售天数")) * pl.col("每日销量"))
        .otherwise(0)
        .alias("集群需补货数量")
    )
    available_days_df = available_days_df.with_columns(
        pl.when(pl.col("FBO越库可售天数") - pl.col("FBO安全天数") > 0)
        .then((pl.col("FBO越库可售天数") - pl.col("FBO安全天数")) * pl.col("每日销量"))
        .otherwise(0)
        .alias("可促销数量")
    )

    # 2.10 清理多余列
    if "record_id" in available_days_df.columns:
        available_days_df = available_days_df.drop("record_id")

    if write_intermediate_csv:
        available_days_df.write_csv("./Orders/available_days_df.csv")

    # ===== 3) 转 records 并写入飞书 =====
    feishu_records = df_to_feishu_records(available_days_df)
    # 注意：records_to_feishu 内部应使用解析出来的 app_token/table_id 调用
    # bitable.v1.app_table_record.batch_create 的 body(records=[{"fields": {...}}])
    #（官方接口文档：Create records / 批量新增记录） [oai_citation:2‡open.feishu.cn](https://open.feishu.cn/document/server-docs/docs/bitable-v1/app-table-record/batch_create?utm_source=chatgpt.com) [oai_citation:3‡open.larksuite.com](https://open.larksuite.com/document/uAjLw4CM/ukTMukTMukTM/reference/bitable-v1/app-table-record/batch_create?utm_source=chatgpt.com)
    _ = records_to_feishu(app_id, app_secret, dest_table_url, feishu_records)

    print("数据已成功写入飞书表格！总记录数 =", len(feishu_records))
    return len(feishu_records)


if __name__ == "__main__":
    # ========= 统一放置两个函数的公共参数 =========
    CFG_PATH = "config.ini"
    DATES = ["2025-08-01"]

    # 从 config.ini 读取默认目标表 URL（也可在此处覆盖）
    _cfg = configparser.ConfigParser()
    _cfg.read(CFG_PATH)

    # 目标表 A：集群可售天数 & 补货数量 写入表（如有该函数）
    CLUSTER_DEST_URL = _cfg["feishu"].get(
        "ozon_cluster_stock_table_url",
        _cfg["feishu"].get("cluster_dict_table_url", "")  # 回退
    ).strip()

    # 目标表 B：库存采购表写入表
    PURCHASE_DEST_URL = _cfg["feishu"].get("ozon_stock_purchase_table_url", "").strip()

    # === 调用 1：集群维度的可售/补货结果入表（如果你已实现前一个函数） ===
    count_cluster = run_cluster_available_days_to_feishu(
        cfg_path=CFG_PATH,
        dates=DATES,
        dest_table_url=CLUSTER_DEST_URL,
        write_intermediate_csv=False,
    )
    print("Cluster 表写入条数:", count_cluster)

    # === 调用 2：库存采购表入表（本次新增/规范化的函数） ===
    count_purchase = run_stock_purchase_to_feishu(
        cfg_path=CFG_PATH,
        dates=DATES,
        dest_table_url=PURCHASE_DEST_URL,
        write_intermediate_csv=False,
    )
    print("Purchase 表写入条数:", count_purchase)
