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
    basicinfo = genbaseinfo(baseinfo_table_url, dates, app_id, app_secret).with_columns(
            pl.col('Ozon ID').cast(pl.Utf8),
            )
    return basicinfo
# 2. 获取FBO的库存信息 
def step2_fboInventory(group_by=['sku']):
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
    overseas_table_url = cfg['feishu']['overseas_table_url']
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
    toucheng_table_url = cfg['feishu']['toucheng_table_url']
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
    local_table_url = cfg['feishu']['local_table_url']
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
    purchased_table_url = cfg['feishu']['purchased_table_url']
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
            mongo_uri=cfg['mongodb']['mongo_uri'],
            db_name=cfg['mongodb']['db_name'],
            coll_name=cfg['mongodb']['coll_name'],
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
    raw_rename = cfg['columns']['rename_dict']   # 现在是字符串
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
    overseas_sales_days = int(cfg['inventorytable']['overseas_safe_days'])  # 海外仓补货安全天数,最近28天最大销量的多少倍
    toucheng_sales_days = int(cfg['inventorytable']['toucheng_safe_days'])  # 头程补货安全天数,每日销量的多少倍
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
    cost_table_url = (cfg['feishu']['cost_table_url']) #成本表的url
    rate = float(cfg['RUB']['rate'])  # 汇率
    import ast
    total_vat_columns = cfg['columns']['total_vat_columns']   # 现在是字符串
    total_vat_columns = ast.literal_eval(total_vat_columns)   # 转成真正的 列表
    total_value_columns = cfg['columns']['total_value_columns']   # 现在是字符串
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
def calculate_purchased_days(df):
    """
    计算加上今日采购可售天数
    :param df: 输入的DataFrame
    :return: 添加了采购在途天数列的DataFrame
    """
    df = df.with_columns(
            (pl.col('今日需采购数量(万)')/ pl.col('每日销量') + pl.col('已采购可售天数')).cast(pl.Int64).alias('今日采购可售天数'),
    )
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
def write_feishu_ozon_stock_purchase():
    """
    将数据插入到飞书表格中
    """
    # 1) 基础信息
    basicinfo_df = step1_genbasic(dates)

    # 2) FBO库存信息
    fbo_inventory_df = step2_fboInventory()

    # 3) 匹配基础信息与FBO库存
    basic_stock_fill_df = step3_basic_stock_fill(basicinfo_df, fbo_inventory_df)

    # 4) 海外仓库存
    overseas_inventory_df = step4_overseas_inventory()

    # 5) 合并 basic_stock_fill 与 overseas_inventory
    basic_stock_fill_seasinventory_df = step5_basic_stock_fill_seasinventory(
        basic_stock_fill_df, overseas_inventory_df
    )

    # 6) 头程库存（普快/普慢），并填0
    overseas_toucheng_inventory_df = GenoverseasInventory()
    overseas_toucheng_inventory_df = fill_null_with_zero(overseas_toucheng_inventory_df)

    # 7) 合并头程库存
    basic_stock_fill_seasinventory_toucheng_df = basic_stock_fill_seasinventory_toucheng(
        basic_stock_fill_seasinventory_df, overseas_toucheng_inventory_df
    )

    # 8) 本地仓库存
    local_inventory_df = local_inventory()

    # 9) 合并本地仓库存
    basic_stock_fill_seasinventory_toucheng_local_df = basic_stock_fill_seasinventory_toucheng_local(
        basic_stock_fill_seasinventory_toucheng_df, local_inventory_df
    )

    # 10) 采购在途库存
    purchased_inventory_df = purchasedInventory()

    # 11) 合并采购在途
    basic_stock_fill_seasinventory_toucheng_local_purchase_df = basic_stock_fill_seasinventory_toucheng_local_purchase(
        basic_stock_fill_seasinventory_toucheng_local_df, purchased_inventory_df
    )

    # 12) 各窗口销量加权日均
    windows_saels_average_df = step12_sales_weighted_average(target_dates=dates)
    windows_saels_average_df = windows_saels_average_df.drop('货号')

    # 13) 合并销量（按 Ozon ID, 日期）
    merged_df = basic_stock_fill_seasinventory_toucheng_local_purchase_df.join(
        windows_saels_average_df, how='left', on=['Ozon ID', '日期']
    ).fill_null(0)

    # 14) 可用天数 + 供应链安全天数
    available_days_df = calcu_available_days(merged_df, safe_days=int(safe_days))

    # 15) 可促销数量
    promotable_quantity_df = promotable_quantity(available_days_df)

    # 16) 需采购数量
    required_purchase_quantity_df = required_purchase_quantity(promotable_quantity_df)
    try:
        required_purchase_quantity_df = required_purchase_quantity_df.drop('record_id')
    except Exception as e:
        print(f"Error dropping 'record_id': {e}")

    # 17) 计算库存总货值
    total_value_df = gentotal_value(required_purchase_quantity_df)

    # 18) 加上今日采购的可售天数
    final_df = calculate_purchased_days(total_value_df)

    # 19) 转飞书 records
    feishu_records = df_to_feishu_records(final_df)
    # 20) 写入飞书
    insert_table = cfg['feishu']['ozon_stock_purchase_table_url']
    records_to_feishu(app_id, app_secret, insert_table, feishu_records)
    print("数据已成功插入飞书—Ozon库存采购表")

def write_cluster_purchasetable():
    # 1. 获取基础信息
    basicinfo = step1_genbasic(dates)
    # 1.1 获取影响平均配送时间的集群信息,集群+中文
    cluster_dict_table_url = cfg['feishu']['cluster_dict_table_url']
    cluster_dict_df = gen_cluster_dict(cluster_dict_table_url, app_id, app_secret)
    # 1.2 讲基础信息的每行sku都要加上17个集群中文
    basicinfo = basicinfo.join(cluster_dict_df, how='cross').fill_null(0)
    # basicinfo.write_csv('./Stocks/basicinfo_cluster.csv')

    # 2. 获取FBO的库存信息 
    promised_amount = step2_fboInventory(group_by=['sku','warehouse_name'])
    # promised_amount.write_csv('./Stocks/promised_amount.csv')
    # 3. 将FBO的库存信息与基础信息进行匹配
    basic_stock_fill = step3_basic_stock_fill(basicinfo, promised_amount, cluster=True)
    # basic_stock_fill.write_csv('./Stocks/basicinfo_cluster_stock_fill.csv')
    # 4.获取各个窗口的销量,计算加权日均销量
    windows_saels_average = step12_sales_weighted_average(target_dates=dates, cluster=True)
    # 删除货号列
    windows_saels_average = windows_saels_average.drop('货号')
    print(windows_saels_average.head(5))
    # 13. 合并basic_stock_fill_seasinventory_toucheng_local_purchase和windows_saels_average,用日期和ozon ID进行连接
    basic_stock_sales = basic_stock_fill.join(
        windows_saels_average, how='left',left_on=['Ozon ID', '日期', '集群'] ,right_on=['Ozon ID', '日期', '配送集群']
    ).fill_null(0)
    # basic_stock_sales.write_csv('./Stocks/basic_stock_sales.csv')
    # 5. 获取各集群的安全天数
    cluster_safe_days_table_url = cfg['feishu']['cluster_safe_days_table_url']
    cluster_safe_days_df = gen_cluster_safedays(cluster_safe_days_table_url, app_id, app_secret)
    # 6. 将集群安全天数与基础信息进行匹配
    basic_stock_fill_windows = basic_stock_sales.join(
        cluster_safe_days_df, how='left', on='集群中俄'
    ).fill_null(0)
    # basic_stock_fill_windows.write_csv('./Stocks/basic_stock_fill_windows.csv')
    # 7. 计算可用天数并添加供应链安全天数列
    available_days_df = calcu_available_days(basic_stock_fill_windows, safe_days=False, cluster=True).fill_nan(0)
    # 8. 计算集群需要补货数量以及ke促销数量
    available_days_df = available_days_df.with_columns(
            pl.when(pl.col('FBO安全天数') - pl.col('FBO越库可售天数') > 0)
            .then((pl.col('FBO安全天数') - pl.col('FBO越库可售天数')) * pl.col('每日销量'))
            .otherwise(0)
            .alias('集群需补货数量'),
    )
    available_days_df = available_days_df.with_columns(
            pl.when(pl.col('FBO越库可售天数') - pl.col('FBO安全天数') > 0)
            .then((pl.col('FBO越库可售天数') - pl.col('FBO安全天数')) * pl.col('每日销量'))
            .otherwise(0)
            .alias('可促销数量')
            )
    try:
        available_days_df = available_days_df.drop('record_id')
    except Exception as e:
        print(f"Error dropping record_id: {e}")
    available_days_df.write_csv('./Orders/available_days_df.csv')
    feishu_record1s = df_to_feishu_records(available_days_df)
    # with open('./Orders/available_days_df.json', 'w', encoding='utf-8') as f:
    #     json.dump(feishu_records, f, ensure_ascii=False, indent=4)
    # 20. 将数据插入到飞书表格中
    # cluster_dict_table_url = cfg['feishu']['ozon_cluster_stock_table_url']
    # cluster_dict_table_url = cfg['feishu']['ozon_cluster_stock_table_url']
    cluster_dict_table_url = 'https://xcn114pn5b7h.feishu.cn/base/N5cFb1e6Za8gShsw6gnc7FWYnod?table=tbl3eqqgEzyjMkd9&view=vew36H8RRn'
    records_to_feishu(app_id, app_secret, cluster_dict_table_url, feishu_record1s)
    print("数据已成功写入飞书表格！\n", len(feishu_record1s))


if __name__ == "__main__":
    dates = ['2025-08-01'] 
    cfg = configparser.ConfigParser()
    cfg.read('config.ini')
    safe_days = cfg['inventorytable']['safe_days']  # 安全天数
    client_id = cfg['ozon']['client_id']
    api_key = cfg['ozon']['api_key']

    baseinfo_table_url = cfg['feishu']['baseinfo_table_url'].strip()


    # 1. 获取配置文件中的app_id和app_secret (https://open.feishu.cn/app/cli_a819ae6445685013/baseinfo)
    app_id=cfg['feishu']['app_id']  # 'cli_a819ae6445685013'
    app_secret=cfg['feishu']['app_secret']  # 'WZVSbtc80PYSJjDr8CHZDgcbILzKgzW0'
    write_feishu_ozon_stock_purchase()
    write_cluster_purchasetable()  # 生成集群采购表




