# encoding='utf-8

# @Time: 2025-08-27
# @File: %
#!/usr/bin/env
from icecream import ic
import os
from icecream import ic
import os
from Feishu.feishu import query_records
from utils.feishu_records_polars import *
import polars as pl
import lark_oapi as lark
from utils.polarsdup2mongo import insert_polars_df_to_mongo
from CreateStockFill import *
import ast, math, re
import json
import polars as pl



def finance_write2feishu(
    cfg_path: str = "config.ini",
    dates: list[str] = None,
    hour_list: list[int] = [31],  # 计算物流费的平均时效列表
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


    # 2) 基础信息
    basicinfo_df = step1_genbasic(dates, cfg).drop(['record_id', '编号']).fill_null('')
    # print(basicinfo_df)
    # 3) 匹配成本
    cost_table_url = _cfg['feishu']['cost_detail_table_url']
    app_id = _cfg['feishu']['app_id']
    app_secret = _cfg['feishu']['app_secret']
    cost_need_columns = _cfg['columns']['cost_need_columns']
    total_vat_columns = ast.literal_eval(cost_need_columns)   # 转成真正的 列表
    print(total_vat_columns)
    cost_df = genfeishu_TableDatas(cost_table_url, app_id, app_secret, '').drop(['record_id', '编号']).fill_null(0)
    cost_df = cost_df.select(total_vat_columns)
    cost_df = cost_df.with_columns(
            (pl.col(r).cast(pl.Float64, strict=False)).alias(r) for r in cost_df.columns if r != 'Ozon ID'
            )
    # 4) 合并基础信息表和成本表
    base_cost_df = basicinfo_df.join(cost_df, on='Ozon ID', how='left').fill_null(0)
    base_cost_df.write_csv('Finance/base_cost_df.csv')
    # 5) 计算均价物流运费, 均价
    LIGHT_SMALL_OZON_IDS = ast.literal_eval(_cfg['shippingcost']['LIGHT_SMALL_OZON_IDS']) # 轻小的ozon id,需要排除计算物流费,函数只计算普通产品的物流费用,轻小固定物流费11
    HOUR_RULES = ast.literal_eval(_cfg['shippingcost']['HOUR_RULES']) # 计算物流费的时间规则
    # NEW_BASE_FEE_BRACKETS = ast.literal_eval(_cfg['shippingcost']['NEW_BASE_FEE_BRACKETS']) # 计算物流费的费用阶梯
    raw = _cfg['shippingcost']['NEW_BASE_FEE_BRACKETS']  # 原字符串
    # 把 float("inf") / float('inf') 替换为合法的字面量（例如 1e300）
    safe = re.sub(r'float\(\s*[\'"]inf[\'"]\s*\)', '1e300', raw, flags=re.I)

    brackets = ast.literal_eval(safe)  # 现在可以安全解析
    # 规范化类型，并把巨大的上限转成 math.inf
    NEW_BASE_FEE_BRACKETS = []
    for low, high, fee in brackets:
        high = math.inf if float(high) >= 1e290 else float(high)
        NEW_BASE_FEE_BRACKETS.append((float(low), high, float(fee)))
    print(NEW_BASE_FEE_BRACKETS)
    light_small_fee_per_unit = float(_cfg['shippingcost']['light_small_fee_per_unit']) # 轻小的物流费
    from Finance.genSizefapi import genSizefapi
    from Finance.Logisticscosts import build_grouped_df
    sku_volume_df = genSizefapi(client_id=cfg['ozon']['client_id'], api_key=cfg['ozon']['api_key'])
    print("hour_list:", hour_list)
    df_out = build_grouped_df(
        mongo_uri=cfg['mongodb']['mongo_uri'],
        db_name=cfg['mongodb']['db_name'],
        order_coll_name=cfg['mongodb']['coll_name'],
        accruals_coll_name=cfg['mongodb']['accrual_coll'],
        selected_dates_ymd=dates,
        avg_delivery_hours_by_date=hour_list,                 # 30 小时 → 系数1.05 + 百分比0.25%
        sku_volume_df=sku_volume_df,
        rule_mode="auto",                      # "auto"|"old"|"new"
        rule_boundary_date_ymd="2025-09-01",   # 9/1 起用新规
        hour_rules=HOUR_RULES,
        light_small_ids=LIGHT_SMALL_OZON_IDS,
        new_base_fee_brackets=NEW_BASE_FEE_BRACKETS,
        light_small_fee_per_unit=light_small_fee_per_unit,         # 如有变更可调整
    )
    # df_out.write_csv('Finance/logistics_costs.csv')
    # print(df_out.schema)
    df_base_cost_logistics = base_cost_df.join(df_out, on=['Ozon ID', '日期'], how='left').fill_null(0)
    # df_base_cost_logistics.write_csv('Finance/df_base_cost_logistics.csv')
    # 6) 聚合广告数据
    from ads.aggregator import main
    op_df, mb_df, merged = main(
        mongo_uri=cfg['mongodb']['mongo_uri'],
        db=cfg['mongodb']['db_name'],
        dates=dates,
        op_coll=cfg['mongodb']['op_coll'],
        mb_coll=cfg['mongodb']['mb_coll'],
        write_csv=False,
        mb_exclude_field=cfg['ads']['mb_exclude_field'],  # "campaignId"
        mb_exclude_values= ast.literal_eval(cfg['ads']['mb_exclude_values']),  # ["8787692"]
        mb_exclude_case_insensitive=True,
        # mb_exclude_field_aliases=["Placement"],
        )
    # print(merged.head())
    print(df_base_cost_logistics.schema)
    print(merged.schema)
    # 7) 合并广告数据
    from utils.polars_before_merged import normalize_for_join
    left2, right2, lkeys, rkeys = normalize_for_join(
    df_base_cost_logistics,
    merged,
    left_on=["Ozon ID", "日期"],
    right_on=[("sku", "sku_right"), ("date", "date_right")],
    )

    final_df =left2.join(right2, left_on=lkeys, right_on=rkeys, how="left").fill_null(0)
    # final_df.write_csv('ads/final_df_before_calc.csv')
    # 8) 计算各类费用
    # 先把参与运算的列转成数值（strict=False 容忍字符串/空值）
    final_df = final_df.with_columns(
        pl.col('均价').cast(pl.Float64, strict=False),
        pl.col('总扣点').cast(pl.Float64, strict=False),
        pl.col('平台总固定费').cast(pl.Float64, strict=False),
        pl.col('平均物流费').cast(pl.Float64, strict=False),
        pl.col('成本|卢布').cast(pl.Float64, strict=False),
        pl.col('数量').cast(pl.Float64, strict=False),
        pl.col('mb_moneySpent').cast(pl.Float64, strict=False),
        pl.col('op_moneySpent').cast(pl.Float64, strict=False),
        pl.col('op_moneySpentFromCPC').cast(pl.Float64, strict=False),
    )

    # 用表达式变量在一次 with_columns 中完成所有新增列（不引用刚创建的列名）
    expr_销售成本 = (pl.col('均价')*pl.col('总扣点') + pl.col('平台总固定费') + pl.col('平均物流费')).round(2)
    expr_毛利   = (pl.col('均价') - pl.col('成本|卢布') - expr_销售成本).round(2)
    expr_模板花费 = pl.when(pl.col('数量') > 0).then((pl.col('mb_moneySpent')/pl.col('数量')).round(2)).otherwise(0.00)
    expr_搜索花费 = pl.when(pl.col('数量') > 0).then(((pl.col('op_moneySpent') + pl.col('op_moneySpentFromCPC'))/pl.col('数量')).round(2)).otherwise(0.00)
    expr_盈亏   = (expr_毛利 - expr_模板花费 - expr_搜索花费).round(2)

    final_df = final_df.with_columns(
        expr_销售成本.alias('销售成本'),
        expr_毛利.alias('毛利'),
        expr_模板花费.alias('模板花费'),
        expr_搜索花费.alias('搜索花费'),
        expr_盈亏.alias('盈亏'),
    )
    # 9) 销量
    '''fanal_df = final_df.with_columns(
            (pl.col('数量').cast(pl.Int64, strict=False)).alias('总销量'),
            ((pl.col('mb_orders') + pl.col('mb_models')).cast(pl.Int64).alias('模板销量')),
            (pl.col('op_orders')).cast(pl.Int64).alias('搜索销量'),
            (pl.col('总销量') - pl.col('模板销量') - pl.col('搜索销量')).cast(pl.Int64).alias('自然销量')
            )'''
    need_cols = ['数量', 'mb_orders', 'mb_models', 'op_orders']
    missing = [c for c in need_cols if c not in final_df.columns]
    if missing:
        raise KeyError(f"缺少必要列: {missing}；当前列: {final_df.columns}")

    tmp = final_df.with_columns([
        pl.col('数量').cast(pl.Int64, strict=False).alias('总销量'),
        (pl.col('mb_orders').fill_null(0).cast(pl.Int64, strict=False) +
         pl.col('mb_models').fill_null(0).cast(pl.Int64, strict=False)).alias('模板销量'),
        pl.col('op_orders').fill_null(0).cast(pl.Int64, strict=False).alias('搜索销量'),
    ])

    fanal_df = tmp.with_columns([
        (pl.col('总销量') - pl.col('模板销量') - pl.col('搜索销量'))
 .cast(pl.Int64).alias('自然销量')
    ])
    # 10) 获取安全库存天数
    # 1.从飞书表格中读取库存数据并与最终表合并
    inventory_table_url = cfg['feishu']['ozon_stock_purchase_table_url']
    from utils.feishu_records_schema_polars import search_records_by_url_to_polars

    inv_df = search_records_by_url_to_polars(
        inventory_table_url, app_id, app_secret,
        date_field_name="日期",
        date_eq=dates,
        date_is_formula=False,
    ).select(['日期', 'Ozon ID','海外仓可售天数', '7日到达可售天数', '每日销量']).fill_null(0)
    # inv_df.write_csv('ads/inv_df.csv')
    from utils.polars_before_merged import normalize_date_col
    left2  = normalize_date_col(fanal_df,  col="日期", out="Date_key")
    right2 = normalize_date_col(inv_df, col="日期", out="Date_key")
    out = left2.join(right2, on=["Date_key", "Ozon ID"], how="left").fill_null(0)
    # 计算安全库存数量
    out = out.with_columns(
            (pl.col('7日到达可售天数') * pl.col('每日销量')).cast(pl.Int64).alias('库存数量'),
            )
    # print(out.head())
    # 11 销售额
    out = out.with_columns(
            pl.col('发货的金额').cast(pl.Float64, strict=False).alias('总销售额'),
            (pl.col('成本|卢布') * pl.col('数量')).cast(pl.Float64, strict=False).round(2).alias('总货物成本'),
            (pl.col('销售成本') * pl.col('数量')).cast(pl.Float64, strict=False).round(2).alias('总销售成本'),
            (pl.col('模板花费') * pl.col('数量')).cast(pl.Float64, strict=False).round(2).alias('总模板花费'),
            (pl.col('搜索花费') * pl.col('数量')).cast(pl.Float64, strict=False).round(2).alias('总搜索花费'),
            (pl.col('盈亏') * pl.col('数量')).cast(pl.Float64, strict=False).round(2).alias('总盈亏')

            )
    out = out.with_columns(
            (pl.col('总销售额') - pl.col('总销售成本') - pl.col('总模板花费') - pl.col('总搜索花费')).round(2).alias('总回款')
            )
    # 12 占比
    out = out.with_columns(
            pl.when(pl.col('总销量') > 0).then((pl.col('自然销量') / pl.col('总销量')*100).round(2)).otherwise(0.00).alias('自然占比'),
            pl.when(pl.col('总销量') > 0).then((pl.col('模板销量') / pl.col('总销量')*100).round(2)).otherwise(0.00).alias('模板占比'),
            pl.when(pl.col('总销量') > 0).then((pl.col('搜索销量') / pl.col('总销量')*100).round(2)).otherwise(0.00).alias('搜索占比'),
            pl.when(pl.col('均价') > 0).then((pl.col('毛利')/pl.col('均价')).round(4)).otherwise(0.00).alias('毛利率'),
            pl.when(pl.col('总销售额') > 0).then(pl.col('总盈亏')/pl.col('总销售额').round(4)).otherwise(0.00).alias('每日盈亏')
            )
    # 13) 选择列的最终顺序
    operation_table_url = cfg['feishu']['operation_table_url']
    final_cols =  ast.literal_eval(cfg['columns']['operation_need_columns'])
    out = out.select(final_cols)
    mongo_uri = cfg['mongodb']['mongo_uri']
    db_name = cfg['mongodb']['db_name']
    operation_coll = cfg['mongodb']['operation_coll']

    res1 = insert_polars_df_to_mongo(
        out, mongo_uri, db_name, operation_coll,
        md5_fields=["日期", "Ozon ID"],
        md5_field_name="dedup_md5",
        batch_size=2000,
    )

    # ===== 3) 转 records 并写入飞书 =====
    dest_table_url = cfg['feishu']['operation_table_url']
    feishu_records = df_to_feishu_records(out)
    # records落盘调试
    with open('ads/finance_feishu_records.json', 'w', encoding='utf-8') as f:
        f.write(json.dumps(feishu_records, ensure_ascii=False, indent=2))

    _ = records_to_feishu(app_id, app_secret, dest_table_url, feishu_records)

    print("数据已成功写入飞书表格！总记录数 =", len(feishu_records))

    # out.write_csv('ads/final_df_before_calc.csv')







if __name__ == '__main__':
    # ========= 统一放置两个函数的公共参数 =========
    CFG_PATH = "config.ini"
    DATES = ["2025-08-20", "2025-08-21"]


    # 从 config.ini 读取默认目标表 URL（也可在此处覆盖）
    _cfg = configparser.ConfigParser()
    _cfg.read(CFG_PATH)
    shipping_hour_table_url = _cfg['feishu']['shipping_hour_table_url']
    app_id = _cfg['feishu']['app_id']
    app_secret = _cfg['feishu']['app_secret']
    shipping_hour = genfeishu_TableDatas(shipping_hour_table_url, app_id, app_secret, '').with_columns(
            pl.col('平均配送时效').cast(pl.Int64, strict=False)
            )
    from utils.genAvgshippinghours import get_hours_list
    HOURLIST = get_hours_list(shipping_hour, DATES)
    print("计算物流费的平均时效列表:", HOURLIST)
    
    # HOURLIST = [30, 32]
    finance_write2feishu(cfg_path=CFG_PATH, dates=DATES, hour_list=HOURLIST)



