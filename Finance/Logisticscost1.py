# encoding='utf-8

# @Time: 2025-08-28
# @File: %
#!/usr/bin/env
# -*- coding: utf-8 -*-
from __future__ import annotations
from pymongo import MongoClient
import polars as pl
import re, math
from datetime import datetime
from typing import Iterable, List, Tuple, Dict, Any, Optional, Set, Union

# =============================================================================
# 工具：默认规则生成（可被传入的自定义规则覆盖）
# =============================================================================
def default_hour_rules() -> Dict[int, Tuple[float, float]]:
    # 平均配送时效(小时) → (基准费系数, 商品价格百分比[百分数形式])
    return {
        29:(1.00,0.00), 30:(1.05,0.25), 31:(1.11,0.55), 32:(1.16,0.80), 33:(1.23,1.15),
        34:(1.28,1.40), 35:(1.32,1.60), 36:(1.36,1.80), 37:(1.40,2.00), 38:(1.44,2.20),
        39:(1.48,2.40), 40:(1.51,2.55), 41:(1.54,2.70), 42:(1.57,2.85), 43:(1.60,3.00),
        44:(1.63,3.15), 45:(1.66,3.30), 46:(1.69,3.45), 47:(1.71,3.55), 48:(1.73,3.65),
        49:(1.75,3.75), 50:(1.76,3.80), 51:(1.77,3.85), 52:(1.774,3.87), 53:(1.78,3.90),
        54:(1.784,3.92), 55:(1.788,3.94), 56:(1.79,3.95), 57:(1.792,3.96), 58:(1.794,3.97),
        59:(1.796,3.98), 60:(1.798,3.99), 61:(1.80,4.00),
    }

def default_light_small_ids() -> Set[str]:
    # 轻小商品 Ozon ID：固定 11 卢布/件
    return {
        "1701596112","1774221575","1774223800","1794079622","1795311329",
        "2272692781","2369643012","2371489844","2382829229","2383634361",
        "2383641082","2383687594","2423621133","2423655359","2423694063"
    }

def default_new_base_fee_brackets() -> List[Tuple[float, float, float]]:
    # 9/1 新规基础物流费分段（体积单位：升），比较规则：low <= volume <= high
    return [
        (0.0, 0.2, 17),(0.201, 0.4, 19),(0.401, 0.6, 21),(0.601, 0.8, 22),(0.801, 1.0, 23),
        (1.001, 1.25, 25),(1.251, 1.5, 26),(1.501, 1.75, 27),(1.751, 2.0, 29),
        (2.001, 3.0, 31),(3.001, 4.0, 35),(4.001, 5.0, 38),(5.001, 6.0, 42),(6.001, 7.0, 57),
        (7.001, 8.0, 61),(8.001, 9.0, 64),(9.001, 10.0, 68),(10.001, 11.0, 78),
        (11.001, 12.0, 82),(12.001, 13.0, 86),(13.001, 14.0, 91),(14.001, 15.0, 95),
        (15.001, 17.0, 100),(17.001, 20.0, 109),(20.001, 25.0, 117),(25.001, 30.0, 129),
        (30.001, 35.0, 144),(35.001, 40.0, 154),(40.001, 45.0, 173),(45.001, 50.0, 186),
        (50.001, 60.0, 204),(60.001, 70.0, 227),(70.001, 80.0, 245),(80.001, 90.0, 270),
        (90.001, 100.0, 280),(100.001, 125.0, 326),(125.001, 150.0, 375),(150.001, 175.0, 429),
        (175.001, 190.0, 476),(190.001, float("inf"), 792),
    ]

# =============================================================================
# 数值/日期工具
# =============================================================================
_num_re = re.compile(r"[0-9]+(?:[.,][0-9]+)?")
def to_float(x) -> float:
    if x is None:
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if not s:
        return 0.0
    s = s.replace(" ", "")
    m = _num_re.search(s)
    if not m:
        return 0.0
    s2 = m.group(0).replace(",", ".")
    try:
        return float(s2)
    except:
        return 0.0

def to_int(x) -> int:
    return int(round(to_float(x)))

_date_re = re.compile(r"(\d{4})[\/\-.](\d{1,2})[\/\-.](\d{1,2})")
def normalize_date_ymd(x) -> Optional[str]:
    if x is None:
        return None
    if isinstance(x, datetime):
        return x.strftime("%Y-%m-%d")
    s = str(x)
    m = _date_re.search(s)
    if not m:
        return None
    y, mth, d = m.groups()
    return f"{y}-{int(mth):02d}-{int(d):02d}"

# =============================================================================
# Mongo 读取（Python 端健壮过滤）
# =============================================================================
def fetch_order_info_for_dates(
    coll, dates_ymd: List[str],
    fields: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """
    读取 order_info，过滤：
      - 状态 != "已取消"
      - 正在处理中 的日期 ∈ dates_ymd
    """
    if fields is None:
        fields = [
            "订单号","发货号码","正在处理中","发运日期","状态","发货的金额",
            "商品名称","Ozon ID","数量"
        ]
    proj = {k: 1 for k in fields}
    cursor = coll.find({}, proj)

    out, dates_set = [], set(dates_ymd)
    for doc in cursor:
        if str(doc.get("状态", "")).strip() == "已取消":
            continue
        proc_day = normalize_date_ymd(doc.get("正在处理中"))
        if proc_day in dates_set:
            out.append(doc)
    return out

def fetch_accruals_by_acceptance_dates(
    coll,
    acceptance_dates_ymd: List[str],
    charge_type: str = "Доставка покупателю",
    acceptance_field: str = "Дата принятия заказа в обработку или оказания услуги",
    fields: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """
    读取 ozon_accruals，过滤：
      - Тип начисления == charge_type
      - <acceptance_field> 的日期（时间格式）∈ acceptance_dates_ymd
    """
    if fields is None:
        fields = [
            acceptance_field, "Тип начисления",
            "Номер отправления или идентификатор услуги",
            "Логистика"
        ]
    proj = {k: 1 for k in fields}
    cursor = coll.find({}, proj)

    dates_set = set(acceptance_dates_ymd)
    out = []
    for doc in cursor:
        if str(doc.get("Тип начисления", "")).strip() != charge_type:
            continue
        acc_day = normalize_date_ymd(doc.get(acceptance_field))
        if acc_day in dates_set:
            out.append(doc)
    return out

# =============================================================================
# 新增：从 accruals 自动计算每日“平均配送时间(小时)” → hours_list
# =============================================================================
def compute_daily_avg_hours_from_accruals(
    coll,
    selected_dates_ymd: Iterable[str],
    charge_type: str = "Доставка покупателю",
    acceptance_field: str = "Дата принятия заказа в обработку или оказания услуги",
    avg_hours_field: str = "Среднее время доставки, часы",
    round_to_int: bool = True,
) -> Tuple[List[str], List[int]]:
    """
    返回 (dates_sorted, hours_list)，其中 hours_list 与 dates_sorted 一一对应。
    仅统计：
      - Тип начисления == charge_type
      - acceptance_field 的日期 == 所选日期
      - avg_hours_field 不为空且 > 0
    """
    # 规范化并去重排序
    dates = []
    for d in selected_dates_ymd:
        nd = normalize_date_ymd(d)
        if not nd:
            raise ValueError(f"非法日期: {d}")
        dates.append(nd)
    dates = sorted(set(dates))
    date_set = set(dates)

    # 只取需要的列
    proj = {acceptance_field: 1, "Тип начисления": 1, avg_hours_field: 1}
    cursor = coll.find({}, proj)

    sums: Dict[str, float] = {d: 0.0 for d in dates}
    cnts: Dict[str, int]   = {d: 0   for d in dates}

    for doc in cursor:
        if str(doc.get("Тип начисления", "")).strip() != charge_type:
            continue
        d = normalize_date_ymd(doc.get(acceptance_field))
        if d not in date_set:
            continue
        h = to_float(doc.get(avg_hours_field))
        if h > 0:
            sums[d] += h
            cnts[d] += 1

    hours_list: List[int] = []
    missing: List[str] = []
    for d in dates:
        if cnts[d] > 0:
            avg_val = sums[d] / cnts[d]
            hours_list.append(int(round(avg_val)) if round_to_int else avg_val)
        else:
            missing.append(d)

    if missing:
        raise ValueError(
            f"这些日期在 ozon_accruals 中没有有效的“{avg_hours_field}”用于计算平均值: {missing}"
        )
    return dates, hours_list

# =============================================================================
# 费用规则函数（参数化）
# =============================================================================
def coeff_and_pct_for_hours(avg_delivery_hours: int,
                            hour_rules: Dict[int, Tuple[float, float]]) -> Tuple[float, float]:
    h = int(round(avg_delivery_hours))
    lo, hi = min(hour_rules), max(hour_rules)
    h = max(min(h, hi), lo)
    return hour_rules[h]

def new_base_fee_by_volume(volume_liters: float,
                           brackets: List[Tuple[float, float, float]]) -> float:
    v = max(float(volume_liters or 0.0), 0.0)
    for low, high, fee in brackets:
        if (v + 1e-9) >= low and (v - 1e-9) <= high:
            return float(fee)
    # 兜底：若未命中任何分段，取最大段的费用
    return float(brackets[-1][2]) if brackets else 0.0

def compute_fallback_logistics_fee(
    shipped_amount: float,         # 发货的金额
    volume_liters: float,          # 升
    avg_delivery_hours: int,       # 平均配送时效（小时，已按行对齐）
    order_date_ymd: str,           # 当前记录的订单日期(YYYY-MM-DD)
    rule_mode: str,                # "auto" | "old" | "new"
    rule_boundary_date_ymd: str,   # 分界：如 "2025-09-01"
    hour_rules: Dict[int, Tuple[float, float]],
    new_base_fee_brackets: List[Tuple[float, float, float]],
    old_base_1l: float = 46.0,
    old_extra_per_l: float = 10.0,
) -> float:
    coeff, pct = coeff_and_pct_for_hours(avg_delivery_hours, hour_rules)  # pct 为“百分数”
    vol = max(float(volume_liters or 1.0), 0.0)

    # 选择规则
    if rule_mode == "new":
        use_new = True
    elif rule_mode == "old":
        use_new = False
    else:  # auto
        use_new = bool(order_date_ymd and order_date_ymd >= rule_boundary_date_ymd)

    # 基础费
    if use_new:
        base = new_base_fee_by_volume(vol, new_base_fee_brackets)
    else:
        # 旧规：≤1L = 46；>1L 每增加 1L +10（向上取整）
        extra_steps = max(0, math.ceil(vol - 1.0))
        base = old_base_1l + old_extra_per_l * extra_steps

    # 费用 = 基础费*系数 + 发货的金额*百分比
    return base * coeff + shipped_amount * (pct / 100.0)

# =============================================================================
# 主流程：生成最终分组的 Polars DataFrame（支持自动从 accruals 取每日时效）
# =============================================================================
def build_grouped_df(
    mongo_uri: str,
    db_name: str,
    order_coll_name: str,
    accruals_coll_name: str,
    selected_dates_ymd: Iterable[str],                 # 多日期：["2025-08-21", ...]
    avg_delivery_hours_by_date: Union[List[int], Dict[str, int], None],  # 可为 None → 自动从 accruals 取
    sku_volume_df: pl.DataFrame,                       # 列: ["sku","volume_rise"]（升）
    rule_mode: str = "auto",                           # "auto" | "old" | "new"
    rule_boundary_date_ymd: str = "2025-09-01",
    # ↓↓↓ 参数化规则（不传用默认）
    hour_rules: Optional[Dict[int, Tuple[float, float]]] = None,
    light_small_ids: Optional[Set[str]] = None,
    new_base_fee_brackets: Optional[List[Tuple[float, float, float]]] = None,
    # 轻小商品费用（可选）
    light_small_fee_per_unit: float = 11.0,
    # 自动计算每日时效时使用的字段名（如你的库字段有变化，可在此覆盖）
    charge_type: str = "Доставка покупателю",
    acceptance_field: str = "Дата принятия заказа в обработку или оказания услуги",
    avg_hours_field: str = "Среднее время доставки, часы",
) -> pl.DataFrame:
    hour_rules = hour_rules or default_hour_rules()
    light_small_ids = light_small_ids or default_light_small_ids()
    new_base_fee_brackets = new_base_fee_brackets or default_new_base_fee_brackets()

    # —— 规范日期列表 —— #
    dates: List[str] = []
    for d in selected_dates_ymd:
        d2 = normalize_date_ymd(d)
        if not d2:
            raise ValueError(f"非法日期: {d}")
        dates.append(d2)
    dates = sorted(set(dates))

    # —— 构建“日期 → 当日时效小时”的映射 —— #
    client = MongoClient(mongo_uri)
    db = client[db_name]
    coll_orders = db[order_coll_name]
    coll_accr = db[accruals_coll_name]

    if avg_delivery_hours_by_date is None:
        # 自动：从 accruals 计算每日平均配送时间
        dates2, hours_list = compute_daily_avg_hours_from_accruals(
            coll=coll_accr,
            selected_dates_ymd=dates,
            charge_type=charge_type,
            acceptance_field=acceptance_field,
            avg_hours_field=avg_hours_field,
            round_to_int=True,
        )
        # dates2 已经是排序好的，等于 dates
        df_hours = pl.DataFrame({"日期": dates2, "当日时效小时": hours_list})
        print(df_hours)
    else:
        # 用户提供（list 或 dict）
        if isinstance(avg_delivery_hours_by_date, dict):
            date_hours_map: Dict[str, int] = {}
            for k, v in avg_delivery_hours_by_date.items():
                nk = normalize_date_ymd(k)
                if not nk:
                    raise ValueError(f"avg_delivery_hours_by_date 中存在非法日期: {k}")
                date_hours_map[nk] = int(v)
            missing = [d for d in dates if d not in date_hours_map]
            if missing:
                raise ValueError(f"缺少这些日期的 avg_delivery_hours: {missing}")
            hours_list = [date_hours_map[d] for d in dates]
        else:
            hours_list = [int(x) for x in avg_delivery_hours_by_date]
            if len(hours_list) != len(dates):
                raise ValueError(
                    f"avg_delivery_hours_by_date 的长度({len(hours_list)})必须等于所选唯一日期数({len(dates)})。"
                    f"\n日期顺序为：{dates}"
                )
        df_hours = pl.DataFrame({"日期": dates, "当日时效小时": hours_list})

    # —— 1) 订单（按日期 + 去掉已取消） —— #
    orders = fetch_order_info_for_dates(coll_orders, dates)
    if not orders:
        return pl.DataFrame(schema={
            "日期": pl.Utf8, "Ozon ID": pl.Utf8,
            "数量": pl.Int64, "发货的金额": pl.Float64, "均价": pl.Float64,
            "物流费": pl.Float64, "平均物流费": pl.Float64,
        })
    df_orders = pl.DataFrame(orders).with_columns([
        pl.col("Ozon ID").cast(pl.Utf8),
    ]).with_columns(
        pl.col("正在处理中").map_elements(normalize_date_ymd, return_dtype=pl.Utf8).alias("日期"),
        pl.col("数量").map_elements(to_int, return_dtype=pl.Int64).alias("数量"),
        pl.col("发货的金额").map_elements(to_float, return_dtype=pl.Float64).alias("发货的金额"),
        pl.col("发货号码").cast(pl.Utf8),
    ).select(["日期","Ozon ID","发货号码","数量","发货的金额"])

    # 把“当日时效小时” join 到订单
    df_orders = df_orders.join(df_hours, on="日期", how="left")
    if df_orders.select(pl.col("当日时效小时").is_null().any()).item():
        raise ValueError("有订单行未匹配到当日时效小时，请检查日期与每日平均配送时间的映射。")

    # —— 2) accruals：按“Дата принятия заказа ...” 的日期 ∈ 所选日期（用于拿 Логистика） —— #
    accr = fetch_accruals_by_acceptance_dates(
        coll_accr,
        acceptance_dates_ymd=dates,
        charge_type=charge_type,
        acceptance_field=acceptance_field,
    )
    # 汇总到单号维度（多条则合计 Логистика）
    df_accr = pl.DataFrame(accr) if accr else pl.DataFrame(schema={
        "Номер отправления или идентификатор услуги": pl.Utf8,
        "Логистика": pl.Float64,
    })
    if df_accr.height > 0:
        df_accr = df_accr.with_columns([
            pl.col("Номер отправления или идентификатор услуги").cast(pl.Utf8),
            pl.col("Логистика").map_elements(to_float, return_dtype=pl.Float64).alias("Логистика"),
        ]).group_by("Номер отправления или идентификатор услуги").agg(
            pl.sum("Логистика").alias("Логистика")
        )

    # —— 3) SKU 体积（升） join —— #
    if "sku" not in sku_volume_df.columns or "volume_rise" not in sku_volume_df.columns:
        raise ValueError("sku_volume_df 需要包含列：['sku','volume_rise']")
    df_sku = sku_volume_df.select([
        pl.col("sku").cast(pl.Utf8).alias("Ozon ID"),
        pl.col("volume_rise").cast(pl.Float64).alias("volume_liters"),
    ])
    df = df_orders.join(df_sku, on="Ozon ID", how="left").with_columns(
        pl.col("volume_liters").fill_null(1.0)
    )

    # —— 4) 关联已发生的 Логистика（优先用它；但轻小商品固定 11/件） —— #
    if df_accr.height > 0:
        df = df.join(
            df_accr,
            left_on="发货号码",
            right_on="Номер отправления или идентификатор услуги",
            how="left"
        )
    else:
        df = df.with_columns(pl.lit(None, dtype=pl.Float64).alias("Логистика"))

    # —— 5) 计算逐行“物流费_row”
    def fallback_fee_struct(s):
        return compute_fallback_logistics_fee(
            shipped_amount=s["发货的金额"],
            volume_liters=s["volume_liters"],
            avg_delivery_hours=int(s["当日时效小时"]),  # ← 按行使用对应日期的平均时效
            order_date_ymd=s["日期"],
            rule_mode=rule_mode,
            rule_boundary_date_ymd=rule_boundary_date_ymd,
            hour_rules=hour_rules,
            new_base_fee_brackets=new_base_fee_brackets,
        )

    df = df.with_columns([
        pl.when(pl.col("Ozon ID").is_in(list(light_small_ids)))
         .then(pl.col("数量") * pl.lit(float(light_small_fee_per_unit)))  # 轻小商品：固定单价 * 数量
         .otherwise(
             pl.when(pl.col("Логистика").is_not_null() & (pl.col("Логистика") > 0))
               .then(pl.col("Логистика"))
               .otherwise(
                   pl.struct(["发货的金额","volume_liters","日期","当日时效小时"]).map_elements(
                       fallback_fee_struct, return_dtype=pl.Float64
                   )
               )
         )
        .alias("物流费_row")
    ])

    # —— 6) 分组聚合并派生 —— #
    g = df.group_by(["日期","Ozon ID"]).agg([
        pl.sum("数量").alias("数量"),
        pl.sum("发货的金额").alias("发货的金额"),
        pl.sum("物流费_row").alias("物流费"),
    ]).with_columns([
        pl.when(pl.col("数量") > 0)
          .then(pl.col("发货的金额") / pl.col("数量"))
          .otherwise(0.0)
          .alias("均价"),
        pl.when(pl.col("数量") > 0)
          .then(pl.col("物流费") / pl.col("数量"))
          .otherwise(0.0)
          .alias("平均物流费"),
    ]).select([
        "日期","Ozon ID","数量","发货的金额","均价","物流费","平均物流费"
    ]).sort(["日期","Ozon ID"])

    return g

# =============================================================================
# 使用示例
# =============================================================================
if __name__ == "__main__":
    # SKU→体积（升）表（示例）
    sku_volume_df = pl.DataFrame({
        "sku": [
            "1390458672","1390525010","1390525016","1390639709","1390770410","1390900843",
            "1391085741","1532738915","1533470896","1533726043","1533824163","1551000756",
            "1551080192","1553058572","1553095803","1553127084","1553731821","1554400248",
            "1554613314","1555128343","1555903521","1556081799","1556148393","1556657592",
            "1558913138","1558931544","1560213310","1560834144","2009007382","2009276052",
            "2010625776","2011182622","2016927477"
        ],
        "volume_rise": [
            0.37,0.54,0.41,0.46,0.31,0.63,1.09,0.65,6.48,1.13,1.35,0.27,0.20,0.45,0.31,1.21,
            0.25,0.33,1.05,0.72,0.36,1.16,0.67,0.83,2.00,2.99,0.57,0.42,0.13,0.09,0.27,0.42,0.06
        ]
    })

    selected_dates = ["2025-08-21"]

    # 方式一：不提供每日时效 → 自动从 ozon_accruals 计算
    df_out = build_grouped_df(
        mongo_uri="mongodb://localhost:27017",
        db_name="ozondatas",
        order_coll_name="order_info",
        accruals_coll_name="ozon_accruals",
        selected_dates_ymd=selected_dates,
        avg_delivery_hours_by_date=None,      # ← 自动计算
        sku_volume_df=sku_volume_df,
        rule_mode="auto",
        rule_boundary_date_ymd="2025-09-01",
        # 可选：light_small_ids / hour_rules / new_base_fee_brackets / light_small_fee_per_unit ...
    )
    print(df_out)

    # 方式二：也可以手动提供每日时效（list or dict）
    # df_out2 = build_grouped_df(
    #     mongo_uri="mongodb://localhost:27017",
    #     db_name="ozondatas",
    #     order_coll_name="order_info",
    #     accruals_coll_name="ozon_accruals",
    #     selected_dates_ymd=selected_dates,
    #     avg_delivery_hours_by_date=[30, 32],  # 或 {"2025/8/21":30, "2025-08-25":32}
    #     sku_volume_df=sku_volume_df,
    # )
