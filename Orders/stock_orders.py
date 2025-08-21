# encoding='utf-8

# @Time: 2025-08-11
# @File: %
#!/usr/bin/env
from dataclasses import dataclass, field
from typing import Sequence, List, Optional

import polars as pl
from icecream import ic

from Orders.calculate_daily_weight_polars import calculate_weighted_daily_sales, calculate_dynamic_daily_sales
from Orders.summarize_order_info_windows_polars import summarize_order_windows
from Orders.summarize_peak_daily_sales_polars import summarize_peak_daily_sales


@dataclass
class OrderSummaryConfig:
    mongo_uri: str = "mongodb://localhost:27017"
    db_name: str = "ozondatas"
    coll_name: str = "order_info"

    # 业务参数
    date_field: str = "正在处理中"
    agg_field: str = "数量"
    windows: Sequence[int] = field(default_factory=lambda: (7, 14, 28, 60, 90))
    timezone: str = "Asia/Seoul"
    date_format: str = "%Y-%m-%d %H:%M:%S"
    target_date: Optional[str] = None  # e.g. "2025-08-07"

    # 动态日销参数
    top_k: int = 3

    # 峰值统计参数
    peak_days_back: int = 28

    # 在计算“动态日销/峰值日销”时，从原始分组字段中要**剔除**的维度（默认剔除“货号”）
    drop_keys_for_daily: Sequence[str] = field(default_factory=lambda: ("货号",))


class OrderSummaryGenerator:
    """
    把公共参数与流程封装，提供通用与快捷方法：
      - run(group_fields=...)
      - generate_by_sku_and_ozon_id()
      - generate_by_sku_ozon_id_and_cluster()
    """
    def __init__(self, cfg: OrderSummaryConfig):
        self.cfg = cfg

    def _build_daily_group_fields(self, group_fields: Sequence[str]) -> List[str]:
        """在计算日销/峰值时，按规则从 group_fields 中剔除某些维度（默认剔除 '货号'）。"""
        drop_set = set(self.cfg.drop_keys_for_daily)
        return [g for g in group_fields if g not in drop_set]

    def run(self, group_fields: Sequence[str]) -> pl.DataFrame:
        """
        通用的汇总流程：
        1) summarize_order_windows：按 group_fields 统计各窗口销量
        2) calculate_dynamic_daily_sales：按(去除指定key后的)维度计算动态日销量
        3) summarize_peak_daily_sales：按(去除指定key后的)维度统计近N天峰值
        4) 依次左连接合并
        """
        # 1) 窗口汇总
        df_windows = summarize_order_windows(
            target_date=self.cfg.target_date,
            mongo_uri=self.cfg.mongo_uri,
            db_name=self.cfg.db_name,
            coll_name=self.cfg.coll_name,
            date_field=self.cfg.date_field,
            group_fields=list(group_fields),
            agg_field=self.cfg.agg_field,
            windows=list(self.cfg.windows),
            date_format=self.cfg.date_format,
            timezone=self.cfg.timezone,
        )
        # print("df_windows", df_windows.head())

        # 2) 动态日销（按剔除“货号”后的键做聚合，与你当前脚本一致）
        daily_keys = self._build_daily_group_fields(group_fields)
        # print("daily_keys=======>", daily_keys)
        df_daily = calculate_dynamic_daily_sales(
            df_windows,
            windows=list(self.cfg.windows),
            group_fields=daily_keys,
            top_k=self.cfg.top_k,
        )
        # print("df_daily", df_daily.head())

        # 将动态日销并回窗口汇总；注意只按 daily_keys 连接（你的原脚本同样这样做）
        # print("daily_keys", daily_keys)
        df_joined = df_windows.join(df_daily, how="left", on=daily_keys).fill_nan(0)

        # 3) 峰值日销（近 N 天）
        df_peak = summarize_peak_daily_sales(
            mongo_uri=self.cfg.mongo_uri,
            db_name=self.cfg.db_name,
            coll_name=self.cfg.coll_name,
            date_field=self.cfg.date_field,
            group_fields=daily_keys,
            agg_field=self.cfg.agg_field,
            days_back=self.cfg.peak_days_back,
            target_date=self.cfg.target_date,
            timezone=self.cfg.timezone,
        )
        # print("df_peak", df_peak.head())

        # 4) 合并峰值
        if '配送集群' in group_fields:
            df_final = df_joined
            # 如果 group_fields 包含 '配送集群'，不合并28天内销量峰值
            pass
        else:
            df_final = df_joined.join(df_peak, how="left", on=daily_keys).fill_nan(0)
        return df_final

    # 便捷方法 1：等价你原来的 generate_order_summary()
    def generate_by_sku_and_ozon_id(self) -> pl.DataFrame:
        group_fields = ["货号", "Ozon ID"]
        return self.run(group_fields)

    # 便捷方法 2：等价你原来的 generate_order_summary_by_sku_and_delivery_cluster()
    def generate_by_sku_ozon_id_and_cluster(self) -> pl.DataFrame:
        group_fields = ["货号", "Ozon ID", "配送集群"]
        return self.run(group_fields)


# ---------------- 使用示例 ----------------
if __name__ == "__main__":
    exit()
    cfg = OrderSummaryConfig(
        mongo_uri="mongodb://localhost:27017",
        db_name="ozondatas",
        coll_name="order_info",
        date_field="正在处理中",
        agg_field="数量",
        windows=(7, 14, 28, 60, 90),
        timezone="Asia/Seoul",
        date_format="%Y-%m-%d %H:%M:%S",
        target_date="2025-08-07",
        top_k=3,
        peak_days_back=28,
        # 如果将来有别的维度也想在日销/峰值阶段去掉，可在这里加上
        drop_keys_for_daily=("货号"),
    )

    gen = OrderSummaryGenerator(cfg)

    # 场景1：按“货号 + Ozon ID”
    df1 = gen.generate_by_sku_and_ozon_id()
    print("final df1", df1.head())
    df1.write_csv("output_by_sku_and_ozon_id.csv")

    # 场景2：按“货号 + Ozon ID + 配送集群”
    df2 = gen.generate_by_sku_ozon_id_and_cluster()
    df2.write_csv("output_by_sku_ozon_id_and_cluster.csv")
    print("final df2", df2.head())
