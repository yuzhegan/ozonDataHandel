"""
mongo_ads_aggregator
--------------------
按给定日期列表，从 MongoDB 的两个集合（opcampaign, mbcampagin）聚合广告数据。
提供可调用函数 `main(...)` 与命令行入口。
"""
from .aggregator import main  # re-export
__all__ = ["main"]
__version__ = "0.1.0"
