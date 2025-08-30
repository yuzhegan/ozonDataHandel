# encoding='utf-8

# @Time: 2025-08-27
# @File: %
#!/usr/bin/env
from icecream import ic
import os
from pymongo import MongoClient
import polars as pl

def daily_grouped_df_by_processing_date(
    date_str: str,                      # 例如 "2025-08-21"
    mongo_uri: str = "mongodb://localhost:27017",
    db_name: str = "ozondatas",
    coll_name: str = "order_info",
):
    client = MongoClient(mongo_uri)
    coll = client[db_name][coll_name]

    pipeline = [
        # 1) 标注类型
        {"$addFields": {"_proc_type": {"$type": "$正在处理中"}}},

        # 2) 规范化“正在处理中”为 YYYY-MM-DD
        {
            "$addFields": {
                "_processing_date_only": {
                    "$cond": [
                        {"$eq": ["$_proc_type", "date"]},
                        {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": "$正在处理中",
                                "onNull": None
                            }
                        },
                        {
                            "$let": {
                                "vars": {
                                    "rf": {
                                        "$regexFind": {
                                            "input": { "$ifNull": ["$正在处理中", ""] },
                                            "regex": "(\\d{4})[\\/\\-.](\\d{1,2})[\\/\\-.](\\d{1,2})"
                                        }
                                    }
                                },
                                "in": {
                                    # 先单独拿到 captures -> caps
                                    "$let": {
                                        "vars": {
                                            "caps": { "$ifNull": ["$$rf.captures", []] }
                                        },
                                        "in": {
                                            # 再用 caps 计算 y/m/d
                                            "$let": {
                                                "vars": {
                                                    "y": { "$ifNull": [ { "$arrayElemAt": ["$$caps", 0] }, "" ] },
                                                    "m": { "$ifNull": [ { "$arrayElemAt": ["$$caps", 1] }, "" ] },
                                                    "d": { "$ifNull": [ { "$arrayElemAt": ["$$caps", 2] }, "" ] }
                                                },
                                                "in": {
                                                    # 如果任一为空则置 None
                                                    "$cond": [
                                                        { "$or": [
                                                            { "$eq": ["$$y", ""] },
                                                            { "$eq": ["$$m", ""] },
                                                            { "$eq": ["$$d", ""] }
                                                        ]},
                                                        None,
                                                        {
                                                            # 补零并拼接为 YYYY-MM-DD
                                                            "$let": {
                                                                "vars": {
                                                                    "mm2": {
                                                                        "$cond": [
                                                                            { "$lte": [ { "$strLenCP": "$$m" }, 1 ] },
                                                                            { "$concat": ["0", "$$m"] },
                                                                            "$$m"
                                                                        ]
                                                                    },
                                                                    "dd2": {
                                                                        "$cond": [
                                                                            { "$lte": [ { "$strLenCP": "$$d" }, 1 ] },
                                                                            { "$concat": ["0", "$$d"] },
                                                                            "$$d"
                                                                        ]
                                                                    }
                                                                },
                                                                "in": { "$concat": ["$$y", "-", "$$mm2", "-", "$$dd2"] }
                                                            }
                                                        }
                                                    ]
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    ]
                }
            }
        },

        # 3) 数值转换（数量/您的价格），兼容空值与字符串
        {
            "$addFields": {
                "_qty_num": {
                    "$cond": [
                        { "$in": [ { "$type": "$数量" }, ["int","long","double","decimal"] ] },
                        { "$toInt": "$数量" },
                        {
                            "$cond": [
                                { "$gt": [ { "$strLenCP": { "$ifNull": ["$数量", ""] } }, 0 ] },
                                {
                                    "$toInt": {
                                        "$replaceAll": {
                                            "input": { "$trim": { "input": { "$ifNull": ["$数量", "0"] } } },
                                            "find": ",",
                                            "replacement": ""
                                        }
                                    }
                                },
                                0
                            ]
                        }
                    ]
                },
                "_price_num": {
                    "$cond": [
                        { "$in": [ { "$type": "$您的价格" }, ["int","long","double","decimal"] ] },
                        { "$toDouble": "$您的价格" },
                        {
                            "$cond": [
                                { "$gt": [ { "$strLenCP": { "$ifNull": ["$您的价格", ""] } }, 0 ] },
                                {
                                    "$toDouble": {
                                        "$replaceAll": {
                                            "input": { "$trim": { "input": { "$ifNull": ["$您的价格", "0"] } } },
                                            "find": ",",
                                            "replacement": "."
                                        }
                                    }
                                },
                                0.0
                            ]
                        }
                    ]
                }
            }
        },

        # 4) 过滤指定日期
        { "$match": { "_processing_date_only": date_str } },

        # 5) group by（日期 + Ozon ID）
        {
            "$group": {
                "_id": {
                    "日期": "$_processing_date_only",
                    "OzonID": "$Ozon ID"
                },
                "数量合计": { "$sum": "$_qty_num" },
                "您的价格合计": { "$sum": "$_price_num" },
                "您的价格均值": { "$avg": "$_price_num" }
            }
        },

        # 6) 扁平化 & 排序
        {
            "$project": {
                "_id": 0,
                "日期": "$_id.日期",
                "Ozon ID": "$_id.OzonID",
                "数量合计": 1,
                "您的价格合计": 1,
                "您的价格均值": 1
            }
        },
        { "$sort": { "日期": 1, "Ozon ID": 1 } },
    ]

    docs = list(coll.aggregate(pipeline, allowDiskUse=True))
    # 防守：空结果时也返回一个有列名的空 DF
    if not docs:
        return pl.DataFrame(schema={
            "日期": pl.Utf8,
            "Ozon ID": pl.Utf8,
            "数量合计": pl.Int64,
            "您的价格合计": pl.Float64,
            "您的价格均值": pl.Float64,
        })
    return pl.DataFrame(docs)


# 示例调用
if __name__ == "__main__":
    df = daily_grouped_df_by_processing_date(
        "2025-08-21",
        mongo_uri="mongodb://localhost:27017",
        db_name="ozondatas",
        coll_name="order_info",
    )
    df.write_csv("daily_grouped_2025-08-21.csv")
