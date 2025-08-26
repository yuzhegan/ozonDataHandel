# encoding='utf-8

# @Time: 2025-08-22
# @File: %
#!/usr/bin/env
from __future__ import annotations
from icecream import ic
import os
import hashlib
from typing import List, Dict, Any, Optional, Tuple
import polars as pl
from pymongo import MongoClient, InsertOne, ASCENDING
from pymongo.collection import Collection
from pymongo.errors import BulkWriteError


def _ensure_unique_index(coll: Collection, md5_field_name: str) -> None:
    """
    为去重键创建唯一索引（若已存在则忽略）。
    """
    coll.create_index([(md5_field_name, ASCENDING)], unique=True, name=f"{md5_field_name}_uniq")


def _with_md5_column(
    df: pl.DataFrame,
    md5_fields: List[str],
    md5_field_name: str = "dedup_md5",
) -> pl.DataFrame:
    """
    基于指定字段，生成去重用 MD5 列（字符串拼接后 md5），并返回带有该列的新 DataFrame。
    - 缺失字段会报错（以避免误去重）。
    - 将被拼接字段统一转为字符串，并把 None/Null 转为空串。
    """
    missing = [c for c in md5_fields if c not in df.columns]
    if missing:
        raise ValueError(f"DataFrame 缺少用于去重的字段: {missing}")

    exprs = [pl.col(c).cast(pl.Utf8).fill_null("") for c in md5_fields]

    def _md5(s: str) -> str:
        return hashlib.md5(s.encode("utf-8")).hexdigest()

    return (
        df.with_columns(
            pl.concat_str(exprs, separator="|")
            .map_elements(_md5)  # 使用 Python 的 hashlib 计算 MD5
            .alias(md5_field_name)
        )
        # 行内去重，避免同批次内重复提交（保留首条）
        .unique(subset=[md5_field_name], keep="first")
    )


def _bulk_insert_ignore_duplicates(
    coll: Collection,
    records: List[Dict[str, Any]],
    batch_size: int = 1000,
) -> Tuple[int, int]:
    """
    批量 InsertOne，遇唯一键冲突（重复）自动跳过。
    返回: (成功插入条数, 重复/被跳过条数)
    """
    inserted = 0
    duplicates = 0
    buf: List[InsertOne] = []

    def _flush():
        nonlocal inserted, duplicates, buf
        if not buf:
            return
        try:
            res = coll.bulk_write(buf, ordered=False)
            inserted += res.inserted_count or 0
        except BulkWriteError as e:
            # 统计插入成功与重复数量
            details = e.details or {}
            writeErrors = details.get("writeErrors", []) or []
            dup_errors = [w for w in writeErrors if w.get("code") == 11000]
            duplicates += len(dup_errors)
            # 成功插入数 = 总尝试数 - 重复数 - 其他错误数
            # 但 bulk 返回的 nInserted 在异常中可能缺失，这里更稳妥的做法是重新估算：
            tried = len(buf)
            other_errors = len(writeErrors) - len(dup_errors)
            # 在 ordered=False 下，除了错误的，其余 op 仍可能成功。
            # 保守估算：把“非重复”的都视为成功（通常也是如此）。
            inserted += max(0, tried - len(writeErrors))
        finally:
            buf.clear()

    for doc in records:
        buf.append(InsertOne(doc))
        if len(buf) >= batch_size:
            _flush()
    _flush()
    return inserted, duplicates


def _df_to_records(
    df: pl.DataFrame,
    extra_fields: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    将 Polars DataFrame 转为 Python 字典列表，并可追加公共字段。
    """
    records = df.to_dicts()
    if extra_fields:
        for r in records:
            r.update(extra_fields)
    return records


def insert_polars_df_to_mongo(
    df: pl.DataFrame,
    mongo_uri: str,
    db_name: str,
    coll_name: str,
    md5_fields: List[str],
    md5_field_name: str = "dedup_md5",
    extra_fields: Optional[Dict[str, Any]] = None,
    batch_size: int = 1000,
) -> Dict[str, int]:
    """
    将一个 Polars DataFrame 写入 MongoDB，按 md5_fields 计算去重键，重复则不入库。

    返回：
      {
        "inserted": 成功插入条数,
        "duplicates": 重复/被跳过条数,
        "total": 本次待写入（行内去重后）条数
      }
    """
    if df.is_empty():
        return {"inserted": 0, "duplicates": 0, "total": 0}

    # 生成 MD5 去重列并做批内去重
    df_md5 = _with_md5_column(df, md5_fields, md5_field_name)

    # 连接 Mongo & 建索引
    client = MongoClient(mongo_uri)
    coll = client[db_name][coll_name]
    _ensure_unique_index(coll, md5_field_name)

    # 准备记录并写入
    records = _df_to_records(df_md5, extra_fields=extra_fields)
    inserted, duplicates = _bulk_insert_ignore_duplicates(coll, records, batch_size=batch_size)
    return {"inserted": inserted, "duplicates": duplicates, "total": len(records)}


def insert_two_frames(
    cluster_df: pl.DataFrame,
    summary_df: pl.DataFrame,
    *,
    mongo_uri: str,
    db_name: str,
    cluster_coll: str = "cluster_daily",
    summary_coll: str = "sku_summary",
    # 指定各自的去重字段
    cluster_md5_fields: Optional[List[str]] = None,
    summary_md5_fields: Optional[List[str]] = None,
    md5_field_name: str = "dedup_md5",
    batch_size: int = 1000,
) -> Dict[str, Dict[str, int]]:
    """
    将两个 DataFrame 分别入库到两个集合，按各自字段 MD5 去重。
    返回每个集合的写入统计。
    """
    # 默认去重字段（可根据你的口径自行调整）
    if cluster_md5_fields is None:
        # 集群明细：用 日期 + SKU + Ozon ID + 集群 作为一条唯一业务记录
        cluster_md5_fields = ["日期", "SKU", "Ozon ID", "集群"]
    if summary_md5_fields is None:
        # 汇总/总览：用 日期 + SKU + Ozon ID 作为唯一业务记录
        summary_md5_fields = ["日期", "SKU", "Ozon ID"]

    res1 = insert_polars_df_to_mongo(
        cluster_df, mongo_uri, db_name, cluster_coll,
        md5_fields=cluster_md5_fields,
        md5_field_name=md5_field_name,
        batch_size=batch_size,
    )
    res2 = insert_polars_df_to_mongo(
        summary_df, mongo_uri, db_name, summary_coll,
        md5_fields=summary_md5_fields,
        md5_field_name=md5_field_name,
        batch_size=batch_size,
    )
    return {
        cluster_coll: res1,
        summary_coll: res2,
    }
