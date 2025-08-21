# encoding='utf-8

# @Time: 2025-08-20
# @File: %
#!/usr/bin/env

# utils/warehouse_name2cluster.py

import polars as pl
from typing import List, Dict, Any

# ============ 兼容老版本 Polars 的规范化函数（不再用 .str.strip()） ============
def _norm_expr(col: str) -> pl.Expr:
    return (
        pl.col(col).cast(pl.Utf8)
        # 统一不可见空白（含 NBSP）
        .str.replace_all("\u00A0", " ")
        # 连续空白折叠为单空格
        .str.replace_all(r"\s+", " ")
        # 去掉首尾空白（正则实现，兼容老版本）
        .str.replace_all(r"^\s+", "")
        .str.replace_all(r"\s+$", "")
        # 统一俄语 Ё -> Е（避免看起来相同但码位不同）
        .str.replace_all("Ё", "Е")
        .str.to_uppercase()
    )

def build_wh_to_cluster_map_df_deep(clusters: List[Dict[str, Any]]) -> pl.DataFrame:
    rows = []
    for c in clusters:
        cluster_name = c.get("name")
        for lc in c.get("logistic_clusters") or []:
            for wh in lc.get("warehouses") or []:
                wname = (wh or {}).get("name")
                if wname:
                    rows.append({"warehouse_name": wname, "cluster": cluster_name})

    if not rows:
        return pl.DataFrame(
            {"warehouse_name": pl.Series([], dtype=pl.Utf8),
             "cluster": pl.Series([], dtype=pl.Utf8)}
        )

    wh_map = pl.DataFrame(rows)
    wh_map = (
        wh_map
        .with_columns(wh_key=_norm_expr("warehouse_name"))
        .unique(subset=["wh_key"], keep="first")     # 同一仓库名多归属时保留第一条（可自定义优先级）
        .select(["warehouse_name", "cluster", "wh_key"])
    )
    return wh_map

def add_cluster_name_column_deep(
    fbo_df: pl.DataFrame,
    clusters: List[Dict[str, Any]],
    *,
    unknown_label: str | None = None,
) -> pl.DataFrame:
    wh_map = build_wh_to_cluster_map_df_deep(clusters)

    fbo_df2 = (
        fbo_df
        .with_columns(warehouse_name=pl.col("warehouse_name").cast(pl.Utf8))
        .with_columns(wh_key=_norm_expr("warehouse_name"))
        .with_columns(pl.col("wh_key").cast(pl.Categorical))
    )
    wh_map2 = wh_map.with_columns(pl.col("wh_key").cast(pl.Categorical))

    out = fbo_df2.join(wh_map2.select(["wh_key", "cluster"]), on="wh_key", how="left")

    if unknown_label is not None:
        out = out.with_columns(pl.col("cluster").fill_null(unknown_label))

    return out.drop(["wh_key"])
if __name__ == "__main__":
    # 示例用法
    exit()

    # ===================== 使用示例 =====================
    # fbo_df 示例（你的结构）
    fbo_df = pl.DataFrame({
        "sku": [1390458672, 1390458672],
        "warehouse_name": ["ХОРУГВИНО_РФЦ", "Ростов_на_Дону_РФЦ"],
        "item_code": ["G_QingJieSheng-BaiSe-45", "G_QingJieSheng-BaiSe-45"],
        "item_name": ["Многофункциональный вантуз с ершиком и тросом для чистки труб"] * 2,
        "promised_amount": [0, 0],
        "free_to_sell_amount": [0, 0],
        "reserved_amount": [2, 2],
    })

    # clusters = [...]  # 直接粘你给的超长 JSON
    # 例：未匹配时填 "未匹配集群"
    result = add_cluster_name_column_deep(fbo_df, clusters, unknown_label="未匹配集群")
    print(result)
