# encoding='utf-8

# @Time: 2025-08-28
# @File: %
#!/usr/bin/env
import polars as pl

def get_hours_list(
    df: pl.DataFrame,
    dates: list[str],
    *,
    tz: str = "Asia/Taipei",
    date_col: str = "日期",
    value_col: str = "平均配送时效",
) -> list[int]:
    # 1) 时间戳(ms) → Datetime(ms) → 设为UTC → 转为台北时间 → 取自然日字符串
    stamped = (
        df.with_columns(
            pl.col(date_col)
            .cast(pl.Int64)
            .cast(pl.Datetime(time_unit="ms"))
            .dt.replace_time_zone("UTC")
            .dt.convert_time_zone(tz)
            .dt.date()
            .cast(pl.Utf8)
            .alias("_date")
        )
        # 同一天多行时取均值（如不需要可改为 .first()）
        .group_by("_date")
        .agg(pl.col(value_col).mean().round(0).cast(pl.Int64).alias(value_col))
    )

    # 2) 保持输入 dates 的顺序
    dates_df = pl.DataFrame({"_date": dates}).with_row_count("ord")

    out = (
        dates_df
        .join(stamped.select("_date", value_col), on="_date", how="left")
        .sort("ord")
    )

    # 3) 缺失校验
    missing = out.filter(pl.col(value_col).is_null()).select("_date").to_series().to_list()
    if missing:
        sample_src = df.select(date_col).head(5).to_series().to_list()
        raise KeyError(
            f"未在源表《{date_col}》中找到这些日期: {missing}；请检查原始日期/时区是否一致。"
            f" 示例前5个原始值: {sample_src}"
        )

    return [int(x) for x in out.select(value_col).to_series().to_list()]
