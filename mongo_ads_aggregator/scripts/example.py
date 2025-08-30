#!/usr/bin/env python3
from mongo_ads_aggregator import main

if __name__ == "__main__":
    # 示例：仅返回 DataFrame，不写 CSV
    op_df, mb_df, merged = main(
        mongo_uri="mongodb://localhost:27017",
        db="ozondatas",
        dates=["2025-08-20", "2025/8/21"],
        write_csv=False,
        mb_exclude_field="placement",
        mb_exclude_values=["PLACEMENT_SEARCH_AND_CATEGORY", "PLACEMENT_SMART"],
        mb_exclude_case_insensitive=True,
        mb_exclude_field_aliases=["Placement"],
    )
    print(merged.head())
