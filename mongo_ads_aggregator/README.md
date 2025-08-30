# mongo_ads_aggregator

按给定日期列表，从 MongoDB 的两个集合（`opcampaign`, `mbcampagin`）聚合广告数据。

- 使用 **PyMongo + Polars**
- 支持将诸如 `"5 782,00"`、`"5782,00"`、`"1,234.56"` 等字符串稳健地转换为浮点数
- 支持 **mbcampagin** 在抓取时按任意字段做“**排除值列表**”过滤（可选大小写不敏感，支持数组字段与字段别名）
- 既可当作 **库函数** 调用，也可通过 **命令行** 使用
- 输出三份 CSV：`ads_opcampaign.csv`, `ads_mbcampagin.csv`, `ads_merged.csv`

## 安装依赖

```bash
pip install -r requirements.txt
```

## 命令行用法

```bash
python -m mongo_ads_aggregator   --mongo-uri "mongodb://host.docker.internal:27017"   --db ozondatas   --dates 2025-08-20 2025/8/21   --mb-exclude-field placement   --mb-exclude-values PLACEMENT_SEARCH_AND_CATEGORY PLACEMENT_SMART   --mb-exclude-ci   --mb-exclude-aliases Placement
```

运行后在当前目录生成：

- `ads_opcampaign.csv`
- `ads_mbcampagin.csv`
- `ads_merged.csv`

## 作为库函数调用

```python
from mongo_ads_aggregator import main

op_df, mb_df, merged = main(
    mongo_uri="mongodb://host.docker.internal:27017",
    db="ozondatas",
    dates=["2025-08-20", "2025/8/21"],
    op_coll="opcampaign",
    mb_coll="mbcampagin",
    write_csv=False,  # 只拿 DataFrame，不落盘
    # 过滤 mbcampagin 中 placement 的两种投放类型
    mb_exclude_field="placement",
    mb_exclude_values=["PLACEMENT_SEARCH_AND_CATEGORY", "PLACEMENT_SMART"],
    mb_exclude_case_insensitive=True,
    mb_exclude_field_aliases=["Placement"],
)
print(merged.head())
```

## 建议索引

为提速，可在两个集合加索引：

```js
db.opcampaign.createIndex({ date: 1, sku: 1 });
db.mbcampagin.createIndex({ date: 1, sku: 1 });
```
