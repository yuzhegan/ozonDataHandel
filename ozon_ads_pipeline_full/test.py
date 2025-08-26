# encoding='utf-8

# @Time: 2025-08-26
# @File: %
#!/usr/bin/env
from icecream import ic
import os
import pandas as pd
#readcsv with pandas
df = pd.read_csv('/Users/mac/Documents/ozns/github/ozonDataHandel/ozon_ads_pipeline_full/mbcampagin.csv')

ic(df.head(5))
