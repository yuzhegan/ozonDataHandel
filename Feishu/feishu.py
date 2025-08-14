# encoding='utf-8

# @Time: 2025-08-11
# @File: %
#!/usr/bin/env
from icecream import ic
import os

import json
import tomllib
import lark_oapi as lark
from lark_oapi.api.bitable.v1 import *
from dataclasses import dataclass
from typing import Dict, Tuple
from urllib.parse import urlparse, parse_qs
# from tenant_token_cache_script import get_tenant_token
from lark_oapi.api.bitable.v1 import (
    SearchAppTableRecordRequest,
    SearchAppTableRecordRequestBody,
    SearchAppTableRecordResponse,
)
from urllib.parse import urlparse, parse_qs
import argparse


# SDK 使用说明: https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/server-side-sdk/python--sdk/preparations-before-development
# 以下示例代码默认根据文档示例值填充，如果存在代码问题，请在 API 调试台填上相关必要参数后再复制代码使用
@dataclass
class FeishuConfig:
    urls: Dict[str, str]

def load_config(path: str = "config.toml") -> FeishuConfig:
    with open(path, "rb") as f:
        data = tomllib.load(f)
    urls = data.get("feishu", {}).get("urls", {})
    return FeishuConfig(urls=urls)



def query_records(table_url: str, page_size: int = 500, *, app_id: str, app_secret: str):
    """
    使用 app_id/app_secret 调用飞书 Bitable 搜索接口，
    返回记录列表。失败时返回空列表。
    """
    app_token, table_id = extract_bitable_ids(table_url)
    ic(app_token, table_id)
    if not app_token or not table_id:
        lark.logger.error(f"URL 中缺少 app_token 或 table_id: {table_url}")
        return []

    # 创建 client，SDK 会自动管理 tenant_access_token
    client = (
        lark.Client.builder()
        .app_id(app_id)
        .app_secret(app_secret)
        .log_level(lark.LogLevel.DEBUG)
        .build()
    )

    # 构建请求
    request = (
        SearchAppTableRecordRequest.builder()
        .app_token(app_token)
        .table_id(table_id)
        .user_id_type("user_id")
        .page_size(page_size)
        .request_body(SearchAppTableRecordRequestBody.builder().build())
        .build()
    )

    # 调用接口（无需单独传 token）
    response = client.bitable.v1.app_table_record.search(request)

    # 处理返回
    if not response.success():
        raw = json.loads(response.raw.content) if response.raw.content else {}
        lark.logger.error(
            f"client.bitable.v1.app_table_record.search failed, code: {response.code}, "
            f"msg: {response.msg}, log_id: {response.get_log_id()}, resp: \n{json.dumps(raw, indent=4, ensure_ascii=False)}"
        )
        return []

    # 返回记录列表
    return json.loads(lark.JSON.marshal(response.data, indent=4)).get("items", [])


from urllib.parse import urlparse, parse_qs


def extract_bitable_ids(url: str):
    """
    返回 (app_token, table_id)

    支持示例：
    - https://.../base/<app_token>?table=<table_id>&view=<view_id>
    - https://.../base/<app_token>#table=<table_id>&view=<view_id>
    - https://.../base/<app_token>/table/<table_id>
    """
    u = urlparse(url)
    app_token = None
    table_id = None

    # 1) 从 path 提取 app_token
    parts = [p for p in u.path.split('/') if p]
    if 'base' in parts:
        i = parts.index('base')
        if i + 1 < len(parts):
            app_token = parts[i + 1]

    # 2) 从 query 提取 table_id
    q = parse_qs(u.query)
    table_id = (q.get('table') or q.get('table_id') or [None])[0]

    # 3) 从 fragment(#) 提取 table_id（不少链接把参数放 # 后面）
    if not table_id and u.fragment:
        fq = parse_qs(u.fragment)
        table_id = (fq.get('table') or fq.get('table_id') or [None])[0]

    # 4) 兜底：从 path 里的 /table/<id> 提取
    if not table_id:
        for i, p in enumerate(parts):
            if p in ('table', 'tables') and i + 1 < len(parts):
                table_id = parts[i + 1]
                break

    return app_token, table_id


# 示例
# url = "https://xcn114pn5b7h.feishu.cn/base/N5cFb1e6Za8gShsw6gnc7FWYnod?table=tbl1zHI1iokbDfMe&view=vew45J0xQ0"
# print(extract_bitable_ids(url))
# 输出: ('N5cFb1e6Za8gShsw6gnc7FWYnod', 'tbl1zHI1iokbDfMe')

if __name__ == "__main__":
    exit()
    # APP_ID = 'cli_a819ae6445685013'
    # APP_SECRET = 'WZVSbtc80PYSJjDr8CHZDgcbILzKgzW0'
    # DOMAIN = os.getenv("FEISHU_DOMAIN", "https://open.feishu.cn")
    # if not APP_ID or not APP_SECRET:
    #     raise SystemExit(
    #         "Please set FEISHU_APP_ID and FEISHU_APP_SECRET environment variables."
    #     )
    # token_info = get_tenant_token(APP_ID, APP_SECRET, DOMAIN)
    # access_token = token_info["tenant_access_token"]
    records = query_records(
            table_url="https://xcn114pn5b7h.feishu.cn/base/N5cFb1e6Za8gShsw6gnc7FWYnod?table=tbl2O9SNHUeHmPfE&view=vew4IHfvZH",
            page_size=5000,
            app_id="cli_a819ae6445685013",
            app_secret="WZVSbtc80PYSJjDr8CHZDgcbILzKgzW0",
        )
    print(records)
    print("Total records:", len(records))
    exit()
    cfg = load_config()
    ic(cfg.urls)
    for name, url in cfg.urls.items():
        app_token, table_id = extract_bitable_ids(url)
        print(name, app_token, table_id)
        

