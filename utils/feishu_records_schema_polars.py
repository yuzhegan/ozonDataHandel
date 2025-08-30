# encoding='utf-8

# @Time: 2025-08-29
# @File: %
#!/usr/bin/env
# -*- coding: utf-8 -*-
# Feishu x Polars helpers (URL 查询 + 扁平化 + 回写)
# --------------------------------------------------
# 依赖:
#   pip install lark-oapi polars
#
# 主要功能:
#   - build_client: 创建飞书客户端
#   - parse_app_and_table_from_url: 从多维表格 URL 解析 app_token/table_id
#   - fetch_field_schema: 拉取字段 schema（用于类型与字段ID->名称映射）
#   - search_records: 传统按 token/table_id 翻页查询
#   - search_records_by_url: 🔥 新增，按 URL + 条件（含日期 ExactDate）翻页查询
#   - search_records_by_url_to_polars: 🔥 新增，直接返回 Polars DataFrame
#   - records_to_polars_by_schema: 将 records 扁平化为 DataFrame
#   - polars_to_records_by_schema: DataFrame 还原回 records（按 schema）
#   - df_to_feishu_records: 将 DataFrame 粗转为 records（宽松版）
#   - insert_records_to_feishu: 批量写入
#
# 注意:
#   - 日期筛选遵循官方“记录筛选指南”：DateTime 字段用 ["ExactDate","<毫秒字符串>"]
#   - 公式日期字段用 ["ExactDate","yyyy/MM/dd"]
#   - value 一律是「列表」；毫秒需是「字符串」

# -*- coding: utf-8 -*-
"""
Feishu Bitable ↔ Polars Utilities (URL search with ExactDate & list support)

功能概要
- build_client: 创建飞书 lark-oapi 客户端
- parse_app_and_table_from_url: 从 URL 解析 app_token/table_id/view_id
- fetch_field_schema: 获取字段 schema（含字段ID→名称映射）
- records_to_polars_by_schema / polars_to_records_by_schema
- df_to_feishu_records: 宽松的 DataFrame -> records
- insert_records_to_feishu: 批量写入
- search_records: 传统按 token/table_id 搜索
- search_records_by_url: ✅ 按 URL 搜索，支持 date_eq 为单值或列表，严格按官方请求结构构造
- search_records_by_url_to_polars: 便捷封装，直接返回 Polars DataFrame
"""
from __future__ import annotations

import ast
import json
import re
import time
import datetime
from typing import Any, Dict, List, Sequence, Optional, Iterable, Literal, Tuple

import polars as pl
import lark_oapi as lark
from zoneinfo import ZoneInfo
from urllib.parse import urlparse, parse_qs

from lark_oapi.api.bitable.v1 import (
    ListAppTableFieldRequest,
    SearchAppTableRecordRequest,
    SearchAppTableRecordRequestBody,
    BatchCreateAppTableRecordRequest,
    BatchCreateAppTableRecordRequestBody,
    BatchCreateAppTableRecordResponse,
    FilterInfo,
    Condition,
)

# ======================= 1) 客户端 & URL 解析 =======================

def build_client(app_id: str, app_secret: str) -> lark.Client:
    assert isinstance(app_id, str) and isinstance(app_secret, str)
    return (
        lark.Client.builder()
        .app_id(app_id)
        .app_secret(app_secret)
        .log_level(lark.LogLevel.INFO)
        .build()
    )

def parse_app_and_table_from_url(table_url: str) -> Tuple[str, str, Optional[str]]:
    """从多维表格 URL 解析 (app_token, table_id, view_id)"""
    if isinstance(table_url, (tuple, list)):
        table_url = table_url[0]
    m = re.search(r"/base/([A-Za-z0-9]+)", table_url)
    if not m:
        raise ValueError("无法从 URL 中解析 app_token")
    app_token = m.group(1)

    q = parse_qs(urlparse(table_url).query)
    table_id = q.get("table", [None])[0]
    if not table_id:
        raise ValueError("无法从 URL 中解析 table_id")
    view_id = q.get("view", [None])[0]
    return app_token, table_id, view_id

# ======================= 2) 字段 schema =======================

TYPE_CODE_TO_NAME = {
    1: "text",
    2: "number",
    3: "single_select",
    4: "multi_select",
    5: "date",       # Date/DateTime 统一叫 date
    7: "checkbox",
}

def normalize_type_name(type_value: Any) -> str:
    if isinstance(type_value, int):
        return TYPE_CODE_TO_NAME.get(type_value, "unknown")
    if isinstance(type_value, str):
        return type_value.strip().lower()
    return "unknown"

def fetch_field_schema(
    client: lark.Client,
    app_token: str,
    table_id: str,
    page_size: int = 100,
    max_pages: int = 20,
) -> Dict[str, Dict[str, Any]]:
    """
    返回: { field_name: {"field_id": "...", "type": "<normalized_type>", "raw_type": <原始>, "property": {...}} }
    """
    schema: Dict[str, Dict[str, Any]] = {}
    page_token = ""
    seen = set()
    page_no = 0

    while True:
        page_no += 1
        builder = (
            ListAppTableFieldRequest.builder()
            .app_token(app_token)
            .table_id(table_id)
            .page_size(page_size)
        )
        if page_token:
            builder = builder.page_token(page_token)

        resp = client.bitable.v1.app_table_field.list(builder.build())
        if not resp.success():
            if resp.code == 1254030 and page_token:
                print(f"[warn] InvalidPageToken {page_token!r}, restart")
                page_token = ""
                continue
            raise RuntimeError(f"拉取字段失败: code={resp.code}, msg={resp.msg}")

        data = resp.data
        items = getattr(data, "items", None) or []
        for it in items:
            field_name = getattr(it, "field_name", None) or getattr(it, "name", None)
            field_id   = getattr(it, "field_id", None)   or getattr(it, "id", None)
            raw_type   = getattr(it, "type", None)
            prop       = getattr(it, "property", None)
            tname = normalize_type_name(raw_type)
            if field_name:
                schema[field_name] = {
                    "field_id": field_id,
                    "type": tname,
                    "raw_type": raw_type,
                    "property": prop,
                }

        next_token = getattr(data, "page_token", "") or ""
        if not next_token:
            break
        if next_token in seen:
            print(f"[warn] repeated page_token {next_token!r}, stop")
            break
        seen.add(next_token)
        page_token = next_token
        if page_no >= max_pages:
            print(f"[warn] reached max_pages={max_pages}, stop")
            break
    return schema

# ======================= 3) records -> Polars =======================

def records_to_polars_by_schema(
    records: Sequence[Dict[str, Any]],
    field_schema: Dict[str, Dict[str, Any]] | None,
    keep_record_id: bool = True,
    list_text_strategy: Literal["first", "join"] = "first",
    join_sep: str = ";",
) -> pl.DataFrame:
    def _first_text_from_list(lst: list[Any]) -> Optional[str]:
        for x in lst:
            if isinstance(x, dict) and x.get("text") is not None:
                return str(x["text"])
            if isinstance(x, (str, int, float, bool)):
                return str(x)
        return None

    def _all_texts_from_list(lst: list[Any]) -> List[str]:
        out: List[str] = []
        for x in lst:
            if isinstance(x, dict) and x.get("text") is not None:
                out.append(str(x["text"]))
            elif isinstance(x, (str, int, float, bool)):
                out.append(str(x))
        return out

    def _extract_textish(value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, (str, int, float, bool)):
            return str(value)
        if isinstance(value, list):
            if list_text_strategy == "first":
                return _first_text_from_list(value)
            all_txt = _all_texts_from_list(value)
            return join_sep.join(all_txt) if all_txt else None
        if isinstance(value, dict):
            if value.get("text") is not None:
                return str(value["text"])
            if isinstance(value.get("value"), list):
                if list_text_strategy == "first":
                    return _first_text_from_list(value["value"])
                all_txt = _all_texts_from_list(value["value"])
                return join_sep.join(all_txt) if all_txt else None
            for k in ("name", "title", "label"):
                if value.get(k) is not None:
                    return str(value[k])
            return str(value)
        return str(value)

    def _flatten_value_by_type(val: Any, tname: str) -> Any:
        if val is None:
            return None
        tname = (tname or "").lower()
        if tname == "number":
            try:
                return float(val)
            except Exception:
                txt = _extract_textish(val)
                try:
                    return float(txt) if txt is not None else None
                except Exception:
                    return None
        if tname == "checkbox":
            if isinstance(val, bool):
                return val
            txt = _extract_textish(val)
            if txt is None:
                return None
            return str(txt).strip().lower() in {"1", "true", "yes", "y", "t"}
        return _extract_textish(val)

    if field_schema:
        all_fields = list(field_schema.keys())
        types_map = {k: (field_schema[k].get("type") or "") for k in all_fields}
    else:
        seen = set()
        for rec in records:
            f = rec.get("fields", {}) or {}
            for k in f.keys():
                seen.add(k)
        all_fields = list(seen)
        types_map = {k: "text" for k in all_fields}

    rows: List[Dict[str, Any]] = []
    for rec in records:
        row: Dict[str, Any] = {}
        if keep_record_id:
            row["record_id"] = rec.get("record_id")
        f = rec.get("fields", {}) or {}
        for name in all_fields:
            tname = types_map.get(name, "text")
            row[name] = _flatten_value_by_type(f.get(name), tname)
        for k, v in rec.items():
            if k not in {"fields", "record_id"} and k not in row:
                row[k] = v
        rows.append(row)

    df = pl.DataFrame(rows)
    for name in all_fields:
        if name not in df.columns:
            continue
        tname = (types_map.get(name) or "").lower()
        try:
            if tname == "number":
                df = df.with_columns(pl.col(name).cast(pl.Float64, strict=False))
            elif tname == "checkbox":
                df = df.with_columns(pl.col(name).cast(pl.Boolean, strict=False))
            else:
                df = df.with_columns(pl.col(name).cast(pl.Utf8, strict=False))
        except Exception:
            pass
    if keep_record_id and "record_id" in df.columns:
        df = df.with_columns(pl.col("record_id").cast(pl.Utf8, strict=False))
    return df

# ======================= 4) Polars -> records =======================

def _restore_value_by_type(val: Any, tname: str) -> Any:
    if val is None:
        return None
    if tname == "text":
        s = "" if val is None else str(val)
        return [] if s == "" else [{"type": "text", "text": s}]
    if tname == "number":
        try:
            num = float(val)
            return int(num) if float(num).is_integer() else num
        except Exception:
            return str(val)
    if tname == "checkbox":
        return bool(val)
    if tname in ("date",):
        return str(val)
    if tname in ("single_select", "multi_select"):
        try:
            return json.loads(val)
        except Exception:
            return val
    try:
        return json.loads(val)
    except Exception:
        return val

def polars_to_records_by_schema(
    df: pl.DataFrame,
    field_schema: Dict[str, Dict[str, Any]],
    keep_record_id: bool = True,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    all_fields = list(field_schema.keys())
    for row in df.iter_rows(named=True):
        fields: Dict[str, Any] = {}
        for name in all_fields:
            if name not in row:
                continue
            tname = field_schema[name]["type"]
            restored = _restore_value_by_type(row[name], tname)
            if restored is None or restored == []:
                continue
            fields[name] = restored
        rec: Dict[str, Any] = {"fields": fields}
        if keep_record_id and ("record_id" in df.columns):
            rid = row.get("record_id")
            if rid:
                rec["record_id"] = rid
        out.append(rec)
    return out

# ======================= 5) 宽松版 DF -> records =======================

def df_to_feishu_records(df: pl.DataFrame) -> list[dict]:
    def to_timestamp(v):
        if isinstance(v, (datetime.date, datetime.datetime)):
            return int(time.mktime(v.timetuple()) * 1000)
        elif isinstance(v, str):
            for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
                try:
                    dt = datetime.datetime.strptime(v, fmt)
                    return int(time.mktime(dt.timetuple()) * 1000)
                except Exception:
                    continue
            return v
        return v

    records = []
    for row in df.to_dicts():
        fields = {}
        for k, v in row.items():
            if v is None:
                fields[k] = None
            elif isinstance(v, (datetime.date, datetime.datetime)):
                fields[k] = to_timestamp(v)
            elif isinstance(v, str) and re.match(r"^\d{4}[-/]\d{1,2}[-/]\d{1,2}", v):
                fields[k] = to_timestamp(v)
            elif isinstance(v, float):
                fields[k] = int(v) if v.is_integer() else v
            else:
                fields[k] = v
        records.append({"fields": fields})
    return records

# ======================= 6) 批量写入 =======================

def insert_records_to_feishu(app_id: str, app_secret: str, table_url: str, records: list[dict]):
    app_token, table_id, _ = parse_app_and_table_from_url(table_url)
    client = (
        lark.Client.builder()
        .app_id(app_id)
        .app_secret(app_secret)
        .log_level(lark.LogLevel.DEBUG)
        .build()
    )
    request: BatchCreateAppTableRecordRequest = (
        BatchCreateAppTableRecordRequest.builder()
        .app_token(app_token)
        .table_id(table_id)
        .request_body(
            BatchCreateAppTableRecordRequestBody.builder()
            .records(records)
            .build()
        )
        .build()
    )
    response: BatchCreateAppTableRecordResponse = client.bitable.v1.app_table_record.batch_create(request)
    if not response.success():
        lark.logger.error(
            f"batch_create failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}, "
            f"resp:\n{json.dumps(json.loads(response.raw.content), indent=4, ensure_ascii=False)}"
        )
        return None
    lark.logger.info(lark.JSON.marshal(response.data, indent=4))
    return response.data

# ======================= 7) 传统按 token/table 查询 =======================

def search_records(
    client: lark.Client,
    app_token: str,
    table_id: str,
    *,
    view_id: str | None = None,
    page_size: int = 500,
    max_pages: int = 50,
    field_schema: dict[str, dict] | None = None,
) -> list[dict]:
    id2name = {}
    if field_schema:
        id2name = {meta.get("field_id"): name for name, meta in field_schema.items() if meta.get("field_id")}
    items: list[dict] = []
    page_token = ""
    seen = set()
    page_no = 0
    while True:
        page_no += 1
        req_builder = (
            SearchAppTableRecordRequest.builder()
            .app_token(app_token)
            .table_id(table_id)
            .page_size(page_size)
        )
        if view_id:
            req_builder = req_builder.view_id(view_id)
        if page_token:
            req_builder = req_builder.page_token(page_token)

        body = SearchAppTableRecordRequestBody.builder().build()
        resp = client.bitable.v1.app_table_record.search(req_builder.request_body(body).build())
        if not resp.success():
            if resp.code == 1254030 and page_token:
                print(f"[warn] InvalidPageToken {page_token!r}, restart")
                page_token = ""
                continue
            raise RuntimeError(f"查询记录失败: code={resp.code}, msg={resp.msg}")

        raw_list = getattr(resp.data, "items", []) or []
        for it in raw_list:
            rec_id  = getattr(it, "record_id", None) if not isinstance(it, dict) else it.get("record_id")
            fields  = getattr(it, "fields", {}) if not isinstance(it, dict) else (it.get("fields") or {}) or {}
            if isinstance(fields, dict) and any(isinstance(k, str) and k.startswith("fld") for k in fields.keys()):
                fields = {id2name.get(k, k): v for k, v in fields.items()}
            items.append({"record_id": rec_id, "fields": fields})

        next_token = getattr(resp.data, "page_token", "") or ""
        has_more   = getattr(resp.data, "has_more", None)
        if not next_token or has_more is False:
            break
        if next_token in seen:
            print(f"[warn] repeated page_token {next_token!r}, stop")
            break
        if page_no >= max_pages:
            print(f"[warn] reached max_pages={max_pages}, stop")
            break
        seen.add(next_token)
        page_token = next_token
    return items

# ======================= 8) URL 搜索（支持 date_eq 列表） =======================

def _date_str_to_midnight_ts_ms(date_str: str, tz: str = "Asia/Shanghai") -> int:
    """
    将 'YYYY-MM-DD' 或 'YYYY/MM/DD' 转为该时区当天 00:00:00 的毫秒时间戳
    """
    s = str(date_str).replace("/", "-").strip()
    dt = datetime.datetime.strptime(s, "%Y-%m-%d").replace(
        tzinfo=ZoneInfo(tz), hour=0, minute=0, second=0, microsecond=0
    )
    return int(dt.timestamp() * 1000)

def _coerce_date_eq_list(date_eq: str | int | Iterable | None) -> List[Any]:
    """
    统一把 date_eq 转为列表：
    - 标量 -> [标量]
    - list/tuple -> list(...)
    - 像列表的字符串（"['2025-08-21','2025-08-22']"）-> 解析为列表
    """
    if date_eq is None:
        return []
    if isinstance(date_eq, (list, tuple)):
        return list(date_eq)
    if isinstance(date_eq, str):
        s = date_eq.strip()
        if (s.startswith("[") and s.endswith("]")) or (s.startswith("(") and s.endswith(")")):
            # 先尝试 JSON，再用 literal_eval 兜底
            try:
                return json.loads(s)
            except Exception:
                try:
                    v = ast.literal_eval(s)
                    return list(v) if isinstance(v, (list, tuple)) else [date_eq]
                except Exception:
                    return [date_eq]
        return [date_eq]
    return [date_eq]

def _exactdate_value_from_item(
    item: Any,
    *,
    is_formula_date: bool,
    doc_tz: str,
) -> List[str]:
    """
    单个日期项 -> 官方 ExactDate 值：
    - DateTime 字段: ["ExactDate", "<毫秒字符串>"]
    - 公式日期字段: ["ExactDate", "yyyy/MM/dd"]
    """
    if is_formula_date:
        # 允许传毫秒：转成文档时区的 yyyy/MM/dd
        if isinstance(item, (int, float)) or (isinstance(item, str) and item.isdigit()):
            ts_ms = int(item)
            dt = datetime.datetime.fromtimestamp(ts_ms / 1000.0, tz=ZoneInfo(doc_tz))
            return ["ExactDate", dt.strftime("%Y/%m/%d")]
        return ["ExactDate", str(item).replace("-", "/")]

    # DateTime 字段：需要毫秒字符串
    if isinstance(item, (int, float)) or (isinstance(item, str) and item.isdigit()):
        return ["ExactDate", str(int(item))]
    ts_ms = _date_str_to_midnight_ts_ms(str(item), tz=doc_tz)
    return ["ExactDate", str(ts_ms)]

def search_records_by_url(
    table_url: str,
    app_id: str,
    app_secret: str,
    *,
    view_id: str | None = None,
    user_id_type: str = "user_id",
    page_size: int = 20,          # 官方示例默认 20，可自行调大
    max_pages: int = 50,
    # 日期等值筛选（当日）
    date_field_name: str | None = None,
    date_eq: str | int | Iterable | None = None,   # ✅ 支持列表或列表字符串
    date_is_formula: bool = False,                 # True=公式日期；False=DateTime
    doc_tz: str = "Asia/Shanghai",
    # 其他附加条件（要求已构造好 value 为字符串数组或可转为字符串的元素列表）
    extra_conditions: Iterable[dict] | None = None,
    automatic_fields: bool = False,
    map_field_id_to_name: bool = True,
) -> list[dict]:
    """
    按官方请求结构，通过 URL 搜索（支持 date_eq 为列表；多日期将分别查询再合并去重）
    """
    app_token, table_id, view_in_url = parse_app_and_table_from_url(table_url)
    if view_id is None:
        view_id = view_in_url

    client = build_client(app_id, app_secret)

    # 可选：字段 ID -> 名称
    id2name = {}
    if map_field_id_to_name:
        try:
            schema = fetch_field_schema(client, app_token, table_id)
            id2name = {meta.get("field_id"): name for name, meta in schema.items() if meta.get("field_id")}
        except Exception:
            id2name = {}

    # 统一日期为列表（可能为空表示不加日期条件）
    date_items = _coerce_date_eq_list(date_eq) if (date_field_name and date_eq is not None) else [None]

    def _run_one_query(one_date_item: Any) -> list[dict]:
        # 组装 Filter（严格对齐官方：value 为「字符串数组」）
        cond_list = []

        if date_field_name and one_date_item is not None:
            exact_val = _exactdate_value_from_item(
                one_date_item, is_formula_date=date_is_formula, doc_tz=doc_tz
            )
            exact_val = [str(exact_val[0]), str(exact_val[1])]
            cond_list.append(
                Condition.builder()
                    .field_name(date_field_name)
                    .operator("is")
                    .value(exact_val)
                    .build()
            )

        if extra_conditions:
            for c in extra_conditions:
                rawv = c.get("value") or []
                # 统一转字符串
                val = [str(v) for v in rawv]
                cond_list.append(
                    Condition.builder()
                        .field_name(c["field_name"])
                        .operator(c["operator"])
                        .value(val)
                        .build()
                )

        filter_obj = None
        if cond_list:
            filter_obj = FilterInfo.builder().conjunction("and").conditions(cond_list).build()

        # 翻页
        items: list[dict] = []
        page_token = ""
        seen = set()
        page_no = 0

        while True:
            page_no += 1
            body_b = SearchAppTableRecordRequestBody.builder()
            if view_id:
                body_b = body_b.view_id(view_id)
            if filter_obj is not None:
                body_b = body_b.filter(filter_obj)
            body_b = body_b.automatic_fields(automatic_fields)
            body = body_b.build()

            req_b = (
                SearchAppTableRecordRequest.builder()
                .app_token(app_token)
                .table_id(table_id)
                .user_id_type(user_id_type)
                .page_size(page_size)
            )
            if page_token:
                req_b = req_b.page_token(page_token)

            resp = client.bitable.v1.app_table_record.search(req_b.request_body(body).build())
            if not resp.success():
                raise RuntimeError(
                    f"search failed, code={resp.code}, msg={resp.msg}, log_id={resp.get_log_id()}, "
                    f"resp={resp.raw.content if getattr(resp, 'raw', None) else ''}"
                )

            raw_list = getattr(resp.data, "items", []) or []
            for it in raw_list:
                rec_id = getattr(it, "record_id", None) if not isinstance(it, dict) else it.get("record_id")
                fields = getattr(it, "fields", {}) if not isinstance(it, dict) else (it.get("fields") or {})
                if isinstance(fields, dict) and any(isinstance(k, str) and k.startswith("fld") for k in fields.keys()):
                    fields = {id2name.get(k, k): v for k, v in fields.items()}
                items.append({"record_id": rec_id, "fields": fields})

            next_token = getattr(resp.data, "page_token", "") or ""
            has_more = getattr(resp.data, "has_more", None)

            if not next_token or has_more is False:
                break
            if next_token in seen or page_no >= max_pages:
                break
            seen.add(next_token)
            page_token = next_token
        return items

    # 多日期：分别查询，与其他条件做 AND，最后按 record_id 合并并集
    merged: dict[str, dict] = {}
    for di in date_items:
        for rec in _run_one_query(di):
            rid = rec.get("record_id") or f"__noid__{id(rec)}"
            if rid not in merged:
                merged[rid] = rec
    return list(merged.values())

def search_records_by_url_to_polars(
    table_url: str,
    app_id: str,
    app_secret: str,
    *,
    view_id: str | None = None,
    user_id_type: str = "user_id",
    page_size: int = 20,
    max_pages: int = 50,
    date_field_name: str | None = None,
    date_eq: str | int | Iterable | None = None,  # ✅ 同步支持列表
    date_is_formula: bool = False,
    doc_tz: str = "Asia/Shanghai",
    extra_conditions: Iterable[dict] | None = None,
    automatic_fields: bool = False,
) -> pl.DataFrame:
    """
    便捷封装：按 URL 搜索（支持 date_eq 列表），并直接返回 Polars DataFrame
    """
    app_token, table_id, _ = parse_app_and_table_from_url(table_url)
    client = build_client(app_id, app_secret)
    field_schema = fetch_field_schema(client, app_token, table_id)

    records = search_records_by_url(
        table_url, app_id, app_secret,
        view_id=view_id,
        user_id_type=user_id_type,
        page_size=page_size,
        max_pages=max_pages,
        date_field_name=date_field_name,
        date_eq=date_eq,
        date_is_formula=date_is_formula,
        doc_tz=doc_tz,
        extra_conditions=extra_conditions,
        automatic_fields=automatic_fields,
        map_field_id_to_name=True,
    )
    return records_to_polars_by_schema(records, field_schema)

# ======================= 9) 示例 =======================

if __name__ == "__main__":
    # 请替换为你的真实信息
    exit()
    APP_ID = "cli_a819ae6445685013"
    APP_SECRET = "WZVSbtc80PYSJjDr8CHZDgcbILzKgzW0"
    TABLE_URL = "https://xcn114pn5b7h.feishu.cn/base/N5cFb1e6Za8gShsw6gnc7FWYnod?table=tbl5FH77jwAWnm4S&view=vew5TWuGAM"
    #
    # # 示例1：单日期（DateTime 字段）
    # try:
    #     recs = search_records_by_url(
    #         TABLE_URL, APP_ID, APP_SECRET,
    #         view_id="vew5TWuGAM",
    #         # user_id_type="user_id",
    #         date_field_name="日期",
    #         date_eq="2025-08-21",     # 也可 "2025/08/21" 或 1755705600000
    #         date_is_formula=False,    # DateTime 字段
    #         # doc_tz="Asia/Seoul",
    #         page_size=20,
    #     )
    #     print(f"[single] records: {len(recs)}")
    # except Exception as e:
    #     print("Error(single):", e)
    #
    # # exit()
    # # 示例2：多日期列表（AND 其他条件，OR 日期并集）
    # try:
    #     recs2 = search_records_by_url(
    #         TABLE_URL, APP_ID, APP_SECRET,
    #         view_id="vew5TWuGAM",
    #         date_field_name="日期",
    #         date_eq=["2025-08-21", "2025-08-20"],  # ✅ 直接列表
    #         date_is_formula=False,
    #         # doc_tz="Asia/Seoul",
    #         extra_conditions=[
    #             # 示例：职位 = 初级销售员
    #             # {"field_name": "职位", "operator": "is", "value": ["初级销售员"]}
    #         ],
    #         page_size=200,
    #     )
    #     print(f"[multi] records: {len(recs2)}")
    # except Exception as e:
    #     print("Error(multi):", e)

    # 示例3：直接拿 DataFrame（同样支持 date_eq 列表）
    try:
        df = search_records_by_url_to_polars(
            TABLE_URL, APP_ID, APP_SECRET,
            # view_id="vew5TWuGAM",
            # user_id_type="user_id",
            date_field_name="日期",
            date_eq="['2025-08-21','2025-08-26']",  # ✅ 列表字符串也OK
            date_is_formula=False,
            # doc_tz="Asia/Seoul",
            # page_size=20,
        )
        print(len(df))
        print(df.head())
    except Exception as e:
        print("Error(df):", e)
