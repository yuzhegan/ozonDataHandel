# encoding='utf-8

# @Time: 2025-08-12
# @File: %
#!/usr/bin/env
# feishu_flatten.py
# feishu_type_safe_flatten.py
from __future__ import annotations
from typing import Any, Dict, List, Tuple, Sequence, Optional
from urllib.parse import urlparse, parse_qs
import json
import re
import polars as pl
import lark_oapi as lark
from lark_oapi.api.bitable.v1 import (
    ListAppTableFieldRequest,
    SearchAppTableRecordRequest,
    SearchAppTableRecordRequestBody,
)

# ========== 1) 飞书客户端 & URL 解析 ==========

def build_client(app_id: str, app_secret: str) -> lark.Client:
    assert isinstance(app_id, str) and isinstance(app_secret, str)
    return (
        lark.Client.builder()
        .app_id(app_id)
        .app_secret(app_secret)
        .log_level(lark.LogLevel.INFO)
        .build()
    )

def parse_app_and_table_from_url(table_url: str):
    # 防呆：有人手滑加了逗号 -> ('https://...',)
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
    return app_token, table_id

# ========== 2) 获取表字段 schema（严格按飞书 type） ==========

# 已知的 type 码（官方可能用数字枚举；此处做常见映射，未知一律 -> 'unknown'）
TYPE_CODE_TO_NAME = {
    1: "text",           # 文本（富文本片段数组）
    2: "number",         # 数字
    3: "single_select",  # 单选
    4: "multi_select",   # 多选
    5: "date",           # 日期/时间
    7: "checkbox",       # 勾选
    # 其他很多类型（成员、附件、关联、公式等）在此不强行定义，统一归为 unknown
}

def normalize_type_name(type_value: Any) -> str:
    """
    飞书返回的字段类型可能是数字或字符串；统一成小写字符串。
    未识别的类型 -> 'unknown'
    """
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
    max_pages: int = 20,            # 止损：最多翻 20 页
) -> Dict[str, Dict[str, Any]]:
    """
    返回: { field_name: {"field_id": "...", "type": "<normalized_type>", "raw_type": <原始值>, "property": {...}} }
    - 仅在 page_token 有值时才传参
    - 记录并去重 page_token，避免重复 token 导致死循环
    - 超过 max_pages 主动停止
    """
    schema: Dict[str, Dict[str, Any]] = {}
    page_token: str = ""            # 空串表示“不要传 page_token 参数”
    seen_tokens: set[str] = set()   # 已见过的 token，防止循环

    page_no = 0
    while True:
        page_no += 1
        builder = ListAppTableFieldRequest.builder() \
            .app_token(app_token) \
            .table_id(table_id) \
            .page_size(page_size)

        if page_token:  # 只有有值才带
            builder = builder.page_token(page_token)

        req = builder.build()
        resp = client.bitable.v1.app_table_field.list(req)

        # 异常处理
        if not resp.success():
            # 常见：1254030 InvalidPageToken，重置一次从头来
            if resp.code == 1254030 and page_token:
                print(f"[warn] InvalidPageToken at page_token={page_token!r}, reset to start")
                page_token = ""
                continue
            raise RuntimeError(f"拉取字段失败: code={resp.code}, msg={resp.msg}")

        data = resp.data
        items = getattr(data, "items", None) or []
        for item in items:
            field_name = getattr(item, "field_name", None) or getattr(item, "name", None)
            field_id   = getattr(item, "field_id", None)   or getattr(item, "id", None)
            raw_type   = getattr(item, "type", None)
            prop       = getattr(item, "property", None)
            tname = normalize_type_name(raw_type)
            if field_name:
                schema[field_name] = {
                    "field_id": field_id,
                    "type": tname,
                    "raw_type": raw_type,
                    "property": prop,
                }

        # 下一页 token
        next_token = getattr(data, "page_token", "") or ""

        # 终止条件 1：没有下一页
        if not next_token:
            # print(f"[info] field schema fetched in {page_no} page(s).")
            break

        # 终止条件 2：重复 token（接口异常或 SDK bug）
        if next_token in seen_tokens:
            print(f"[warn] detected repeated page_token={next_token!r}, stop to avoid loop")
            break

        seen_tokens.add(next_token)
        page_token = next_token

        # 终止条件 3：最多翻 max_pages
        if page_no >= max_pages:
            print(f"[warn] reached max_pages={max_pages}, stop to avoid loop")
            break

    return schema

# ========== 3) 按 schema 拍平 records -> DataFrame（不臆测） ==========

RICH_TEXT_JOINER = " "  # 多片段合并分隔

def _is_rich_text_item(x: Any) -> bool:
    return isinstance(x, dict) and isinstance(x.get("type"), str) and ("text" in x)

def _flatten_value_by_type(val: Any, tname: str) -> Any:
    """
    仅按 tname 决定拍平策略；未知类型统一 JSON 字符串。
    """
    if val is None:
        return None

    if tname == "text":
        # 期望是富文本数组 -> 串
        if isinstance(val, list) and all(_is_rich_text_item(x) for x in val):
            pieces = []
            for it in val:
                if it.get("type") == "text":
                    pieces.append(str(it.get("text", "")))
                else:
                    pieces.append(json.dumps(it, ensure_ascii=False))
            return pieces[0] if len(pieces) <= 1 else RICH_TEXT_JOINER.join(pieces)
        # 有些场景 SDK 也可能直接给 str
        if isinstance(val, str):
            return val
        # 其他情况全部 JSON 化
        return json.dumps(val, ensure_ascii=False)

    if tname == "number":
        # 数字或数字字符串
        if isinstance(val, (int, float)):
            return float(val)
        try:
            return float(str(val))
        except Exception:
            # 遇到脏数据，兜底成字符串，避免报错
            return str(val)

    if tname == "checkbox":
        return bool(val)

    if tname in ("date",):
        # 飞书一般给 ISO 字符串，保持原样
        return str(val)

    if tname in ("single_select", "multi_select"):
        # 选择类：出于安全，不擅自展开结构；直接 JSON -> 字符串，保证不丢信息
        # 如果你想显示 name，可在此解析 val 的结构后输出 name/name 列
        return json.dumps(val, ensure_ascii=False)

    # 其余未知类型：统一 JSON 字符串
    return json.dumps(val, ensure_ascii=False)
from typing import Any, Dict, List, Sequence, Literal, Optional
import polars as pl

def records_to_polars_by_schema(
    records: Sequence[Dict[str, Any]],
    field_schema: Dict[str, Dict[str, Any]] | None,
    keep_record_id: bool = True,
    list_text_strategy: Literal["first", "join"] = "first",  # 选“first”取第一个，“join”拼接全部
    join_sep: str = ";",
) -> pl.DataFrame:
    """
    将飞书多维表格 records 扁平化为 Polars DataFrame。
    - 对于形如 [{"text": "…"}] 的列表，或 {"value": [{"text": "…"}]} 的选择器对象，
      默认取第一个 text（list_text_strategy="first"）。
    - 若希望拼接全部文本，设置 list_text_strategy="join"。
    """
    def _first_text_from_list(lst: list[Any]) -> Optional[str]:
        # 从列表中找第一个有 text 的元素
        for x in lst:
            if isinstance(x, dict):
                t = x.get("text")
                if t is not None:
                    return str(t)
            elif isinstance(x, (str, int, float, bool)):
                return str(x)
        return None

    def _all_texts_from_list(lst: list[Any]) -> List[str]:
        out: List[str] = []
        for x in lst:
            if isinstance(x, dict) and "text" in x and x["text"] is not None:
                out.append(str(x["text"]))
            elif isinstance(x, (str, int, float, bool)):
                out.append(str(x))
        return out

    def _extract_textish(value: Any) -> Optional[str]:
        """
        通用抽取：支持
        - {"text": "..."}
        - {"value": [ {...}, {...} ]}   # 选择器/多选
        - [ {...}, {...} ]              # 富文本/多值
        - 纯标量
        """
        if value is None:
            return None

        # 纯字符串/数字/布尔 -> 转成字符串
        if isinstance(value, (str, int, float, bool)):
            return str(value)

        # 列表：取第一个/拼接全部
        if isinstance(value, list):
            if list_text_strategy == "first":
                return _first_text_from_list(value)
            else:
                all_txt = _all_texts_from_list(value)
                return join_sep.join(all_txt) if all_txt else None

        # 字典：可能是 {"text": ...} 或 {"value": [...]} 等
        if isinstance(value, dict):
            if "text" in value and value["text"] is not None:
                return str(value["text"])

            # 飞书 select/multi_select 常见格式：{"type": 1, "value": [ {"text": "..."} ]}
            if "value" in value and isinstance(value["value"], list):
                if list_text_strategy == "first":
                    return _first_text_from_list(value["value"])
                else:
                    all_txt = _all_texts_from_list(value["value"])
                    return join_sep.join(all_txt) if all_txt else None

            # 其他字典，尽量找 text
            for k in ("name", "title", "label"):
                if k in value and value[k] is not None:
                    return str(value[k])

            # 实在不行就转字符串
            return str(value)

        # 其他非常规类型
        return str(value)

    def _flatten_value_by_type(val: Any, tname: str) -> Any:
        """
        依据 schema 的类型做基本扁平：
        - number/checkbox 保持数值/布尔
        - 其他一律抽取 textish（含选择器、列表等）
        """
        if val is None:
            return None

        tname = (tname or "").lower()
        if tname in {"number"}:
            # 尽量转成 float
            try:
                return float(val)
            except Exception:
                # 如果 val 是 dict/list，抽文本再转
                txt = _extract_textish(val)
                try:
                    return float(txt) if txt is not None else None
                except Exception:
                    return None

        if tname in {"checkbox"}:
            if isinstance(val, bool):
                return val
            # 尝试把 "true"/"1" 等转 bool
            txt = _extract_textish(val)
            if txt is None:
                return None
            return str(txt).strip().lower() in {"1", "true", "yes", "y", "t"}

        # 其他类型（text/rich_text/url/select/multi_select/...）统一抽取文本
        return _extract_textish(val)

    # 字段清单（容错：若 schema 为空/None，则从记录里收集 keys）
    if field_schema:
        all_fields = list(field_schema.keys())
        types_map = {k: (field_schema[k].get("type") or "") for k in all_fields}
    else:
        # 无 schema：按记录里出现过的字段来；类型一律按 text 处理
        seen = set()
        for rec in records:
            f = rec.get("fields", {}) or {}
            for k in f.keys():
                seen.add(k)
        all_fields = list(seen)
        types_map = {k: "text" for k in all_fields}

    # 行构造
    rows: List[Dict[str, Any]] = []
    for rec in records:
        row: Dict[str, Any] = {}
        if keep_record_id:
            row["record_id"] = rec.get("record_id")
        f = rec.get("fields", {}) or {}

        for name in all_fields:
            tname = types_map.get(name, "text")
            row[name] = _flatten_value_by_type(f.get(name), tname)

        # 允许顶层额外字段（比如你填的 “日期”）
        for k, v in rec.items():
            if k not in {"fields", "record_id"} and k not in row:
                row[k] = v
        rows.append(row)

    df = pl.DataFrame(rows)

    # 基于类型再做一次安全 cast
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


# ========== 4) 按 schema 还原 DataFrame -> records（不臆测） ==========

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
            # 脏值以字符串回写，避免失败
            return str(val)

    if tname == "checkbox":
        return bool(val)

    if tname in ("date",):
        return str(val)

    if tname in ("single_select", "multi_select"):
        # 选择类：我们之前存成 JSON 字符串，这里尝试反序列化回原结构
        try:
            return json.loads(val)
        except Exception:
            return val

    # 其他未知类型：尝试 JSON 反序列化
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
                # 空值不写，避免覆盖
                continue
            fields[name] = restored

        rec: Dict[str, Any] = {"fields": fields}
        if keep_record_id and ("record_id" in df.columns):
            rid = row.get("record_id")
            if rid:
                rec["record_id"] = rid
        out.append(rec)

    return out

# ========== 5) 读取与示例 ==========

# utils/feishu_records_polars.py

from lark_oapi.api.bitable.v1 import (
    SearchAppTableRecordRequest,
    SearchAppTableRecordRequestBody,
)

def search_records(
    client,
    app_token: str,
    table_id: str,
    *,
    view_id: str | None = None,
    page_size: int = 500,
    max_pages: int = 50,
    field_schema: dict[str, dict] | None = None,   # <== 新增，用于 fld->name 映射
):
    id2name = {}
    if field_schema:
        id2name = {meta.get("field_id"): name for name, meta in field_schema.items() if meta.get("field_id")}

    items: list[dict] = []
    page_token = ""
    seen_tokens: set[str] = set()
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
        req = req_builder.request_body(body).build()

        resp = client.bitable.v1.app_table_record.search(req)
        if not resp.success():
            if resp.code == 1254030 and page_token:
                print(f"[warn] InvalidPageToken at page_token={page_token!r}, reset to start")
                page_token = ""
                continue
            raise RuntimeError(f"查询记录失败: code={resp.code}, msg={resp.msg}")

        raw_list = getattr(resp.data, "items", []) or []
        for it in raw_list:
            # it 可能是 AppTableRecord 对象
            rec_id  = getattr(it, "record_id", None) if not isinstance(it, dict) else it.get("record_id")
            fields  = getattr(it, "fields", {}) if not isinstance(it, dict) else (it.get("fields") or {})
            fields  = fields or {}

            # 若返回的是以字段ID为键，映射成字段名称
            if isinstance(fields, dict) and any(isinstance(k, str) and k.startswith("fld") for k in fields.keys()):
                fields = { id2name.get(k, k): v for k, v in fields.items() }

            items.append({"record_id": rec_id, "fields": fields})

        next_token = getattr(resp.data, "page_token", "") or ""
        has_more   = getattr(resp.data, "has_more", None)

        if not next_token or has_more is False:
            break
        if next_token in seen_tokens:
            print(f"[warn] detected repeated page_token={next_token!r}, stop to avoid loop")
            break
        if page_no >= max_pages:
            print(f"[warn] reached max_pages={max_pages}, stop to avoid loop")
            break

        seen_tokens.add(next_token)
        page_token = next_token

    return items


if __name__ == "__main__":
    exit()
    # 1) 基础配置
    APP_ID = "cli_xxx"          # 替换
    APP_SECRET = "xxx"          # 替换
    TABLE_URL = "https://xxx.feishu.cn/base/APP_TOKEN?table=TABLE_ID&view=VIEW_ID"  # 替换

    client = build_client(APP_ID, APP_SECRET)
    app_token, table_id = parse_app_and_table_from_url(TABLE_URL)

    # 2) 拉 schema（严格按飞书返回）
    field_schema = fetch_field_schema(client, app_token, table_id)

    # 3) 拉 records -> DataFrame
    records = search_records(client, app_token, table_id)
    df = records_to_polars_by_schema(records, field_schema)

    # 4) 你在 df 上做你的分析/修改……
    # 例：把 text 类型的 'sku' 转大写（如果存在且为 text）
    if "sku" in df.columns and field_schema.get("sku", {}).get("type") == "text":
        df = df.with_columns(pl.col("sku").str.to_uppercase())

    # 5) 还原回 records（严格按 schema）
    restored = polars_to_records_by_schema(df, field_schema)

    # 6) 打印验证
    print(df)
    print(json.dumps(restored[:1], ensure_ascii=False, indent=2))
