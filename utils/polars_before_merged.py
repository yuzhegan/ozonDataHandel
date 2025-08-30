# encoding='utf-8

# @Time: 2025-08-29
# @File: %
#!/usr/bin/env
from icecream import ic
import os
import re
import polars as pl
from typing import List, Tuple, Union, Callable, Dict, Sequence

# --- 你的原始工具函数（保留） ---
def _normalize_date_string(x):
    """把任意形式的日期转成 'YYYY/M/D'（无前导零）"""
    if x is None:
        return None
    s = str(x)
    s = s.replace("\u00A0", "").replace(" ", "").strip()
    s = s.replace("-", "/")
    m = re.search(r"(\d{4})\D(\d{1,2})\D(\d{1,2})", s)
    if not m:
        return s
    y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
    return f"{y}/{mo}/{d}"

def _normalize_ozon_id(x):
    """把 Ozon ID 规范为纯字符串（去掉 123.0、科学计数等）"""
    if x is None or str(x).strip() == "":
        return None
    try:
        return str(int(float(str(x))))
    except Exception:
        return str(x).strip()

def _normalize_generic_str(x):
    """通用字符串清洗：去空白，保持为字符串/None"""
    if x is None:
        return None
    s = str(x).replace("\u00A0", "").strip()
    return s if s != "" else None

# --- 简单的列名启发式，自动选择规范化函数 ---
def _is_date_key(name: str) -> bool:
    n = name.lower()
    return ("date" in n) or ("日期" in n)

def _is_id_key(name: str) -> bool:
    n = name.lower()
    # 你可以按需要再扩充关键字
    return ("ozon id" in n) or ("ozonid" in n) or ("sku" in n) or ("item" in n)

def _pick_normalizer(colname: str) -> Callable:
    if _is_date_key(colname):
        return _normalize_date_string
    if _is_id_key(colname):
        return _normalize_ozon_id
    return _normalize_generic_str

KeySpec = Union[str, Tuple[str, ...]]

def normalize_for_join(
    left: pl.DataFrame,
    right: pl.DataFrame,
    left_on: Sequence[KeySpec],
    right_on: Sequence[KeySpec],
    *,
    key_suffix: str = "_key",
    # 可选：手动覆盖某些键的规范化函数（键名 = 输出键名 或 参与 coalesce 的任意源列名）
    normalizer_overrides: Dict[str, Callable] | None = None,
) -> Tuple[pl.DataFrame, pl.DataFrame, List[str], List[str]]:
    """
    将左右两表的连接键统一规范化，并生成新的 *_key 列。
    支持每个键传入一个列名，或一个候选列元组 (会对存在的列做 coalesce)。

    参数
    ----
    left, right:        两个 polars.DataFrame
    left_on, right_on:  键列表。元素可以是 'col' 或 ('col_a','col_b','col_c') 这样的候选元组。
    key_suffix:         生成的键列后缀（默认 '_key'）
    normalizer_overrides:
        显式指定某些键使用的规范化函数。字典 key 可为：
        - 输出键名（例如 '日期_key'），或
        - 任何一个源列名（例如 '日期'、'date_right'、'sku'）
        value 为一个函数：f(x)->str|None

    返回
    ----
    (left2, right2, left_keys, right_keys)
        left2/right2 为新增了 *_key 列后的 DataFrame
        left_keys/right_keys 为对应 join 使用的键列名列表
    """
    if len(left_on) != len(right_on):
        raise ValueError(f"left_on 与 right_on 数量不一致: {len(left_on)} != {len(right_on)}")

    normalizer_overrides = normalizer_overrides or {}

    def _build_one_side(
        df: pl.DataFrame,
        keys: Sequence[KeySpec],
        side_name: str
    ) -> Tuple[pl.DataFrame, List[str]]:
        cols_set = set(df.columns)
        out_keys: List[str] = []
        exprs: List[pl.Expr] = []

        for idx, spec in enumerate(keys):
            candidates: Tuple[str, ...]
            if isinstance(spec, str):
                candidates = (spec,)
            else:
                candidates = spec

            # 仅保留存在的候选列
            present = [c for c in candidates if c in cols_set]
            if not present:
                raise KeyError(
                    f"{side_name} 缺少用于第 {idx+1} 个 join 键的列：候选 {list(candidates)}；现有列：{sorted(cols_set)}"
                )

            # 选择输出键名：以首个“已存在”的候选名为基准
            base_name = present[0]
            out_col = f"{base_name}{key_suffix}"
            out_keys.append(out_col)

            # 构建 coalesce(exprs)
            expr_list = [pl.col(c) for c in present]
            expr = expr_list[0] if len(expr_list) == 1 else pl.coalesce(expr_list)

            # 选择规范化函数（优先 overrides）
            # 1) 以输出键名匹配
            func = normalizer_overrides.get(out_col)
            # 2) 以任一源列名匹配
            if func is None:
                for c in present:
                    if c in normalizer_overrides:
                        func = normalizer_overrides[c]
                        break
            # 3) 自动启发式
            if func is None:
                # 以候选的“最像语义”的列名来决定（优先第一个存在的）
                func = _pick_normalizer(base_name)

            # 应用 map_elements，统一成 Utf8
            exprs.append(
                expr.map_elements(func, return_dtype=pl.Utf8).alias(out_col)
            )

        new_df = df.with_columns(exprs)
        return new_df, out_keys

    left2, lkeys = _build_one_side(left, left_on, "left")
    right2, rkeys = _build_one_side(right, right_on, "right")
    return left2, right2, lkeys, rkeys


# ------------------------------
# 基础规范化工具
# ------------------------------
def _normalize_date_string(x):
    """
    把任意形式的日期文本转成 'YYYY/M/D'（无前导零）。
    若无法解析则原样返回（用于后续 strptime 的 strict=False 宽松解析）。
    """
    if x is None:
        return None
    s = str(x)
    # 去除空白（含不间断空格）
    s = s.replace("\u00A0", "").replace(" ", "").strip()
    # 统一常见分隔符（不强制替换 .，regex 会处理）
    s = s.replace("-", "/")
    m = re.search(r"(\d{4})\D(\d{1,2})\D(\d{1,2})", s)
    if not m:
        return s
    y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
    return f"{y}/{mo}/{d}"

def _normalize_ozon_id(x):
    """
    把 Ozon ID / SKU 等规范为纯字符串，去掉 123.0 / 科学计数 等。
    """
    if x is None or str(x).strip() == "":
        return None
    try:
        return str(int(float(str(x))))
    except Exception:
        return str(x).strip()

def _normalize_generic_str(x):
    """
    通用字符串清洗：去空白，保持为字符串或 None。
    """
    if x is None:
        return None
    s = str(x).replace("\u00A0", "").strip()
    return s if s != "" else None

def _is_date_key(name: str) -> bool:
    n = name.lower()
    return ("date" in n) or ("日期" in n)

def _is_id_key(name: str) -> bool:
    n = name.lower()
    return ("ozon id" in n) or ("ozonid" in n) or ("sku" in n) or ("item" in n)

def _pick_normalizer(colname: str) -> Callable:
    if _is_date_key(colname):
        return _normalize_date_string
    if _is_id_key(colname):
        return _normalize_ozon_id
    return _normalize_generic_str


# ------------------------------
# 1) 任意日期列 -> pl.Date
# ------------------------------
def normalize_date_col(df: pl.DataFrame, col: str, out: str = "Date_key") -> pl.DataFrame:
    """
    将任意形式的日期列（整数/浮点时间戳、字符串日期或字符串时间戳、Datetime、Date）
    统一转换为 pl.Date，输出到 `out` 列。
    """
    if col not in df.columns:
        raise KeyError(f"列不存在: {col}；当前列: {df.columns}")

    dtype = df.schema[col]

    INT_DTYPES = {
        pl.Int8, pl.Int16, pl.Int32, pl.Int64,
        pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
    }
    FLOAT_DTYPES = {pl.Float32, pl.Float64}
    STR_DTYPES = {pl.Utf8}
    if hasattr(pl, "String"):  # 某些版本存在 pl.String
        STR_DTYPES.add(pl.String)

    # --- 数值（秒/毫秒）时间戳 ---
    if dtype in INT_DTYPES or dtype in FLOAT_DTYPES:
        # 用 Float64 判阈值，最后落到 Int64 再 from_epoch
        val_f = pl.col(col).cast(pl.Float64, strict=False)
        val_i = val_f.floor().cast(pl.Int64, strict=False)
        expr = (
            pl.when(val_f >= 1_000_000_000_000.0)
              .then(pl.from_epoch(val_i, time_unit="ms"))
              .otherwise(pl.from_epoch(val_i, time_unit="s"))
              .dt.date()
        )
        return df.with_columns(expr.alias(out))

    # --- 字符串：既可能是文本日期，也可能是“字符串里的纯数字时间戳” ---
    if dtype in STR_DTYPES or dtype == pl.Categorical:
        clean = (
            pl.col(col)
            .cast(pl.Utf8)
            .str.replace_all(r"\s+", "")      # 去空白
            .str.replace_all("-", "/")        # 统一分隔符
            .str.replace_all(r"\.0+$", "")    # 去掉末尾 .0
        )

        # 仅在“纯数字”行保留值；其它行置为 null，避免整列 cast 失败
        ts_num = (
            pl.when(clean.str.contains(r"^\d+$"))
              .then(clean)
              .otherwise(None)
              .cast(pl.Int64, strict=False)
        )

        expr = (
            pl.when(ts_num.is_not_null() & (ts_num >= 1_000_000_000_000))
              .then(pl.from_epoch(ts_num, time_unit="ms"))
            .when(ts_num.is_not_null() & (ts_num >= 1_000_000_000))
              .then(pl.from_epoch(ts_num, time_unit="s"))
            .otherwise(
                clean
                .map_elements(_normalize_date_string, return_dtype=pl.Utf8)
                .str.strptime(pl.Date, format="%Y/%m/%d", strict=False)
                .cast(pl.Datetime)  # 统一分支类型，便于最后 .dt.date()
            )
        )

        return df.with_columns(expr.dt.date().alias(out))

    # --- 已经是 Date/Datetime ---
    if dtype == pl.Date:
        return df.with_columns(pl.col(col).alias(out))
    if dtype == pl.Datetime:
        return df.with_columns(pl.col(col).dt.date().alias(out))

    # --- 兜底：先转字符串再按字符串分支处理 ---
    df2 = df.with_columns(pl.col(col).cast(pl.Utf8).alias(col))
    return normalize_date_col(df2, col, out)

