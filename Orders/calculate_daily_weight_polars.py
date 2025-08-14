# encoding='utf-8

# @Time: 2025-08-10
# @File: %
#!/usr/bin/env
# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
"""
Compute weighted daily sales from pre‑aggregated multi‑window sales
data using Polars.  This module defines a helper function,
``calculate_weighted_daily_sales``, that takes a Polars DataFrame
containing columns of the form ``sum_X`` (where ``X`` is a lookback
window in days) and computes a new ``daily_sales`` column using a
weighted average of per‑day sales across the specified windows.

Daily sales are calculated according to the formula:

``daily_sales = Σ[ (sum_X / X) * weight_X ]``

where ``sum_X`` is the total sales over the last ``X`` days and
``weight_X`` is a user‑supplied weight for that window.  The
function also provides an option to aggregate the resulting daily
sales by one or more grouping fields (e.g. by ``Ozon ID``).

Example usage::

    import polars as pl
    from calculate_daily_sales_polars import calculate_weighted_daily_sales

    # Suppose ``df`` has columns: '货号', 'Ozon ID', 'sum_7', 'sum_14',
    # 'sum_28', 'sum_60', 'sum_90'.  Define weights for each window.
    weights = {7: 0.4, 14: 0.3, 28: 0.15, 60: 0.1, 90: 0.05}

    # Compute daily sales for each row, then aggregate by Ozon ID
    df_daily = calculate_weighted_daily_sales(df, weights=weights, windows=[7,14,28,60,90], group_by=["Ozon ID"])
    print(df_daily)

If ``group_by`` is ``None`` or empty, the function returns the input
DataFrame with an additional ``daily_sales`` column, without
aggregating.  If grouping fields are provided, it returns a
DataFrame aggregated by those fields, with ``daily_sales`` summed
within each group.

This helper is useful when combined with the multi‑window summaries
produced by ``summarize_order_info_windows_polars``.
"""

from __future__ import annotations

from typing import Sequence, Dict, Optional, List

import polars as pl

__all__ = [
    "calculate_weighted_daily_sales",
    "calculate_dynamic_daily_sales",
]


def calculate_weighted_daily_sales(
    df: pl.DataFrame,
    *,
    weights: Optional[Dict[int, float]] = None,
    windows: Sequence[int] = (7, 14, 28, 60, 90),
    group_by: Optional[Sequence[str]] = ("Ozon ID",),
    group_field: Optional[str] = None,
) -> pl.DataFrame:
    """Compute weighted daily sales from multi‑window sales columns.

    Parameters
    ----------
    df:
        Polars DataFrame containing columns named ``sum_X`` for each
        window ``X`` in ``windows``.  Other columns (e.g. group
        identifiers) are preserved.
    weights:
        A mapping from window length to its weight in the daily
        sales calculation.  If omitted or if a weight for a
        particular window is missing, that window defaults to a
        weight of 1.0 (equal weighting).  Weights do not need to
        sum to 1.
    windows:
        Sequence of window lengths (in days) to use.  The function
        will look for columns named ``sum_{w}`` for each ``w``.  If
        a particular column is missing, its contribution is
        ignored.
    group_by:
        Optional list of columns by which to aggregate the computed
        ``daily_sales``.  If ``None`` or an empty sequence, no
        aggregation is performed and the result contains the same
        number of rows as ``df`` with an added ``daily_sales``
        column.  If provided, the result contains one row per
        unique combination of the grouping fields with ``daily_sales``
        summed across matching rows.
    group_field:
        Alias for ``group_by`` when only a single grouping column is
        desired.  If both ``group_by`` and ``group_field`` are
        provided, ``group_by`` takes precedence.  This parameter
        exists for backward compatibility with earlier examples that
        used ``group_field`` instead of ``group_by``.

    Returns
    -------
    pl.DataFrame
        Either the input DataFrame with an additional ``daily_sales``
        column (when ``group_by`` is empty), or a new DataFrame
        containing only the grouping fields and the summed
        ``daily_sales``.
    """
    if weights is None:
        weights = {}
    # Build the expression for daily_sales as the sum of weighted per-day sales
    terms = []
    for w in windows:
        col_name = f"sum_{w}"
        if col_name not in df.columns:
            # Skip missing columns gracefully
            continue
        weight = float(weights.get(w, 1.0))
        # Each term: (sum_w / w) * weight
        term_expr = (pl.col(col_name).cast(pl.Float64) / float(w)) * weight
        terms.append(term_expr)
    if not terms:
        raise ValueError("No valid sum_X columns found in DataFrame for the provided windows")
    # Sum all terms to form the daily_sales expression
    daily_sales_expr = terms[0]
    for expr in terms[1:]:
        daily_sales_expr = daily_sales_expr + expr
    daily_sales_expr = daily_sales_expr.alias("daily_sales")
    # Add daily_sales to the DataFrame
    df_with_daily = df.with_columns(daily_sales_expr)
    # If grouping is requested, aggregate daily_sales by the specified fields.
    # Determine grouping fields: use group_by if provided and non-empty;
    # otherwise fall back to group_field.  If neither is provided, return
    # the DataFrame with the new column.
    gb_fields: Optional[List[str]] = None
    if group_by:
        gb_fields = list(group_by)
    elif group_field:
        gb_fields = [group_field]
    if gb_fields:
        return df_with_daily.group_by(gb_fields).agg(pl.col("daily_sales").sum())
    return df_with_daily


def calculate_dynamic_daily_sales(
    df: pl.DataFrame,
    *,
    windows: Sequence[int] = (7, 14, 28, 60, 90),
    group_fields: Sequence[str] = ("Ozon ID",),
    top_k: int = 3,
) -> pl.DataFrame:
    """Compute daily sales with dynamically derived weights per group.

    This helper aggregates the provided DataFrame by ``group_fields``
    (e.g. [``Ozon ID``]) to sum the multi‑window sales columns, then
    computes per‑window average daily sales (``sum_X / X``) for each
    window ``X``.  It selects the top ``top_k`` windows with the
    highest average daily sales, derives weights by normalising
    these top averages (windows outside the top ``k`` receive zero
    weight), and then computes a weighted daily sales estimate for
    each group.  The result contains two columns: ``group_field``
    and ``daily_sales``.

    Parameters
    ----------
    df:
        Polars DataFrame with columns named ``sum_X`` for each
        window ``X`` in ``windows``.  Additional columns are
        ignored except for those in ``group_fields``.
    windows:
        Sequence of window lengths (days) to consider.  Only
        columns present in the DataFrame will be used.
    group_fields:
        A sequence of columns to group by when aggregating
        multi‑window sums.  For example, ``["货号", "Ozon ID", "配送集群"]``.
    top_k:
        Number of top windows (by average daily sales) to use when
        computing weights.  Windows not among the top ``k`` receive
        zero weight.

    Returns
    -------
    pl.DataFrame
        A DataFrame containing the grouping fields and a
        ``daily_sales`` column representing the weighted average
        daily sales for each group.
    """
    if not windows:
        raise ValueError("windows must be a non-empty sequence")
    # Aggregate the sum_X columns by the group_field.
    agg_exprs = []
    for w in windows:
        col_name = f"sum_{w}"
        if col_name in df.columns:
            agg_exprs.append(pl.col(col_name).sum().alias(col_name))
    if not agg_exprs:
        raise ValueError("No matching sum_X columns found in the DataFrame")
    # Group by all specified fields
    grouped = df.group_by(list(group_fields)).agg(agg_exprs)
    # Compute daily sales with dynamic weights per group
    result_rows = []
    for row in grouped.iter_rows(named=True):
        # Extract group identifiers for all group_fields
        group_value = {fld: row[fld] for fld in group_fields}
        # Compute average daily sales for each window; handle missing
        # sums by treating them as zero.
        avg_daily = {}
        for w in windows:
            col_name = f"sum_{w}"
            sum_val = row.get(col_name, 0)
            try:
                avg_val = float(sum_val) / float(w)
            except Exception:
                avg_val = 0.0
            avg_daily[w] = avg_val
        # Determine the top_k windows by average daily sales.  If
        # there are fewer than k windows available, use all.
        sorted_windows = sorted(avg_daily.keys(), key=lambda x: avg_daily[x], reverse=True)
        top_windows = sorted_windows[:min(top_k, len(sorted_windows))]
        # Compute the sum of averages for the top windows
        total_top_avg = sum(avg_daily[w] for w in top_windows) if top_windows else 0
        # Derive weights: ratio of each top window's average to the total; others get 0
        weights = {}
        for w in windows:
            if total_top_avg > 0 and w in top_windows:
                weights[w] = avg_daily[w] / total_top_avg
            else:
                weights[w] = 0.0
        print(f"Group: {group_value}, Weights: {weights}")
        # Compute the weighted daily sales
        daily_sales = 0.0
        for w in windows:
            sum_val = row.get(f"sum_{w}", 0)
            if sum_val is None:
                sum_val = 0
            try:
                daily_sales += (float(sum_val) / float(w)) * weights[w]
            except Exception:
                continue
        # Compose the result dict with all group identifiers
        result_row = dict(group_value)
        result_row["daily_sales"] = daily_sales
        result_rows.append(result_row)
    return pl.DataFrame(result_rows)


if __name__ == "__main__":
    exit()

    # df 是包含 sum_7、sum_14、... 等列的数据框，按 Ozon ID 聚合各 SKU 后再计算动态日销量
    df_daily = calculate_dynamic_daily_sales(
        df,
        windows=[7, 14, 28, 60, 90],
        group_field="Ozon ID",
        top_k=3
    )
    print(df_daily)
    # 输出包含 Ozon ID 和 dynamic daily_sales 两列


    weights = {7: 0.4, 14: 0.3, 28: 0.15, 60: 0.1, 90: 0.05}

    # group_by=["Ozon ID"] 或 group_field="Ozon ID" 均可
    df_daily_weighted = calculate_weighted_daily_sales(
        df,
        weights=weights,
        windows=[7, 14, 28, 60, 90],
        group_field="Ozon ID"
    )
