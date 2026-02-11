from typing import Any

from sqlalchemy import String, cast, func


def build_period_key(
    group_by_period: str | None,
    table: Any,
    *,
    with_group_fields: bool = False,
    quarter_expr=None,
):
    gb = (group_by_period or "month").strip().lower()

    quarter_col = quarter_expr if quarter_expr is not None else table.quarter

    if gb == "year":
        period_expr = cast(table.year, String)  # "2024"
        group_fields = [table.year]

    elif gb == "quarter":
        period_expr = func.concat(
            cast(table.year, String),
            "-Q",
            cast(quarter_col, String),
        )  # "2024-Q2"
        group_fields = [table.year, quarter_col]

    else:
        period_expr = func.concat(
            cast(table.year, String),
            "-",
            func.to_char(table.month, "FM00"),
        )  # "2024-06"
        group_fields = [table.year, table.month]

    return (period_expr, group_fields) if with_group_fields else period_expr
