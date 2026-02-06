from typing import Any

from sqlalchemy import asc, desc, or_


class ListQueryHelper:
    @staticmethod
    def apply_sorting(
        stmt, sort: dict[str, str] | None, sort_map: dict[str, Any], default_sort
    ):
        if sort:
            for field, direction in sort.items():
                column = sort_map.get(field)
                if column is None:
                    continue
                stmt = stmt.order_by(
                    asc(column) if direction == "ASC" else desc(column)
                )
            return stmt.order_by(default_sort)

        return stmt.order_by(default_sort)

    @staticmethod
    def apply_in_or_null(stmt, column, values: list[int] | None):
        if not values:
            return stmt

        include_null = 0 in values
        non_zero_values = [value for value in values if value != 0]

        if include_null and non_zero_values:
            return stmt.where(or_(column.in_(non_zero_values), column.is_(None)))

        if include_null:
            return stmt.where(column.is_(None))

        return stmt.where(column.in_(non_zero_values))

    @staticmethod
    def apply_pagination(stmt, limit: int | None, offset: int | None):
        if limit:
            stmt = stmt.limit(limit)
        if offset:
            stmt = stmt.offset(offset)
        return stmt
