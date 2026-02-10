from dataclasses import dataclass
from typing import Any, Iterable, Tuple

from sqlalchemy import asc, desc, or_


@dataclass(frozen=True)
class InOrNullSpec:
    column: Any
    values: list[int] | None


@dataclass(frozen=True)
class NumberTypedSpec:
    column: Any
    expr: str | None


@dataclass(frozen=True)
class StringTypedSpec:
    column: Any
    expr: str | None


@dataclass(frozen=True)
class BoolListSpec:
    column: Any
    values: list[str] | None


@dataclass(frozen=True)
class EqualsSpec:
    column: Any
    value: Any


@dataclass(frozen=True)
class SearchSpec:
    term: str | None
    columns: list  # list[Any]


class ListQueryHelper:
    @staticmethod
    def apply_search(stmt, term: str | None, columns: list):
        if not term or not columns:
            return stmt
        like = f"%{term}%"
        return stmt.where(or_(*[c.ilike(like) for c in columns]))

    @staticmethod
    def apply_specs(stmt, specs: Iterable[Any]):
        """
        Применяет набор спецификаций фильтров.
        """
        for spec in specs:
            if spec is None:
                continue

            if isinstance(spec, InOrNullSpec):
                stmt = ListQueryHelper.apply_in_or_null(stmt, spec.column, spec.values)
                continue

            if isinstance(spec, NumberTypedSpec):
                stmt = ListQueryHelper.apply_number_typed_filter(
                    stmt, spec.column, spec.expr
                )
                continue

            if isinstance(spec, StringTypedSpec):
                stmt = ListQueryHelper.apply_string_typed_filter(
                    stmt, spec.column, spec.expr
                )
                continue

            if isinstance(spec, BoolListSpec):
                stmt = ListQueryHelper.apply_boolean_list_filter(
                    stmt, spec.column, spec.values
                )
                continue

            if isinstance(spec, EqualsSpec):
                if spec.value is not None:
                    stmt = stmt.where(spec.column == spec.value)
                continue

            if isinstance(spec, SearchSpec):
                stmt = ListQueryHelper.apply_search(stmt, spec.term, spec.columns)
                continue

        return stmt

    @staticmethod
    def apply_string_typed_filter(stmt, column, expr: str | None):
        """
        Поддержка:
          contains:value      -> ILIKE %value%
          startsWith:value    -> ILIKE value%
          equals:value        -> = value
          doesNotEqual:value  -> != value (и NULL тоже пропускаем как "не равно")
        """
        if not expr:
            return stmt

        parsed = parse_typed_filter(expr)
        if not parsed:
            return stmt

        op, raw = parsed
        raw = raw.strip()
        if raw == "":
            return stmt

        if op == "contains":
            return stmt.where(column.ilike(f"%{raw}%"))
        if op == "startsWith":
            return stmt.where(column.ilike(f"{raw}%"))
        if op == "equals":
            return stmt.where(column == raw)
        if op == "doesNotEqual":
            # чтобы NULL тоже считался "не равно"
            return stmt.where((column.is_(None)) | (column != raw))

        return stmt

    @staticmethod
    def apply_number_typed_filter(stmt, column, expr: str | None):
        """
        Поддержка:
          =:10, >:10, >=:10, <:10, <=:10
          between:10,20  (включительно)
        """
        if not expr:
            return stmt

        parsed = parse_typed_filter(expr)
        if not parsed:
            return stmt

        op, raw = parsed

        def to_int(x: str) -> int | None:
            x = x.strip()
            if x == "":
                return None
            try:
                return int(x)
            except ValueError:
                return None

        if op == "between":
            parts = [p.strip() for p in raw.split(",")]
            if len(parts) != 2:
                return stmt
            a = to_int(parts[0])
            b = to_int(parts[1])
            if a is None or b is None:
                return stmt
            lo, hi = (a, b) if a <= b else (b, a)
            return stmt.where(column.between(lo, hi))

        n = to_int(raw)
        if n is None:
            return stmt

        if op == "=":
            return stmt.where(column == n)
        if op == ">":
            return stmt.where(column > n)
        if op == ">=":
            return stmt.where(column >= n)
        if op == "<":
            return stmt.where(column < n)
        if op == "<=":
            return stmt.where(column <= n)

        return stmt

    @staticmethod
    def build_sort_payload(sort_by: str | None, sort_order: str | None):
        if sort_by and sort_order:
            return {sort_by: sort_order}
        return None

    @staticmethod
    def apply_sorting_with_default(
        stmt,
        sort_by: str | None,
        sort_order: str | None,
        sort_map: dict[str, Any],
        default_sort=None,
    ):
        sort_payload = ListQueryHelper.build_sort_payload(sort_by, sort_order)
        return ListQueryHelper.apply_sorting(stmt, sort_payload, sort_map, default_sort)

    @staticmethod
    def apply_sorting(
        stmt, sort: dict[str, str] | None, sort_map: dict[str, Any], default_sort=None
    ):
        if sort:
            for field, direction in sort.items():
                column = sort_map.get(field)
                if column is None:
                    continue
                stmt = stmt.order_by(
                    asc(column) if direction == "ASC" else desc(column)
                )

        # ✅ поддержка default_sort как списка/кортежа
        if default_sort is not None:
            if isinstance(default_sort, (list, tuple)):
                stmt = stmt.order_by(*default_sort)
            else:
                stmt = stmt.order_by(default_sort)

        return stmt

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

    @staticmethod
    def apply_boolean_list_filter(stmt, column, values: list[str] | None):
        """
        values: ['true'], ['false'], ['true','false']
        """
        if not values:
            return stmt

        bools: list[bool] = []

        for v in values:
            if isinstance(v, bool):
                bools.append(v)
            elif isinstance(v, str):
                if v.lower() == "true":
                    bools.append(True)
                elif v.lower() == "false":
                    bools.append(False)

        # если пришли оба значения — фильтр не нужен
        if len(set(bools)) == 2:
            return stmt

        if not bools:
            return stmt

        if len(bools) == 1:
            return stmt.where(column == bools[0])

        return stmt.where(or_(*(column == b for b in set(bools))))


def parse_typed_filter(expr: str) -> Tuple[str, str] | None:
    """
    expr: "contains:натрий" | ">=:2024" | "between:10,20"
    returns: (op, raw)
    """
    if not expr or ":" not in expr:
        return None
    op, raw = expr.split(":", 1)
    op = (op or "").strip()
    raw = (raw or "").strip()
    if not op or raw == "":
        return None
    return op, raw
