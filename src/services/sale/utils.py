from typing import TYPE_CHECKING, Any

from sqlalchemy import and_, literal_column, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


def _deduplicate_batch_rows(
    rows: list[dict[str, Any]], key_fields: tuple[str, ...]
) -> list[dict[str, Any]]:
    dedup_map: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        key = tuple(row[field] for field in key_fields)
        dedup_map[key] = row
    return list(dedup_map.values())


async def upsert_batch_with_stats(
    session: "AsyncSession",
    model: Any,
    rows: list[dict[str, Any]],
    key_fields: tuple[str, ...],
    constraint_name: str,
) -> tuple[int, int, int, int]:
    if not rows:
        return 0, 0, 0, 0

    deduped_rows = _deduplicate_batch_rows(rows, key_fields)
    file_duplicates = len(rows) - len(deduped_rows)

    max_params = 32767
    columns_per_row = max(1, len(deduped_rows[0]))
    max_rows_per_stmt = max(1, max_params // columns_per_row)

    inserted = 0
    updated = 0

    for start in range(0, len(deduped_rows), max_rows_per_stmt):
        chunk = deduped_rows[start : start + max_rows_per_stmt]

        filters = [
            and_(*(getattr(model, field) == row[field] for field in key_fields))
            for row in chunk
        ]
        existing_count_stmt = (
            select(literal_column("count(*)")).select_from(model).where(or_(*filters))
        )
        existing_count = await session.scalar(existing_count_stmt) or 0

        stmt = pg_insert(model).values(chunk)
        stmt = stmt.on_conflict_do_update(
            constraint=constraint_name,
            set_={
                "packages": stmt.excluded.packages,
                "amount": stmt.excluded.amount,
            },
        )
        await session.execute(stmt)

        inserted_in_chunk = len(chunk) - existing_count
        inserted += inserted_in_chunk
        updated += existing_count

    imported = len(deduped_rows)
    deduplicated = file_duplicates + updated

    return imported, inserted, updated, deduplicated
