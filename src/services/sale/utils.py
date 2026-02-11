from typing import TYPE_CHECKING, Any

from sqlalchemy import literal_column
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
    deduplicated_in_batch = len(rows) - len(deduped_rows)

    max_params = 32767
    columns_per_row = max(1, len(deduped_rows[0]))
    max_rows_per_stmt = max(1, max_params // columns_per_row)

    inserted = 0
    updated = 0

    for start in range(0, len(deduped_rows), max_rows_per_stmt):
        chunk = deduped_rows[start : start + max_rows_per_stmt]
        stmt = pg_insert(model).values(chunk)
        stmt = stmt.on_conflict_do_update(
            constraint=constraint_name,
            set_={
                "packages": stmt.excluded.packages,
                "amount": stmt.excluded.amount,
            },
        )
        stmt = stmt.returning(literal_column("xmax").label("xmax"))
        result = await session.execute(stmt)

        xmax_values = result.scalars().all()
        inserted_in_chunk = sum(1 for xmax in xmax_values if xmax == 0)
        inserted += inserted_in_chunk
        updated += len(xmax_values) - inserted_in_chunk

    imported = len(deduped_rows)

    return imported, inserted, updated, deduplicated_in_batch
