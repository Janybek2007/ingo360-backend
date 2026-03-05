from typing import Any

from src.utils.deduplicate_skipped_records import deduplicate_skipped_records


def build_import_result(
    *,
    total: int,
    imported: int,
    skipped_records: list[dict[str, Any]],
    skipped_total: int | None = None,
    inserted: int = 0,
    updated: int = 0,
    deduplicated_in_batch: int = 0,
    **extra: Any,
) -> dict[str, Any]:
    unique_skipped_records = deduplicate_skipped_records(skipped_records)
    skipped_count = (
        skipped_total if skipped_total is not None else len(unique_skipped_records)
    )
    result = {
        "total": total,
        "imported": imported,
        "skipped": skipped_count,
        "inserted": inserted,
        "updated": updated,
        "deduplicated_in_batch": deduplicated_in_batch,
        "skipped_records": unique_skipped_records,
    }
    result.update(extra)
    return result
