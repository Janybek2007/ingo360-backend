from typing import Any

from src.utils.deduplicate_skipped_records import deduplicate_skipped_records


def build_import_result(
    *,
    total: int,
    imported: int,
    skipped_records: list[dict[str, Any]],
    skipped_total: int | None = None,
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
        "skipped_records": unique_skipped_records,
    }
    result.update(extra)
    return result
