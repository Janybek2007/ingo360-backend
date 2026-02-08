from typing import Any


def deduplicate_skipped_records(
    records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    unique_map: dict[str, dict[str, Any]] = {}
    for record in records:
        missing = record.get("missing") or []
        key = ", ".join(missing)
        if key and key not in unique_map:
            unique_map[key] = record
    return list(unique_map.values())
