from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable

from fastapi import HTTPException
from sqlalchemy import literal_column
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.db.models import SKU
from src.utils.case_insensitive_dict import CaseInsensitiveDict
from src.utils.case_insensitive_set import CaseInsensitiveSet
from src.utils.excel_parser import iter_excel_records
from src.utils.import_result import build_import_result
from src.utils.mapping import map_record
from src.utils.records_resolver import FieldResolverConfig, normalize_record
from src.utils.validate_required_columns import validate_required_columns

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


@dataclass(frozen=True)
class RelationSpec:
    model: Any
    name_key: str
    missing_label: str
    id_field: str | None
    required: bool = True


def is_missing_value(value: Any) -> bool:
    if value is None:
        return True

    if isinstance(value, str):
        stripped = value.strip()
        if stripped == "":
            return True
        if stripped == "-":
            return True
        lowered = stripped.lower()
        if lowered in ("#n/a", "nan", "na"):
            return True

    return False


def format_missing_value(value: Any) -> str:
    if value is None:
        return "(пусто)"

    if isinstance(value, str):
        stripped = value.strip()
        if stripped == "" or stripped == "-":
            return "(пусто)"
        return stripped

    return str(value)


async def import_sales_from_excel(
    *,
    session: "AsyncSession",
    file_path: str,
    user_id: int,
    batch_size: int,
    model: Any,
    import_log_model: Any,
    target_table: str,
    required_fields: list[FieldResolverConfig],
    mapping: dict[str, str],
    key_fields: tuple[str, ...],
    constraint_name: str,
    relations: list[RelationSpec],
    get_id_map: Callable[..., Any],
    normalize_indicator: Callable[[str], str] | None = None,
) -> dict[str, Any]:
    with open(file_path, "rb") as f:
        first_row = next(iter_excel_records(f), None)

    if first_row is None:
        raise HTTPException(status_code=400, detail="Файл пустой")

    _, first_record = first_row
    try:
        validate_required_columns(
            [first_record], required_fields, raise_exception=False
        )
    except Exception as e:
        raise ValueError(str(e))

    total_records = 0

    import_log = import_log_model(
        uploaded_by=user_id,
        target_table=target_table,
        records_count=0,
        target_table_name=model.__tablename__,
    )
    session.add(import_log)
    await session.flush()

    relation_cache_by_key: dict[str, CaseInsensitiveDict] = {
        rel.name_key: CaseInsensitiveDict() for rel in relations
    }
    sku_cache: CaseInsensitiveDict = CaseInsensitiveDict()
    missing_relations_by_key: dict[str, CaseInsensitiveSet] = {
        rel.name_key: CaseInsensitiveSet() for rel in relations
    }
    missing_skus: CaseInsensitiveSet = CaseInsensitiveSet()

    skipped_records: list[dict[str, Any]] = []
    skipped_total = 0
    data_to_insert: list[dict[str, Any]] = []
    records: list[tuple[int, dict[str, Any]]] = []
    relation_names_by_key: dict[str, set[str]] = {
        rel.name_key: set() for rel in relations
    }
    sku_names: set[str] = set()
    imported = 0
    inserted = 0
    updated = 0
    deduplicated = 0

    async def process_records():
        nonlocal skipped_total, imported, inserted, updated, deduplicated
        nonlocal data_to_insert

        if not records:
            return

        for row_index, record in records:
            missing_keys = []
            sku_name = record.get("sku")

            for rel in relations:
                relation_name = record.get(rel.name_key)
                if is_missing_value(relation_name):
                    if not rel.required:
                        continue
                    missing_keys.append(
                        f"{rel.missing_label}: {format_missing_value(relation_name)}"
                    )
                elif relation_name in missing_relations_by_key[rel.name_key]:
                    missing_keys.append(f"{rel.missing_label}: {relation_name}")

            if is_missing_value(sku_name):
                missing_keys.append(f"SKU: {format_missing_value(sku_name)}")
            elif sku_name in missing_skus:
                missing_keys.append(f"SKU: {sku_name}")

            for excel_col in ("упаковки", "сумма"):
                val = record.get(excel_col)
                if val is None:
                    missing_keys.append(f"{excel_col}: (пусто)")
                    continue
                if not isinstance(val, (int, float)):
                    cleaned = str(val).replace(" ", "").replace(",", ".")
                    try:
                        record[excel_col] = float(cleaned)
                    except ValueError:
                        missing_keys.append(
                            f"некорректное число в '{excel_col}': {val}"
                        )

            if missing_keys:
                skipped_total += 1
                skipped_records.append({"row": row_index, "missing": missing_keys})
                continue

            relation_fields = {
                "sku_id": sku_cache[sku_name],
                "import_log_id": import_log.id,
            }
            for rel in relations:
                relation_name = record.get(rel.name_key)
                if rel.id_field and not is_missing_value(relation_name):
                    relation_fields[rel.id_field] = relation_cache_by_key[rel.name_key][
                        relation_name
                    ]
            data_to_insert.append(map_record(record, mapping, relation_fields))

            if len(data_to_insert) >= batch_size:
                (
                    batch_imported,
                    batch_inserted,
                    batch_updated,
                    batch_deduplicated,
                ) = await upsert_batch_with_stats(
                    session=session,
                    model=model,
                    rows=data_to_insert,
                    key_fields=key_fields,
                    constraint_name=constraint_name,
                )
                imported += batch_imported
                inserted += batch_inserted
                updated += batch_updated
                deduplicated += batch_deduplicated
                data_to_insert = []
        records.clear()

    with open(file_path, "rb") as f:
        for row_index, record in iter_excel_records(f):
            total_records += 1
            normalize_record(record, required_fields)
            sku_name = record.get("sku")
            month_value = record.get("месяц")
            record["квартал"] = (int(month_value) - 1) // 3 + 1 if month_value else None

            if normalize_indicator and "indicator" in record:
                raw = record.get("indicator")
                if raw is not None:
                    record["indicator"] = normalize_indicator(str(raw))

            records.append((row_index, record))
            for rel in relations:
                relation_name = record.get(rel.name_key)
                if not is_missing_value(relation_name):
                    relation_names_by_key[rel.name_key].add(relation_name)
            if not is_missing_value(sku_name):
                sku_names.add(sku_name)

    for rel in relations:
        names = relation_names_by_key[rel.name_key]
        if names:
            relation_map, missing = await get_id_map(session, rel.model, "name", names)
            relation_cache_by_key[rel.name_key].update(relation_map)
            missing_relations_by_key[rel.name_key].update(missing)

    if sku_names:
        sku_map, missing = await get_id_map(session, SKU, "name", sku_names)
        sku_cache.update(sku_map)
        missing_skus.update(missing)

    await process_records()

    if data_to_insert:
        (
            batch_imported,
            batch_inserted,
            batch_updated,
            batch_deduplicated,
        ) = await upsert_batch_with_stats(
            session=session,
            model=model,
            rows=data_to_insert,
            key_fields=key_fields,
            constraint_name=constraint_name,
        )
        imported += batch_imported
        inserted += batch_inserted
        updated += batch_updated
        deduplicated += batch_deduplicated

    import_log.records_count = total_records
    await session.commit()

    return build_import_result(
        total=total_records,
        imported=imported,
        skipped_records=skipped_records,
        skipped_total=skipped_total,
        inserted=inserted,
        deduplicated=deduplicated,
    )


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

        stmt = pg_insert(model).values(chunk)
        stmt = stmt.on_conflict_do_update(
            constraint=constraint_name,
            set_={
                "packages": stmt.excluded.packages,
                "amount": stmt.excluded.amount,
            },
        )
        stmt = stmt.returning(literal_column("xmax = 0").label("inserted"))
        result = await session.execute(stmt)
        inserted_flags = result.scalars().all()

        inserted_in_chunk = sum(1 for flag in inserted_flags if flag)
        inserted += inserted_in_chunk
        updated += len(inserted_flags) - inserted_in_chunk

    imported = len(deduped_rows)
    deduplicated = file_duplicates + updated

    return imported, inserted, updated, deduplicated
