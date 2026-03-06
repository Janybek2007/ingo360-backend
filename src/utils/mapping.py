def map_record(record: dict, mapping: dict, foreign_keys: dict | None = None) -> dict:
    mapped = {db_col: record.get(excel_col) for excel_col, db_col in mapping.items()}

    if foreign_keys:
        mapped.update(foreign_keys)

    return mapped
