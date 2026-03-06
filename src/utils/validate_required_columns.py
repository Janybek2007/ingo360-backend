from fastapi import HTTPException


def validate_required_columns(
    records: list[dict],
    required_keys: set[str],
) -> None:
    """
    Проверяет наличие обязательных колонок в Excel.

    Поддерживает альтернативные названия:
    {"область|region"} → достаточно одной из колонок.
    """

    if not records:
        raise HTTPException(
            status_code=422,
            detail="Файл пуст или не содержит данных",
        )

    headers = set(records[0].keys())
    missing: list[str] = []

    for key in required_keys:
        alternatives = [k.strip() for k in key.split("|")]

        if not headers.intersection(alternatives):
            missing.append(" | ".join(alternatives))

    if missing:
        raise HTTPException(
            status_code=422,
            detail="Отсутствуют обязательные колонки:\n\n" + "\n".join(missing),
        )
