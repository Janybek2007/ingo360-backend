from fastapi import HTTPException

from src.utils.records_resolver import FieldResolverConfig


class ValidationError(Exception):
    """Ошибка валидации файла импорта"""

    pass


def validate_required_columns(
    records: list[dict],
    required_keys: set[str] | list[FieldResolverConfig],
    raise_exception: bool = True,
) -> tuple[bool, str | None]:
    """
    Проверяет наличие обязательных колонок в Excel.

    Поддерживает альтернативные названия:
    {"область|region"} → достаточно одной из колонок.

    Может принимать либо set[str] (record_keys), либо list[FieldResolverConfig]

    Returns:
        (is_valid, error_message): (True, None) если валидно, (False, error_msg) если ошибка
    """

    if not records:
        error_msg = "Файл пуст или не содержит данных"
        if raise_exception:
            raise HTTPException(status_code=422, detail=error_msg)
        raise ValidationError(error_msg)

    headers = set(records[0].keys())
    missing: list[str] = []

    # Если переданы FieldResolverConfig, фильтруем только required поля
    if isinstance(required_keys, list):
        required_keys = {cfg.record_key for cfg in required_keys if cfg.required}

    for key in required_keys:
        alternatives = [k.strip() for k in key.split("|")]

        if not headers.intersection(alternatives):
            missing.append(" | ".join(alternatives))

    if missing:
        error_msg = "Отсутствуют обязательные колонки:\n\n" + "\n".join(missing)
        if raise_exception:
            raise HTTPException(status_code=422, detail=error_msg)
        raise ValidationError(error_msg)

    return True, None
