from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Any

import polars as pl


def _get_nested_value(data: dict[str, Any], path: str) -> Any:
    current: Any = data
    for part in path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def _format_template(template: str, data: dict[str, Any]) -> str:
    formatted = template
    for key in data.keys():
        formatted = formatted.replace(f"{{{key}}}", str(data.get(key) or ""))
    return formatted.strip()


def _apply_custom_map(
    rule: dict[str, str], row: dict[str, Any], field_name: str = ""
) -> str:
    # rule: {"company": "компания", "global": "обший"} — value-to-label mapping
    # field_name: имя поля в row (например "mode")

    # 1. Если есть field_name — берём значение этого поля и маппим
    if field_name:
        value = row.get(field_name)
        if value in rule:
            return rule[value]

    # 2. Boolean flag fallback: {"is_active": "Да"} — если поле True/1
    for flag_field, label in rule.items():
        value = row.get(flag_field)
        if value is True or value == 1:
            return label

    # 3. Value-mapping fallback: ищем в row значение совпадающее с ключом rule
    for field_val, label in rule.items():
        for row_val in row.values():
            if row_val == field_val:
                return label

    return ""


def build_export_row_values(
    *,
    row: dict[str, Any],
    headers: list[str],
    header_map: dict[str, str],
    fields_map: dict[str, str] | None = None,
    boolean_map: dict[str, list[str]] | None = None,
    custom_map: dict[str, dict[str, str]] | None = None,
) -> list[Any]:
    fields_map = fields_map or {}
    boolean_map = boolean_map or {}
    custom_map = custom_map or {}

    values: list[Any] = []
    for key in headers:
        if key in custom_map:
            value = _apply_custom_map(custom_map[key], row, field_name=key)
        elif key in fields_map:
            value = _format_template(fields_map[key], row)
        elif "." in key:
            value = _get_nested_value(row, key)
        else:
            value = row.get(key)

        if key in boolean_map and value is not None:
            normalized = 1 if value is True or value == 1 else 0
            labels = boolean_map[key]
            if 0 <= normalized < len(labels):
                value = labels[normalized]

        values.append("" if value is None else value)

    return values


def _build_export_dataframe(
    rows: list[dict[str, Any]],
    header_map: dict[str, str],
    fields_map: dict[str, str] | None = None,
    boolean_map: dict[str, list[str]] | None = None,
    custom_map: dict[str, dict[str, str]] | None = None,
) -> pl.DataFrame:
    headers = list(header_map.keys())
    data = []
    for row in rows:
        values = build_export_row_values(
            row=row,
            headers=headers,
            header_map=header_map,
            fields_map=fields_map,
            boolean_map=boolean_map,
            custom_map=custom_map,
        )
        data.append(values)

    return pl.DataFrame(
        {header_map[key]: col_values for key, col_values in zip(headers, zip(*data))}
    )


def export_excel_to_file(
    *,
    rows: list[dict[str, Any]],
    header_map: dict[str, str],
    output_path: str,
    fields_map: dict[str, str] | None = None,
    boolean_map: dict[str, list[str]] | None = None,
    custom_map: dict[str, dict[str, str]] | None = None,
) -> str:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    df = _build_export_dataframe(rows, header_map, fields_map, boolean_map, custom_map)
    df.write_excel(str(path))
    return str(path)


def export_excel(
    *,
    rows: list[dict[str, Any]],
    header_map: dict[str, str],
    fields_map: dict[str, str] | None = None,
    boolean_map: dict[str, list[str]] | None = None,
    custom_map: dict[str, dict[str, str]] | None = None,
) -> bytes:
    df = _build_export_dataframe(rows, header_map, fields_map, boolean_map, custom_map)

    buffer = BytesIO()
    df.write_excel(buffer)
    buffer.seek(0)
    return buffer.read()
