import asyncio
import json
import os
from pathlib import Path
from typing import Any, Iterator

import polars as pl
from sqlalchemy.orm import joinedload

from src.celery_app import celery_app
from src.db.models.excel_tasks import ExcelTaskStatus, ExcelTaskType
from src.tasks.utils import (
    create_excel_task_record,
    format_error_message,
    get_async_session_context,
    import_class,
    publish_excel_status,
    update_excel_task_result,
)
from src.utils.export_excel import build_export_row_values


def _sanitize_file_name(file_name: str) -> str:
    cleaned = (file_name or "export").strip().replace("/", "_").replace("\\", "_")
    return cleaned or "export"


async def create_export_task_record(
    *,
    task_id: str,
    started_by: int,
    file_path: str = "",
) -> None:
    await create_excel_task_record(
        task_id=task_id,
        started_by=started_by,
        file_path=file_path,
        task_type=ExcelTaskType.EXPORT,
    )


def _build_export_file_path(task_id: str, file_name: str) -> str:
    temp_dir = Path(os.getenv("EXPORT_TEMP_DIR", "temp"))
    try:
        temp_dir.mkdir(parents=True, exist_ok=True)
        if not os.access(temp_dir, os.W_OK):
            raise PermissionError(f"Temp dir not writable: {temp_dir}")
    except OSError:
        temp_dir = Path("/tmp/ingo360-export")
        temp_dir.mkdir(parents=True, exist_ok=True)
    safe_file_name = _sanitize_file_name(file_name)
    file_path = temp_dir / f"export_{task_id}_{safe_file_name}.xlsx"
    return str(file_path)


def _iter_rows_from_jsonl(rows_file_path: str) -> Iterator[dict[str, Any]]:
    with open(rows_file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def _build_joinedload_option(model_cls: Any, relation_path: str):
    parts = [part for part in relation_path.split(".") if part]
    if not parts:
        return None

    try:
        first_attr = getattr(model_cls, parts[0])
    except AttributeError:
        return None

    option = joinedload(first_attr)

    current_cls = first_attr.property.mapper.class_
    current_option = option
    for part in parts[1:]:
        try:
            rel_attr = getattr(current_cls, part)
        except AttributeError:
            return None
        current_option = current_option.joinedload(rel_attr)
        current_cls = rel_attr.property.mapper.class_

    return current_option


def _write_export_file_from_jsonl(
    *,
    rows_file_path: str,
    output_path: str,
    header_map: dict[str, str],
    fields_map: dict[str, str] | None = None,
    boolean_map: dict[str, list[str]] | None = None,
    custom_map: dict[str, dict[str, str]] | None = None,
) -> None:
    headers = list(header_map.keys())

    # Build data with transformed values
    data = []
    for row in _iter_rows_from_jsonl(rows_file_path):
        values = build_export_row_values(
            row=row,
            headers=headers,
            header_map=header_map,
            fields_map=fields_map,
            boolean_map=boolean_map,
            custom_map=custom_map,
        )
        data.append(values)

    # Create DataFrame with display names as column names
    df = pl.DataFrame(
        {header_map[key]: col_values for key, col_values in zip(headers, zip(*data))}
    )

    df.write_excel(output_path)


async def _write_export_file_from_service(
    *,
    output_path: str,
    header_map: dict[str, str],
    service_path: str,
    model_path: str,
    serializer_path: str,
    load_options_paths: list[str] | None = None,
    chunk_size: int = 1000,
    fields_map: dict[str, str] | None = None,
    boolean_map: dict[str, list[str]] | None = None,
    custom_map: dict[str, dict[str, str]] | None = None,
) -> None:
    service_cls = import_class(service_path)
    model_cls = import_class(model_path)
    serializer_cls = import_class(serializer_path)

    # Автоматически маппим старые пути Doctor → через GlobalDoctor
    load_option_remap = {
        "medical_facility": "global_doctor.medical_facility",
        "speciality": "speciality",
    }
    remapped_paths = [load_option_remap.get(p, p) for p in (load_options_paths or [])]

    load_options = []
    for path in remapped_paths:
        option = _build_joinedload_option(model_cls, path)
        if option is not None:
            load_options.append(option)

    headers = list(header_map.keys())

    # Build data with transformed values
    data = []
    async with get_async_session_context() as session:
        service = service_cls(model_cls)
        async for item in service.iter_multi(
            session=session,
            load_options=load_options,
            chunk_size=chunk_size,
        ):
            row = serializer_cls.model_validate(item).model_dump()
            values = build_export_row_values(
                row=row,
                headers=headers,
                header_map=header_map,
                fields_map=fields_map,
                boolean_map=boolean_map,
                custom_map=custom_map,
            )
            data.append(values)

    # Create DataFrame with display names as column names
    df = pl.DataFrame(
        {header_map[key]: col_values for key, col_values in zip(headers, zip(*data))}
    )

    df.write_excel(output_path)


@celery_app.task(bind=True)
def export_excel_task(
    self,
    *,
    user_id: int,
    file_name: str,
    rows_file_path: str | None = None,
    service_path: str | None = None,
    model_path: str | None = None,
    serializer_path: str | None = None,
    load_options_paths: list[str] | None = None,
    chunk_size: int = 1000,
    header_map: dict[str, str],
    fields_map: dict[str, str] | None = None,
    boolean_map: dict[str, list[str]] | None = None,
    custom_map: dict[str, dict[str, str]] | None = None,
):
    task_id = self.request.id

    async def _run():
        from src.db.session import db_session

        await db_session.engine.dispose()

        saved_file_path = _build_export_file_path(task_id, file_name)
        try:
            if service_path and model_path and serializer_path:
                await _write_export_file_from_service(
                    output_path=saved_file_path,
                    header_map=header_map,
                    service_path=service_path,
                    model_path=model_path,
                    serializer_path=serializer_path,
                    load_options_paths=load_options_paths,
                    chunk_size=chunk_size,
                    fields_map=fields_map,
                    boolean_map=boolean_map,
                    custom_map=custom_map,
                )
            elif rows_file_path:
                _write_export_file_from_jsonl(
                    rows_file_path=rows_file_path,
                    output_path=saved_file_path,
                    header_map=header_map,
                    fields_map=fields_map,
                    boolean_map=boolean_map,
                    custom_map=custom_map,
                )
            else:
                raise ValueError(
                    "Не передан источник данных: rows_file_path или service/model/serializer"
                )

            updated_task = await update_excel_task_result(
                task_id=task_id,
                status=ExcelTaskStatus.COMPLETED,
                saved_file_path=saved_file_path,
                reset_download=True,
            )
            return "completed", saved_file_path, updated_task

        except Exception as e:
            error_message = format_error_message(e)
            updated_task = await update_excel_task_result(
                task_id=task_id,
                status=ExcelTaskStatus.FAILED,
                saved_file_path="",
                error=error_message,
            )
            return "failed", saved_file_path, (updated_task, error_message)

    try:
        status, saved_file_path, extra = asyncio.run(_run())

        if status == "completed":
            updated_task = extra
            payload = {
                "type": "excel_exported",
                "user_id": user_id,
                "task_id": task_id,
                "status": "completed",
                "result": {
                    "saved_file_path": saved_file_path,
                    "file_name": f"{_sanitize_file_name(file_name)}.xlsx",
                    "created_at": (
                        updated_task.created_at.isoformat()
                        if updated_task and updated_task.created_at
                        else None
                    ),
                },
            }
            publish_excel_status(payload)
            return {"saved_file_path": saved_file_path}
        else:
            updated_task, error_message = extra
            payload = {
                "type": "excel_exported",
                "user_id": user_id,
                "task_id": task_id,
                "status": "failed",
                "result": {
                    "saved_file_path": None,
                    "file_name": f"{_sanitize_file_name(file_name)}.xlsx",
                    "created_at": (
                        updated_task.created_at.isoformat()
                        if updated_task and updated_task.created_at
                        else None
                    ),
                },
            }
            publish_excel_status(payload)
            raise RuntimeError(error_message)
    finally:
        try:
            if rows_file_path and Path(rows_file_path).exists():
                os.remove(rows_file_path)
        except Exception:
            pass
