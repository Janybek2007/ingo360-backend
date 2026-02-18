from datetime import datetime
from typing import Annotated
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.dependencies.current_user import current_operator_user
from src.db.models import User
from src.db.session import db_session
from src.schemas import base_filter
from src.schemas.export import ExportExcelRequest
from src.schemas.import_log import ImportLogResponse
from src.services.import_log import import_log_service
from src.tasks.import_log_batch_delete import delete_import_log_task

router = APIRouter(dependencies=[Depends(current_operator_user)])


def format_created_at(dt: datetime | None) -> str | None:
    if dt is None:
        return None

    # считаем, что created_at в БД хранится в UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("UTC"))

    bishkek_dt = dt.astimezone(ZoneInfo("Asia/Bishkek"))
    return bishkek_dt.strftime("%d.%m.%Y %H:%M")


@router.get("", response_model=list[ImportLogResponse])
async def get_import_logs(
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
    filters: Annotated[base_filter.BaseFilter, Query()],
):
    return await import_log_service.get_multi(session, filters=filters)


@router.post("/export-excel")
async def export_distributors_excel(
    payload: ExportExcelRequest,
    current_user: Annotated["User", Depends(current_operator_user)],
):
    from src.tasks.export_excel import create_export_task_record, export_excel_task

    task = export_excel_task.delay(
        user_id=current_user.id,
        file_name=payload.file_name,
        service_path="src.services.import_log.ImportLogService",
        model_path="src.db.models.ImportLogs",
        serializer_path="src.schemas.import_log.ImportLogResponse",
        header_map=payload.header_map,
        fields_map=payload.fields_map,
        boolean_map=payload.boolean_map,
        custom_map=payload.custom_map,
    )

    await create_export_task_record(
        task_id=task.id,
        started_by=current_user.id,
        file_path="",
    )

    return {"task_id": task.id}


@router.delete("/{import_log_id}")
async def delete_import_log(
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
    import_log_id: int,
):
    obj = await import_log_service.get_or_404(session, import_log_id)

    delete_import_log_task.delay(import_log_id, obj.target_table_name)

    return {"status": "accepted"}
