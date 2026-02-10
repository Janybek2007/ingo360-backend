from datetime import datetime
from typing import Annotated
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.dependencies.current_user import current_operator_user
from src.api.utils.export_excel import export_excel_response
from src.db.session import db_session
from src.schemas import base_filter
from src.schemas.export import ExportExcelRequest
from src.schemas.import_log import ImportLogResponse
from src.services.import_log import import_log_service

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
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
):
    return await export_excel_response(
        payload=payload,
        get_rows=lambda: import_log_service.get_multi(session),
        serialize=lambda il: ImportLogResponse.model_validate(il).model_dump(),
    )


@router.delete("/{import_log_id}")
async def delete_import_log(
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
    import_log_id: int,
):
    return await import_log_service.delete(session, import_log_id)
