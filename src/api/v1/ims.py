from typing import TYPE_CHECKING, Annotated

from fastapi import APIRouter, Depends, UploadFile

from src.api.dependencies.current_user import current_active_user, current_operator_user
from src.db.session import db_session
from src.schemas.export import ExportExcelRequest
from src.schemas.ims import (
    IMSCreate,
    IMSRequest,
    IMSResponse,
    IMSTableFilter,
    IMSTopFilter,
    IMSUpdate,
)
from src.services.ims import ims_service

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from src.db.models import User


router = APIRouter()


@router.post(
    "", dependencies=[Depends(current_operator_user)], response_model=list[IMSResponse]
)
async def get_all_ims(
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
    filters: IMSRequest,
):
    return await ims_service.get_multi(session, filters)


@router.post("/export-excel")
async def export_visits_excel(
    payload: ExportExcelRequest,
    current_user: Annotated["User", Depends(current_active_user)],
):
    from src.tasks.export_excel import create_export_task_record, export_excel_task

    task = export_excel_task.delay(
        user_id=current_user.id,
        file_name=payload.file_name,
        service_path="src.services.ims.IMSMetricsService",
        model_path="src.db.models.IMS",
        serializer_path="src.schemas.ims.IMSResponse",
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


@router.post(
    "create", dependencies=[Depends(current_operator_user)], response_model=IMSResponse
)
async def create_ims(
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
    new_ims: IMSCreate,
):
    return await ims_service.create(session, new_ims)


@router.post("/import-excel")
async def ims_import_excel(
    file: UploadFile,
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
    current_user: Annotated["User", Depends(current_operator_user)],
):
    return await ims_service.import_excel(session, file, current_user.id)


@router.get(
    "/{ims_id}",
    response_model=IMSResponse,
    dependencies=[Depends(current_operator_user)],
)
async def get_ims(
    session: Annotated["AsyncSession", Depends(db_session.get_session)], ims_id: int
):
    return await ims_service.get_or_404(session, ims_id)


@router.patch(
    "/{ims_id}",
    response_model=IMSResponse,
    dependencies=[Depends(current_operator_user)],
)
async def update_ims(
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
    ims_id: int,
    ims: IMSUpdate,
):
    return await ims_service.update(session, ims_id, ims)


@router.delete("/{ims_id}", dependencies=[Depends(current_operator_user)])
async def delete_ims(
    session: Annotated["AsyncSession", Depends(db_session.get_session)], ims_id: int
):
    await ims_service.delete(session, ims_id)


@router.post("/reports/top")
async def get_top(
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
    filters: IMSTopFilter,
    current_user: Annotated["User", Depends(current_active_user)],
):
    return await ims_service.get_entities_with_metrics(
        session, filters, company_id=current_user.company_id
    )


@router.post("/reports/table", dependencies=[Depends(current_active_user)])
async def get_market_data(
    filters: IMSTableFilter,
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
):
    return await ims_service.get_table_data(session, filters)
