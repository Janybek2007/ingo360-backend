from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.dependencies.current_user import (
    current_admin_or_operator_user,
    current_admin_user,
)
from src.db.models import User
from src.db.session import db_session
from src.schemas import company, search_filter
from src.schemas.export import ExportExcelRequest
from src.services.company import company_service, registration_application_service

router = APIRouter()


@router.get(
    "",
    response_model=list[company.CompanyResponse],
    dependencies=[Depends(current_admin_or_operator_user)],
)
async def get_companies(
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
    filters: Annotated[search_filter.SearchFilter, Query()],
):
    return await company_service.get_multi(session, filters=filters)


@router.post("/export-excel")
async def export_companies_excel(
    payload: ExportExcelRequest,
    current_user: Annotated["User", Depends(current_admin_or_operator_user)],
):
    from src.tasks.export_excel import create_export_task_record, export_excel_task

    task = export_excel_task.delay(
        user_id=current_user.id,
        file_name=payload.file_name,
        service_path="src.services.company.CompanyService",
        model_path="src.db.models.Company",
        serializer_path="src.schemas.company.CompanyFormattedResponse",
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
    "",
    response_model=company.CompanyResponse,
    dependencies=[Depends(current_admin_user)],
)
async def create_company(
    new_company: company.CompanyCreate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await company_service.create(session, new_company)


@router.get(
    "/{company_id}",
    response_model=company.CompanyResponse,
    dependencies=[Depends(current_admin_or_operator_user)],
)
async def get_company(
    company_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await company_service.get_or_404(session, company_id)


@router.patch(
    "/{company_id}",
    response_model=company.CompanyResponse,
    dependencies=[Depends(current_admin_user)],
)
async def update_company(
    company_id: int,
    updated_company: company.CompanyUpdate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await company_service.update(session, company_id, updated_company)


@router.delete("/{company_id}", dependencies=[Depends(current_admin_user)])
async def delete_company(
    company_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await company_service.delete(session, company_id)


@router.post("/registration-application")
async def create_registration_application(
    application: company.RegistrationApplicationCreate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await registration_application_service.create(session, application)


@router.get("/registration-application/{registration_application_id}")
async def get_registration_application(
    registration_application_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await registration_application_service.get_or_404(
        session, registration_application_id
    )
