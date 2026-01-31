from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.session import db_session
from src.schemas import company, base_filter
from src.services.company import company_service, registration_application_service
from src.api.dependencies.current_user import current_admin_user, current_admin_or_operator_user


router = APIRouter()


@router.get('', response_model=list[company.CompanyResponse], dependencies=[Depends(current_admin_or_operator_user)])
async def get_companies(
        session: Annotated['AsyncSession', Depends(db_session.get_session)],
        filters: Annotated[base_filter.BaseFilter, Query()]
):
    return await company_service.get_multi(session, filters=filters)


@router.post('', response_model=company.CompanyResponse, dependencies=[Depends(current_admin_user)])
async def create_company(
    new_company: company.CompanyCreate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await company_service.create(session, new_company)


@router.get('/{company_id}', response_model=company.CompanyResponse, dependencies=[Depends(current_admin_or_operator_user)])
async def get_company(
    company_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await company_service.get_or_404(session, company_id)


@router.patch('/{company_id}', response_model=company.CompanyResponse, dependencies=[Depends(current_admin_user)])
async def update_company(
    company_id: int,
    updated_company: company.CompanyUpdate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await company_service.update(session, company_id, updated_company)


@router.delete('/{company_id}', dependencies=[Depends(current_admin_user)])
async def delete_company(
    company_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await company_service.delete(session, company_id)


@router.post('/registration-application')
async def create_registration_application(
    application: company.RegistrationApplicationCreate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)]
):
    return await registration_application_service.create(session, application)


@router.get('/registration-application/{registration_application_id}')
async def get_registration_application(
    registration_application_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)]
):
    return await registration_application_service.get_or_404(session, registration_application_id)
