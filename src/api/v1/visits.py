from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from src.api.dependencies.company import can_view_visits
from src.api.dependencies.current_user import current_active_user, current_operator_user
from src.db.models import MedicalFacility, Pharmacy, User, Visit
from src.db.session import db_session
from src.schemas import base_filter
from src.schemas.visit import (
    DoctorsBySpecialtyResponse,
    DoctorsCountFilter,
    DoctorsCountWithVisitFilter,
    VisitCountFilter,
    VisitCreate,
    VisitResponse,
    VisitSumForPeriodFilter,
    VisitUpdate,
)
from src.services.visit import visit_service

router = APIRouter()


@router.post(
    "",
    response_model=list[VisitResponse],
    dependencies=[Depends(current_operator_user)],
)
async def get_visits(
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    filters: base_filter.BaseFilter,
):
    load_options = [
        joinedload(Visit.pharmacy).joinedload(Pharmacy.geo_indicator),
        joinedload(Visit.pharmacy).joinedload(Pharmacy.distributor),
        joinedload(Visit.doctor),
        joinedload(Visit.product_group),
        joinedload(Visit.employee),
        joinedload(Visit.medical_facility).joinedload(MedicalFacility.geo_indicator),
    ]
    return await visit_service.get_multi(
        session, load_options=load_options, filters=filters
    )


@router.post(
    "/create",
    response_model=VisitResponse,
    dependencies=[Depends(current_operator_user)],
)
async def create_visit(
    new_visit: VisitCreate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [
        joinedload(Visit.pharmacy).joinedload(Pharmacy.geo_indicator),
        joinedload(Visit.pharmacy).joinedload(Pharmacy.distributor),
        joinedload(Visit.doctor),
        joinedload(Visit.product_group),
        joinedload(Visit.employee),
        joinedload(Visit.medical_facility).joinedload(MedicalFacility.geo_indicator),
    ]
    return await visit_service.create(session, new_visit, load_options=load_options)


@router.get(
    "/{visit_id}",
    response_model=VisitResponse,
    dependencies=[Depends(current_operator_user)],
)
async def get_visit(
    visit_id: int, session: Annotated[AsyncSession, Depends(db_session.get_session)]
):
    load_options = [
        joinedload(Visit.pharmacy).joinedload(Pharmacy.geo_indicator),
        joinedload(Visit.pharmacy).joinedload(Pharmacy.distributor),
        joinedload(Visit.doctor),
        joinedload(Visit.product_group),
        joinedload(Visit.employee),
        joinedload(Visit.medical_facility).joinedload(MedicalFacility.geo_indicator),
    ]
    return await visit_service.get(session, visit_id, load_options=load_options)


@router.patch(
    "/{visit_id}",
    response_model=VisitResponse,
    dependencies=[Depends(current_operator_user)],
)
async def update_visit(
    visit_id: int,
    visit_update: VisitUpdate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [
        joinedload(Visit.pharmacy).joinedload(Pharmacy.geo_indicator),
        joinedload(Visit.pharmacy).joinedload(Pharmacy.distributor),
        joinedload(Visit.doctor),
        joinedload(Visit.product_group),
        joinedload(Visit.employee),
        joinedload(Visit.medical_facility).joinedload(MedicalFacility.geo_indicator),
    ]
    return await visit_service.update(
        session, visit_id, visit_update, load_options=load_options
    )


@router.delete("/{visit_id}", dependencies=[Depends(current_operator_user)])
async def delete_visit(
    visit_id: int, session: Annotated[AsyncSession, Depends(db_session.get_session)]
):
    return await visit_service.delete(session, visit_id)


@router.post("/import-excel", dependencies=[Depends(current_operator_user)])
async def bulk_insert_visits(
    file: UploadFile,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_active_user)],
):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Импорт только для эксель файлов",
        )
    result = await visit_service.import_sales(session, file, user_id=current_user.id)
    return result


@router.post(
    "/reports/doctors-by-specialty",
    response_model=list[DoctorsBySpecialtyResponse],
    dependencies=[Depends(can_view_visits)],
)
async def get_doctors_by_specialty(
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    filters: DoctorsCountFilter,
    current_user: Annotated[User, Depends(current_active_user)],
):
    return await visit_service.get_doctor_count_by_speciality(
        session, filters, company_id=current_user.company_id
    )


@router.post(
    "/reports/doctors-with-visits-by-specialty", dependencies=[Depends(can_view_visits)]
)
async def get_doctors_with_visits_by_specialty(
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    filters: DoctorsCountWithVisitFilter,
    current_user: Annotated[User, Depends(current_active_user)],
):
    return await visit_service.get_doctor_count_with_visits_by_speciality(
        session, filters, company_id=current_user.company_id
    )


@router.post("/reports/visits-sum-for-period", dependencies=[Depends(can_view_visits)])
async def get_visits_summary(
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    filters: VisitSumForPeriodFilter,
    current_user: Annotated[User, Depends(current_active_user)],
):
    return await visit_service.get_visits_sum_for_period(
        session, filters, company_id=current_user.company_id
    )


@router.get("/reports/visits-by-period", dependencies=[Depends(can_view_visits)])
async def get_visit_by_period(
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    filters: Annotated[VisitCountFilter, Query()],
    current_user: Annotated[User, Depends(current_active_user)],
):
    return await visit_service.get_visits_by_period(
        session, filters, company_id=current_user.company_id
    )
