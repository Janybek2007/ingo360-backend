from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from src.api.dependencies.company import can_view_visits
from src.api.dependencies.current_user import current_active_user, current_operator_user
from src.api.dependencies.excel_file import ExcelFile
from src.db.models import Doctor, MedicalFacility, Pharmacy, User, Visit
from src.db.session import db_session
from src.schemas.base_filter import PaginatedResponse
from src.schemas.export import ExportExcelRequest
from src.schemas.visit import (
    DoctorsBySpecialtyResponse,
    DoctorsCountFilter,
    DoctorsCountWithVisitFilter,
    VisitCountFilter,
    VisitCreate,
    VisitResponse,
    VisitsRequest,
    VisitSumForPeriodFilter,
    VisitUpdate,
)
from src.services.visit import visit_service

router = APIRouter()


@router.post(
    "",
    response_model=PaginatedResponse[VisitResponse],
    dependencies=[Depends(current_operator_user)],
)
async def get_visits(
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    filters: VisitsRequest,
):
    load_options = [
        joinedload(Visit.pharmacy).joinedload(Pharmacy.geo_indicator),
        joinedload(Visit.pharmacy).joinedload(Pharmacy.distributor),
        joinedload(Visit.doctor).joinedload(Doctor.global_doctor),
        joinedload(Visit.product_group),
        joinedload(Visit.employee),
        joinedload(Visit.medical_facility).joinedload(MedicalFacility.geo_indicator),
    ]
    return await visit_service.get_multi(
        session, load_options=load_options, filters=filters
    )


@router.post("/export-excel")
async def export_visits_excel(
    payload: ExportExcelRequest,
    current_user: Annotated[User, Depends(current_active_user)],
):
    from src.tasks.export_excel import create_export_task_record, export_excel_task

    task = export_excel_task.delay(
        user_id=current_user.id,
        file_name=payload.file_name,
        service_path="src.services.visit.VisitService",
        model_path="src.db.models.Visit",
        serializer_path="src.schemas.visit.VisitResponse",
        load_options_paths=[
            "pharmacy.geo_indicator",
            "pharmacy.distributor",
            "doctor",
            "product_group",
            "employee",
            "medical_facility.geo_indicator",
        ],
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
    file: ExcelFile,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_active_user)],
):
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


@router.post("/reports/visits-by-period", dependencies=[Depends(can_view_visits)])
async def get_visit_by_period(
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    filters: VisitCountFilter,
    current_user: Annotated[User, Depends(current_active_user)],
):
    return await visit_service.get_visits_by_period(
        session, filters, company_id=current_user.company_id
    )
