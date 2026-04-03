from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

from src.api.dependencies.current_user import current_operator_user
from src.api.dependencies.excel_file import ExcelFile
from src.db.models import Doctor, GlobalDoctor, MedicalFacility, Pharmacy, User
from src.db.session import db_session
from src.schemas import client
from src.schemas.base_filter import PaginatedResponse
from src.schemas.export import ExportExcelRequest
from src.services import client as client_service

router = APIRouter()


@router.post(
    "/client-categories",
    response_model=PaginatedResponse[client.ClientCategoryResponse],
    dependencies=[Depends(current_operator_user)],
)
async def get_client_categories(
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
    filters: client.ClientCategoryListRequest,
):
    return await client_service.client_category_service.get_multi(
        session, filters=filters
    )


@router.post("/client-categories/export-excel")
async def export_client_categories_excel(
    payload: ExportExcelRequest,
    current_user: Annotated[User, Depends(current_operator_user)],
):
    from src.tasks.export_excel import create_export_task_record, export_excel_task

    task = export_excel_task.delay(
        user_id=current_user.id,
        file_name=payload.file_name,
        service_path="src.services.client.client_category.ClientCategoryService",
        model_path="src.db.models.clients.ClientCategory",
        serializer_path="src.schemas.client.ClientCategoryResponse",
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
    "/client-categories/create",
    response_model=client.ClientCategoryResponse,
    dependencies=[Depends(current_operator_user)],
)
async def create_client_category(
    client_category: client.ClientCategoryCreate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await client_service.client_category_service.create(session, client_category)


@router.post("/client-categories/import-excel")
async def bulk_insert_client_categories(
    file: ExcelFile,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_operator_user)],
):
    result = await client_service.client_category_service.import_excel(
        session, file, user_id=current_user.id
    )
    return result


@router.get(
    "/client-categories/{client_category_id}",
    response_model=client.ClientCategoryResponse,
    dependencies=[Depends(current_operator_user)],
)
async def get_client_category(
    client_category_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await client_service.client_category_service.get_or_404(
        session, client_category_id
    )


@router.patch(
    "/client-categories/{client_category_id}",
    response_model=client.ClientCategoryResponse,
    dependencies=[Depends(current_operator_user)],
)
async def update_client_category(
    client_category_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    client_category: client.ClientCategoryUpdate,
):
    return await client_service.client_category_service.update(
        session, client_category_id, client_category
    )


@router.delete(
    "/client-categories/{client_category_id}",
    dependencies=[Depends(current_operator_user)],
)
async def delete_client_category(
    client_category_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await client_service.client_category_service.delete(
        session, client_category_id
    )


@router.post(
    "/doctors",
    response_model=PaginatedResponse[client.DoctorResponse],
    dependencies=[Depends(current_operator_user)],
)
async def get_doctors(
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
    filters: client.DoctorListRequest,
):
    if filters.mode == "global":
        load_options = [
            selectinload(GlobalDoctor.medical_facility),
        ]
    else:
        load_options = [
            selectinload(Doctor.global_doctor).selectinload(
                GlobalDoctor.medical_facility
            ),
            selectinload(Doctor.speciality),
            selectinload(Doctor.responsible_employee),
            selectinload(Doctor.client_category),
            selectinload(Doctor.product_group),
            selectinload(Doctor.company),
        ]
    return await client_service.doctor_service.get_multi(
        session, load_options=load_options, filters=filters
    )


@router.post("/doctors/export-excel")
async def export_doctors_excel(
    payload: ExportExcelRequest,
    current_user: Annotated[User, Depends(current_operator_user)],
):
    from src.tasks.export_excel import create_export_task_record, export_excel_task

    task = export_excel_task.delay(
        user_id=current_user.id,
        file_name=payload.file_name,
        service_path="src.services.client.doctor.DoctorService",
        model_path="src.db.models.clients.Doctor",
        serializer_path="src.schemas.client.DoctorResponse",
        load_options_paths=[
            "global_doctor",
            "global_doctor.medical_facility",
            "speciality",
            "responsible_employee",
            "client_category",
            "product_group",
            "company",
        ],
        header_map=payload.header_map,
        fields_map=payload.fields_map,
        boolean_map=payload.boolean_map,
        custom_map={"mode": {"company": "компания", "global": "общий"}},
    )

    await create_export_task_record(
        task_id=task.id,
        started_by=current_user.id,
        file_path="",
    )

    return {"task_id": task.id}


@router.post(
    "/doctors/create",
    response_model=client.DoctorResponse,
    dependencies=[Depends(current_operator_user)],
)
async def create_doctor(
    doctor: client.DoctorCreate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    if doctor.mode == "global":
        load_options = [
            joinedload(GlobalDoctor.medical_facility),
        ]
    else:
        load_options = [
            joinedload(Doctor.responsible_employee),
            joinedload(Doctor.client_category),
            joinedload(Doctor.product_group),
            joinedload(Doctor.company),
            joinedload(Doctor.global_doctor).joinedload(GlobalDoctor.medical_facility),
            joinedload(Doctor.speciality),
        ]
    return await client_service.doctor_service.create(
        session, doctor, load_options=load_options
    )


@router.post("/doctors/import-excel")
async def bulk_insert_doctors(
    file: ExcelFile,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_operator_user)],
):
    result = await client_service.doctor_service.import_excel(
        session, file, user_id=current_user.id
    )
    return result


@router.get(
    "/doctors/{doctor_id}",
    response_model=client.DoctorResponse,
    dependencies=[Depends(current_operator_user)],
)
async def get_doctor(
    doctor_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [
        joinedload(Doctor.responsible_employee),
        joinedload(Doctor.client_category),
        joinedload(Doctor.product_group),
        joinedload(Doctor.company),
        joinedload(Doctor.global_doctor).joinedload(GlobalDoctor.medical_facility),
        joinedload(Doctor.speciality),
    ]
    return await client_service.doctor_service.get_or_404(
        session, doctor_id, load_options=load_options
    )


@router.patch(
    "/doctors/{doctor_id}",
    response_model=None,
    dependencies=[Depends(current_operator_user)],
)
async def update_doctor(
    doctor_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    doctor: client.DoctorUpdate,
):
    load_options = [
        joinedload(Doctor.responsible_employee),
        joinedload(Doctor.client_category),
        joinedload(Doctor.product_group),
        joinedload(Doctor.company),
        joinedload(Doctor.global_doctor).joinedload(GlobalDoctor.medical_facility),
        joinedload(Doctor.speciality),
    ]
    return await client_service.doctor_service.update(
        session, doctor_id, doctor, load_options=load_options
    )


@router.delete("/doctors/{doctor_id}", dependencies=[Depends(current_operator_user)])
async def delete_doctor(
    doctor_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await client_service.doctor_service.delete(session, doctor_id)


@router.post(
    "/pharmacies",
    response_model=PaginatedResponse[client.PharmacyResponse],
    dependencies=[Depends(current_operator_user)],
)
async def get_pharmacies(
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
    filters: client.PharmacyListRequest,
):
    load_options = [
        selectinload(Pharmacy.distributor),
        selectinload(Pharmacy.responsible_employee),
        selectinload(Pharmacy.settlement),
        selectinload(Pharmacy.district),
        selectinload(Pharmacy.client_category),
        selectinload(Pharmacy.product_group),
        selectinload(Pharmacy.company),
        selectinload(Pharmacy.geo_indicator),
    ]
    return await client_service.pharmacy_service.get_multi(
        session, load_options=load_options, filters=filters
    )


@router.post("/pharmacies/export-excel")
async def export_pharmacies_excel(
    payload: ExportExcelRequest,
    current_user: Annotated[User, Depends(current_operator_user)],
):
    from src.tasks.export_excel import create_export_task_record, export_excel_task

    task = export_excel_task.delay(
        user_id=current_user.id,
        file_name=payload.file_name,
        service_path="src.services.client.pharmacy.PharmacyService",
        model_path="src.db.models.clients.Pharmacy",
        serializer_path="src.schemas.client.PharmacyResponse",
        load_options_paths=[
            "distributor",
            "responsible_employee",
            "settlement",
            "district",
            "client_category",
            "product_group",
            "company",
            "geo_indicator",
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
    "/pharmacies/create",
    response_model=client.PharmacyResponse,
    dependencies=[Depends(current_operator_user)],
)
async def create_pharmacy(
    pharmacy: client.PharmacyCreate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [
        joinedload(Pharmacy.distributor),
        joinedload(Pharmacy.responsible_employee),
        joinedload(Pharmacy.settlement),
        joinedload(Pharmacy.district),
        joinedload(Pharmacy.client_category),
        joinedload(Pharmacy.product_group),
        joinedload(Pharmacy.company),
        joinedload(Pharmacy.geo_indicator),
    ]
    return await client_service.pharmacy_service.create(
        session, pharmacy, load_options=load_options
    )


@router.post("/pharmacies/import-excel")
async def bulk_insert_pharmacies(
    file: ExcelFile,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_operator_user)],
):
    result = await client_service.pharmacy_service.import_excel(
        session, file, user_id=current_user.id
    )
    return result


@router.get(
    "/pharmacies/{pharmacy_id}",
    response_model=client.PharmacyResponse,
    dependencies=[Depends(current_operator_user)],
)
async def get_pharmacy(
    pharmacy_id: int, session: Annotated[AsyncSession, Depends(db_session.get_session)]
):
    load_options = [
        joinedload(Pharmacy.distributor),
        joinedload(Pharmacy.responsible_employee),
        joinedload(Pharmacy.settlement),
        joinedload(Pharmacy.district),
        joinedload(Pharmacy.client_category),
        joinedload(Pharmacy.product_group),
        joinedload(Pharmacy.company),
        joinedload(Pharmacy.geo_indicator),
    ]
    return await client_service.pharmacy_service.get_or_404(
        session, pharmacy_id, load_options=load_options
    )


@router.patch(
    "/pharmacies/{pharmacy_id}",
    response_model=client.PharmacyResponse,
    dependencies=[Depends(current_operator_user)],
)
async def update_pharmacy(
    pharmacy_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    pharmacy: client.PharmacyUpdate,
):
    load_options = [
        joinedload(Pharmacy.distributor),
        joinedload(Pharmacy.responsible_employee),
        joinedload(Pharmacy.settlement),
        joinedload(Pharmacy.district),
        joinedload(Pharmacy.client_category),
        joinedload(Pharmacy.product_group),
        joinedload(Pharmacy.company),
        joinedload(Pharmacy.geo_indicator),
    ]
    return await client_service.pharmacy_service.update(
        session, pharmacy_id, pharmacy, load_options=load_options
    )


@router.delete(
    "/pharmacies/{pharmacy_id}", dependencies=[Depends(current_operator_user)]
)
async def delete_pharmacy(
    pharmacy_id: int, session: Annotated[AsyncSession, Depends(db_session.get_session)]
):
    return await client_service.pharmacy_service.delete(session, pharmacy_id)


@router.post(
    "/specialities",
    response_model=PaginatedResponse[client.SpecialityResponse],
    dependencies=[Depends(current_operator_user)],
)
async def get_specialities(
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
    filters: client.SpecialityListRequest,
):
    return await client_service.speciality_service.get_multi(session, filters=filters)


@router.post("/specialities/export-excel")
async def export_specialities_excel(
    payload: ExportExcelRequest,
    current_user: Annotated[User, Depends(current_operator_user)],
):
    from src.tasks.export_excel import create_export_task_record, export_excel_task

    task = export_excel_task.delay(
        user_id=current_user.id,
        file_name=payload.file_name,
        service_path="src.services.client.speciality.SpecialityService",
        model_path="src.db.models.clients.Speciality",
        serializer_path="src.schemas.client.SpecialityResponse",
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


@router.post("/specialities/import-excel")
async def bulk_insert_specialities(
    file: ExcelFile,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_operator_user)],
):
    result = await client_service.speciality_service.import_excel(
        session, file, user_id=current_user.id
    )
    return result


@router.post(
    "/specialities/create",
    response_model=client.SpecialityResponse,
    dependencies=[Depends(current_operator_user)],
)
async def create_speciality(
    speciality: client.SpecialityCreate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await client_service.speciality_service.create(session, speciality)


@router.get(
    "/specialities/{speciality_id}",
    response_model=client.SpecialityResponse,
    dependencies=[Depends(current_operator_user)],
)
async def get_speciality(
    speciality_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await client_service.speciality_service.get_or_404(session, speciality_id)


@router.patch(
    "/specialities/{speciality_id}",
    response_model=client.SpecialityResponse,
    dependencies=[Depends(current_operator_user)],
)
async def update_speciality(
    speciality_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    speciality: client.SpecialityUpdate,
):
    return await client_service.speciality_service.update(
        session, speciality_id, speciality
    )


@router.delete(
    "/specialities/{speciality_id}", dependencies=[Depends(current_operator_user)]
)
async def delete_speciality(
    speciality_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await client_service.speciality_service.delete(session, speciality_id)


@router.post(
    "/medical-facilities",
    response_model=PaginatedResponse[client.MedicalFacilityResponse],
    dependencies=[Depends(current_operator_user)],
)
async def get_medical_facilities(
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
    filters: client.MedicalFacilityListRequest,
):
    load_options = [
        joinedload(MedicalFacility.settlement),
        joinedload(MedicalFacility.district),
        joinedload(MedicalFacility.geo_indicator),
    ]
    return await client_service.medical_facility_service.get_multi(
        session, load_options=load_options, filters=filters
    )


@router.post("/medical-facilities/export-excel")
async def export_medical_facilities_excel(
    payload: ExportExcelRequest,
    current_user: Annotated[User, Depends(current_operator_user)],
):
    from src.tasks.export_excel import create_export_task_record, export_excel_task

    task = export_excel_task.delay(
        user_id=current_user.id,
        file_name=payload.file_name,
        service_path="src.services.client.medical_facility.MedicalFacilityService",
        model_path="src.db.models.clients.MedicalFacility",
        serializer_path="src.schemas.client.MedicalFacilityResponse",
        load_options_paths=["settlement", "district", "geo_indicator"],
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
    "/medical-facilities/create",
    response_model=client.MedicalFacilityResponse,
    dependencies=[Depends(current_operator_user)],
)
async def create_medical_facility(
    medical_facility: client.MedicalFacilityCreate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [
        joinedload(MedicalFacility.settlement),
        joinedload(MedicalFacility.district),
        joinedload(MedicalFacility.geo_indicator),
    ]
    return await client_service.medical_facility_service.create(
        session, medical_facility, load_options=load_options
    )


@router.post("/medical-facilities/import-excel")
async def bulk_insert_medical_facilities(
    file: ExcelFile,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_operator_user)],
):
    result = await client_service.medical_facility_service.import_excel(
        session, file, user_id=current_user.id
    )
    return result


@router.get(
    "/medical-facilities/{medical_facility_id}",
    response_model=client.MedicalFacilityResponse,
    dependencies=[Depends(current_operator_user)],
)
async def get_medical_facility(
    medical_facility_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [
        joinedload(MedicalFacility.settlement),
        joinedload(MedicalFacility.district),
        joinedload(MedicalFacility.geo_indicator),
    ]
    return await client_service.medical_facility_service.get_or_404(
        session, medical_facility_id, load_options=load_options
    )


@router.patch(
    "/medical-facilities/{medical_facility_id}",
    response_model=client.MedicalFacilityResponse,
    dependencies=[Depends(current_operator_user)],
)
async def update_medical_facility(
    medical_facility_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    medical_facility: client.MedicalFacilityUpdate,
):
    load_options = [
        joinedload(MedicalFacility.settlement),
        joinedload(MedicalFacility.district),
        joinedload(MedicalFacility.geo_indicator),
    ]
    return await client_service.medical_facility_service.update(
        session, medical_facility_id, medical_facility, load_options=load_options
    )


@router.delete(
    "/medical-facilities/{medical_facility_id}",
    dependencies=[Depends(current_operator_user)],
)
async def delete_medical_facility(
    medical_facility_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await client_service.medical_facility_service.delete(
        session, medical_facility_id
    )


@router.post(
    "/distributors",
    response_model=PaginatedResponse[client.DistributorResponse],
    dependencies=[Depends(current_operator_user)],
)
async def get_distributors(
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
    filters: client.DistributorListRequest,
):
    return await client_service.distributor_service.get_multi(session, filters=filters)


@router.post("/distributors/export-excel")
async def export_distributors_excel(
    payload: ExportExcelRequest,
    current_user: Annotated[User, Depends(current_operator_user)],
):
    from src.tasks.export_excel import create_export_task_record, export_excel_task

    task = export_excel_task.delay(
        user_id=current_user.id,
        file_name=payload.file_name,
        service_path="src.services.client.distributor.DistributorService",
        model_path="src.db.models.clients.Distributor",
        serializer_path="src.schemas.client.DistributorResponse",
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
    "/distributors/create",
    response_model=client.DistributorResponse,
    dependencies=[Depends(current_operator_user)],
)
async def create_distributor(
    distributor: client.DistributorCreate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await client_service.distributor_service.create(session, distributor)


@router.post("/distributors/import-excel")
async def bulk_insert_distributors(
    file: ExcelFile,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_operator_user)],
):
    result = await client_service.distributor_service.import_excel(
        session, file, user_id=current_user.id
    )
    return result


@router.get(
    "/distributors/{distributor_id}",
    response_model=client.DistributorResponse,
    dependencies=[Depends(current_operator_user)],
)
async def get_distributor(
    distributor_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await client_service.distributor_service.get_or_404(session, distributor_id)


@router.patch(
    "/distributors/{distributor_id}",
    response_model=client.DistributorResponse,
    dependencies=[Depends(current_operator_user)],
)
async def update_distributor(
    distributor_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    distributor: client.DistributorUpdate,
):
    return await client_service.distributor_service.update(
        session, distributor_id, distributor
    )


@router.delete(
    "/distributors/{distributor_id}", dependencies=[Depends(current_operator_user)]
)
async def delete_distributor(
    distributor_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await client_service.distributor_service.delete(session, distributor_id)


@router.post(
    "/geo-indicators",
    response_model=PaginatedResponse[client.GeoIndicatorResponse],
    dependencies=[Depends(current_operator_user)],
)
async def get_geo_indicators(
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
    filters: client.GeoIndicatorListRequest,
):
    return await client_service.geo_indicator_service.get_multi(
        session, filters=filters
    )


@router.post("/geo-indicators/export-excel")
async def export_geo_indicators_excel(
    payload: ExportExcelRequest,
    current_user: Annotated[User, Depends(current_operator_user)],
):
    from src.tasks.export_excel import create_export_task_record, export_excel_task

    task = export_excel_task.delay(
        user_id=current_user.id,
        file_name=payload.file_name,
        service_path="src.services.client.geo_indicator.GeoIndicatorService",
        model_path="src.db.models.GeoIndicator",
        serializer_path="src.schemas.client.GeoIndicatorResponse",
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
    "/geo-indicators/create",
    response_model=client.GeoIndicatorResponse,
    dependencies=[Depends(current_operator_user)],
)
async def create_geo_indicator(
    geo_indicator: client.GeoIndicatorCreate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await client_service.geo_indicator_service.create(session, geo_indicator)


@router.post("/geo-indicators/import-excel")
async def bulk_insert_geo_indicators(
    file: ExcelFile,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_operator_user)],
):
    result = await client_service.geo_indicator_service.import_excel(
        session, file, user_id=current_user.id
    )
    return result


@router.get(
    "/geo-indicators/{geo_indicator_id}",
    response_model=client.GeoIndicatorResponse,
    dependencies=[Depends(current_operator_user)],
)
async def get_geo_indicator(
    geo_indicator_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await client_service.geo_indicator_service.get_or_404(
        session, geo_indicator_id
    )


@router.patch(
    "/geo-indicators/{geo_indicator_id}",
    response_model=client.GeoIndicatorResponse,
    dependencies=[Depends(current_operator_user)],
)
async def update_geo_indicator(
    geo_indicator_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    geo_indicator: client.GeoIndicatorUpdate,
):
    return await client_service.geo_indicator_service.update(
        session, geo_indicator_id, geo_indicator
    )


@router.delete(
    "/geo-indicators/{geo_indicator_id}", dependencies=[Depends(current_operator_user)]
)
async def delete_geo_indicator(
    geo_indicator_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await client_service.geo_indicator_service.delete(session, geo_indicator_id)
