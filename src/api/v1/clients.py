from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, UploadFile, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from src.api.dependencies.current_user import current_operator_user
from src.api.utils.export_excel import export_excel_response
from src.db.models import Doctor, MedicalFacility, Pharmacy, User
from src.db.session import db_session
from src.schemas import client
from src.schemas.export import ExportExcelRequest
from src.services import client as client_service

router = APIRouter()


@router.post(
    "/client-categories",
    response_model=list[client.ClientCategoryResponse],
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
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
):
    return await export_excel_response(
        payload=payload,
        get_rows=lambda: client_service.client_category_service.get_multi(session),
        serialize=lambda cc: client.ClientCategoryResponse.model_validate(
            cc
        ).model_dump(),
    )


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
    file: UploadFile,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_operator_user)],
):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only Excel files are allowed",
        )
    await client_service.client_category_service.import_excel(
        session, file, user_id=current_user.id
    )


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
    response_model=list[client.DoctorResponse],
    dependencies=[Depends(current_operator_user)],
)
async def get_doctors(
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
    filters: client.DoctorListRequest,
):
    load_options = [
        joinedload(Doctor.responsible_employee),
        joinedload(Doctor.medical_facility),
        joinedload(Doctor.speciality),
        joinedload(Doctor.client_category),
        joinedload(Doctor.product_group),
    ]
    return await client_service.doctor_service.get_multi(
        session, load_options=load_options, filters=filters
    )


@router.post("/doctors/export-excel")
async def export_doctors_excel(
    payload: ExportExcelRequest,
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
):
    load_options = [
        joinedload(Doctor.responsible_employee),
        joinedload(Doctor.medical_facility),
        joinedload(Doctor.speciality),
        joinedload(Doctor.client_category),
        joinedload(Doctor.product_group),
    ]
    return await export_excel_response(
        payload=payload,
        get_rows=lambda: client_service.doctor_service.get_multi(
            session, load_options=load_options
        ),
        serialize=lambda d: client.DoctorResponse.model_validate(d).model_dump(),
    )


@router.post(
    "/doctors/create",
    response_model=client.DoctorResponse,
    dependencies=[Depends(current_operator_user)],
)
async def create_doctor(
    doctor: client.DoctorCreate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [
        joinedload(Doctor.responsible_employee),
        joinedload(Doctor.medical_facility),
        joinedload(Doctor.speciality),
        joinedload(Doctor.client_category),
        joinedload(Doctor.product_group),
    ]
    return await client_service.doctor_service.create(
        session, doctor, load_options=load_options
    )


@router.post("/doctors/import-excel")
async def bulk_insert_doctors(
    file: UploadFile,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_operator_user)],
):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only Excel files are allowed",
        )
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
    doctor_id: int, session: Annotated[AsyncSession, Depends(db_session.get_session)]
):
    load_options = [
        joinedload(Doctor.responsible_employee),
        joinedload(Doctor.medical_facility),
        joinedload(Doctor.speciality),
        joinedload(Doctor.client_category),
        joinedload(Doctor.product_group),
    ]
    return await client_service.doctor_service.get_or_404(
        session, doctor_id, load_options=load_options
    )


@router.patch(
    "/doctors/{doctor_id}",
    response_model=client.DoctorResponse,
    dependencies=[Depends(current_operator_user)],
)
async def update_doctor(
    doctor_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    doctor: client.DoctorUpdate,
):
    load_options = [
        joinedload(Doctor.responsible_employee),
        joinedload(Doctor.medical_facility),
        joinedload(Doctor.speciality),
        joinedload(Doctor.client_category),
        joinedload(Doctor.product_group),
    ]
    return await client_service.doctor_service.update(
        session, doctor_id, doctor, load_options=load_options
    )


@router.delete("/doctors/{doctor_id}", dependencies=[Depends(current_operator_user)])
async def delete_doctor(
    doctor_id: int, session: Annotated[AsyncSession, Depends(db_session.get_session)]
):
    return await client_service.doctor_service.delete(session, doctor_id)


@router.post(
    "/pharmacies",
    response_model=list[client.PharmacyResponse],
    dependencies=[Depends(current_operator_user)],
)
async def get_pharmacies(
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
    filters: client.PharmacyListRequest,
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
    return await client_service.pharmacy_service.get_multi(
        session, load_options=load_options, filters=filters
    )


@router.post("/pharmacies/export-excel")
async def export_pharmacies_excel(
    payload: ExportExcelRequest,
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
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

    return await export_excel_response(
        payload=payload,
        get_rows=lambda: client_service.pharmacy_service.get_multi(
            session, load_options=load_options
        ),
        serialize=lambda p: client.PharmacyResponse.model_validate(p).model_dump(),
    )


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
    file: UploadFile,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_operator_user)],
):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only Excel files are allowed",
        )
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
    response_model=list[client.SpecialityResponse],
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
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
):
    return await export_excel_response(
        payload=payload,
        get_rows=lambda: client_service.speciality_service.get_multi(session),
        serialize=lambda s: client.SpecialityResponse.model_validate(s).model_dump(),
    )


@router.post("/specialities/import-excel")
async def bulk_insert_specialities(
    file: UploadFile,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_operator_user)],
):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only Excel files are allowed",
        )
    await client_service.speciality_service.import_excel(
        session, file, user_id=current_user.id
    )


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
    response_model=list[client.MedicalFacilityResponse],
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
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
):
    load_options = [
        joinedload(MedicalFacility.settlement),
        joinedload(MedicalFacility.district),
        joinedload(MedicalFacility.geo_indicator),
    ]
    return await export_excel_response(
        payload=payload,
        get_rows=lambda: client_service.medical_facility_service.get_multi(
            session, load_options=load_options
        ),
        serialize=lambda mf: client.MedicalFacilityResponse.model_validate(
            mf
        ).model_dump(),
    )


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
    file: UploadFile,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_operator_user)],
):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only Excel files are allowed",
        )
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
    response_model=list[client.DistributorResponse],
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
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
):
    return await export_excel_response(
        payload=payload,
        get_rows=lambda: client_service.distributor_service.get_multi(session),
        serialize=lambda d: client.DistributorResponse.model_validate(d).model_dump(),
    )


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
    file: UploadFile,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_operator_user)],
):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only Excel files are allowed",
        )
    await client_service.distributor_service.import_excel(
        session, file, user_id=current_user.id
    )


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
    response_model=list[client.GeoIndicatorResponse],
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
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
):
    return await export_excel_response(
        payload=payload,
        get_rows=lambda: client_service.geo_indicator_service.get_multi(session),
        serialize=lambda gi: client.GeoIndicatorResponse.model_validate(
            gi
        ).model_dump(),
    )


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
    file: UploadFile,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_operator_user)],
):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only Excel files are allowed",
        )
    await client_service.geo_indicator_service.import_excel(
        session, file, user_id=current_user.id
    )


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
