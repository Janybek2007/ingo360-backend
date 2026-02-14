from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, UploadFile, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from src.api.dependencies.current_user import current_active_user, current_operator_user
from src.db.models import District, Region, Settlement, User
from src.db.session import db_session
from src.schemas import geography
from src.schemas.export import ExportExcelRequest
from src.services import geography as geography_service

router = APIRouter(dependencies=[Depends(current_operator_user)])


@router.post("/countries/create", response_model=geography.CountryResponse)
async def create_country(
    country: geography.CountryCreate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await geography_service.country_service.create(session, country)


@router.post("/countries/import-excel")
async def bulk_insert_countries(
    file: UploadFile,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_active_user)],
):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only Excel files are allowed",
        )
    await geography_service.country_service.import_excel(
        session, file, user_id=current_user.id
    )


@router.post("/countries", response_model=list[geography.CountryResponse])
async def get_countries(
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    filters: geography.CountryListRequest,
):
    return await geography_service.country_service.get_multi(session, filters=filters)


@router.post("/countries/export-excel")
async def export_countries_excel(
    payload: ExportExcelRequest,
    current_user: Annotated[User, Depends(current_active_user)],
):
    from src.tasks.export_excel import create_export_task_record, export_excel_task

    task = export_excel_task.delay(
        user_id=current_user.id,
        file_name=payload.file_name,
        service_path="src.services.geography.country.CountryService",
        model_path="src.db.models.geography.Country",
        serializer_path="src.schemas.geography.CountryResponse",
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


@router.get("/countries/{country_id}", response_model=geography.CountryResponse)
async def get_country(
    country_id: int, session: Annotated[AsyncSession, Depends(db_session.get_session)]
):
    return await geography_service.country_service.get_or_404(session, country_id)


@router.patch("/countries/{country_id}", response_model=geography.CountryResponse)
async def update_country(
    country_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    country: geography.CountryUpdate,
):
    return await geography_service.country_service.update(session, country_id, country)


@router.delete("/countries/{country_id}")
async def delete_country(
    country_id: int, session: Annotated[AsyncSession, Depends(db_session.get_session)]
):
    return await geography_service.country_service.delete(session, country_id)


@router.post("/regions/create", response_model=geography.RegionResponse)
async def create_region(
    region: geography.RegionCreate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [joinedload(Region.country)]
    return await geography_service.region_service.create(
        session, region, load_options=load_options
    )


@router.post("/regions", response_model=list[geography.RegionResponse])
async def get_regions(
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    filters: geography.RegionListRequest,
):
    load_options = [joinedload(Region.country)]
    return await geography_service.region_service.get_multi(
        session, load_options=load_options, filters=filters
    )


@router.post("/regions/export-excel")
async def export_regions_excel(
    payload: ExportExcelRequest,
    current_user: Annotated[User, Depends(current_active_user)],
):
    from src.tasks.export_excel import create_export_task_record, export_excel_task

    task = export_excel_task.delay(
        user_id=current_user.id,
        file_name=payload.file_name,
        service_path="src.services.geography.region.RegionService",
        model_path="src.db.models.geography.Region",
        serializer_path="src.schemas.geography.RegionResponse",
        load_options_paths=["country"],
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


@router.post("/regions/import-excel")
async def bulk_insert_regions(
    file: UploadFile,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_active_user)],
):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only Excel files are allowed",
        )
    result = await geography_service.region_service.import_excel(
        session, file, user_id=current_user.id
    )
    return result


@router.get("/regions/{region_id}", response_model=geography.RegionResponse)
async def get_region(
    region_id: int, session: Annotated[AsyncSession, Depends(db_session.get_session)]
):
    load_options = [joinedload(Region.country)]
    return await geography_service.region_service.get_or_404(
        session, region_id, load_options=load_options
    )


@router.patch("/regions/{region_id}", response_model=geography.RegionResponse)
async def update_region(
    region_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    region: geography.RegionUpdate,
):
    load_options = [joinedload(Region.country)]
    return await geography_service.region_service.update(
        session, region_id, region, load_options=load_options
    )


@router.delete("/regions/{region_id}")
async def delete_region(
    region_id: int, session: Annotated[AsyncSession, Depends(db_session.get_session)]
):
    return await geography_service.region_service.delete(session, region_id)


@router.post("/settlements/create", response_model=geography.SettlementResponse)
async def create_settlement(
    settlement: geography.SettlementCreate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [joinedload(Settlement.region)]
    return await geography_service.settlement_service.create(
        session, settlement, load_options=load_options
    )


@router.post("/settlements", response_model=list[geography.SettlementResponse])
async def get_settlements(
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    filters: geography.SettlementListRequest,
):
    load_options = [joinedload(Settlement.region)]
    return await geography_service.settlement_service.get_multi(
        session, load_options=load_options, filters=filters
    )


@router.post("/settlements/export-excel")
async def export_settlements_excel(
    payload: ExportExcelRequest,
    current_user: Annotated[User, Depends(current_active_user)],
):
    from src.tasks.export_excel import create_export_task_record, export_excel_task

    task = export_excel_task.delay(
        user_id=current_user.id,
        file_name=payload.file_name,
        service_path="src.services.geography.settlement.SettlementService",
        model_path="src.db.models.geography.Settlement",
        serializer_path="src.schemas.geography.SettlementResponse",
        load_options_paths=["region"],
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


@router.post("/settlements/import-excel")
async def bulk_insert_settlements(
    file: UploadFile,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_active_user)],
):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only Excel files are allowed",
        )
    result = await geography_service.settlement_service.import_excel(
        session, file, user_id=current_user.id
    )
    return result


@router.get("/settlements/{settlement_id}", response_model=geography.SettlementResponse)
async def get_settlement(
    settlement_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [joinedload(Settlement.region)]
    return await geography_service.settlement_service.get_or_404(
        session, settlement_id, load_options=load_options
    )


@router.patch(
    "/settlements/{settlement_id}", response_model=geography.SettlementResponse
)
async def update_settlement(
    settlement_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    settlement: geography.SettlementUpdate,
):
    load_options = [joinedload(Settlement.region)]
    return await geography_service.settlement_service.update(
        session, settlement_id, settlement, load_options=load_options
    )


@router.delete("/settlements/{settlement_id}")
async def delete_settlement(
    settlement_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await geography_service.settlement_service.delete(session, settlement_id)


@router.post("/districts/create", response_model=geography.DistrictResponse)
async def create_district(
    district: geography.DistrictCreate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [
        joinedload(District.settlement),
        joinedload(District.region),
        joinedload(District.company),
    ]
    return await geography_service.district_service.create(
        session, district, load_options=load_options
    )


@router.post("/districts", response_model=list[geography.DistrictResponse])
async def get_districts(
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    filters: geography.DistrictListRequest,
):
    load_options = [
        joinedload(District.settlement),
        joinedload(District.region),
        joinedload(District.company),
    ]
    return await geography_service.district_service.get_multi(
        session, load_options=load_options, filters=filters
    )


@router.post("/districts/export-excel")
async def export_districts_excel(
    payload: ExportExcelRequest,
    current_user: Annotated[User, Depends(current_active_user)],
):
    from src.tasks.export_excel import create_export_task_record, export_excel_task

    task = export_excel_task.delay(
        user_id=current_user.id,
        file_name=payload.file_name,
        service_path="src.services.geography.district.DistrictService",
        model_path="src.db.models.geography.District",
        serializer_path="src.schemas.geography.DistrictResponse",
        load_options_paths=["settlement", "region", "company"],
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


@router.post("/districts/import-excel")
async def bulk_insert_districts(
    file: UploadFile,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_active_user)],
):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only Excel files are allowed",
        )
    result = await geography_service.district_service.import_excel(
        session, file, user_id=current_user.id
    )
    return result


@router.get("/districts/{district_id}", response_model=geography.DistrictResponse)
async def get_district(
    district_id: int, session: Annotated[AsyncSession, Depends(db_session.get_session)]
):
    load_options = [
        joinedload(District.settlement),
        joinedload(District.region),
        joinedload(District.company),
    ]
    return await geography_service.district_service.get_or_404(
        session, district_id, load_options=load_options
    )


@router.patch("/districts/{district_id}", response_model=geography.DistrictResponse)
async def update_district(
    district_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    district: geography.DistrictUpdate,
):
    load_options = [
        joinedload(District.settlement),
        joinedload(District.region),
        joinedload(District.company),
    ]
    return await geography_service.district_service.update(
        session, district_id, district, load_options=load_options
    )


@router.delete("/districts/{district_id}")
async def delete_district(
    district_id: int, session: Annotated[AsyncSession, Depends(db_session.get_session)]
):
    return await geography_service.district_service.delete(session, district_id)
