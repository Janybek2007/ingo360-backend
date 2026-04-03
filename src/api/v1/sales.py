from typing import Annotated

import orjson
from fastapi import APIRouter, Depends, Query
from fastapi.responses import Response
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from src.api.dependencies.company import (
    can_view_primary_sales,
    can_view_secondary_sales,
    can_view_tertiary_sales,
)
from src.api.dependencies.current_user import current_active_user, current_operator_user
from src.api.dependencies.excel_file import ExcelFile
from src.api.utils.pivot_distributore_share import pivot_distributor_share
from src.api.utils.pivot_sales_by_distributors import pivot_sales_by_distributors
from src.db.models import (
    SKU,
    Pharmacy,
    PrimarySalesAndStock,
    SecondarySales,
    TertiarySalesAndStock,
    User,
    Visit,
)
from src.db.session import db_session
from src.schemas import sale
from src.schemas.base_filter import PaginatedResponse
from src.schemas.export import ExportExcelRequest
from src.services.sale import (
    primary_sales_service,
    secondary_sales_service,
    tertiary_sales_service,
)

router = APIRouter()


@router.post(
    "/primary",
    response_model=PaginatedResponse[sale.PrimarySalesAndStockResponse],
    dependencies=[Depends(current_operator_user)],
)
async def list_primary_sales(
    payload: sale.PrimarySalesAndStockListRequest,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [
        joinedload(PrimarySalesAndStock.sku).joinedload(SKU.brand),
        joinedload(PrimarySalesAndStock.distributor),
    ]
    return await primary_sales_service.get_multi(
        session, payload, load_options=load_options
    )


@router.post("/primary/export-excel")
async def export_primary_excel(
    payload: ExportExcelRequest,
    current_user: Annotated[User, Depends(current_active_user)],
):
    from src.tasks.export_excel import create_export_task_record, export_excel_task

    task = export_excel_task.delay(
        user_id=current_user.id,
        file_name=payload.file_name,
        service_path="src.services.sale.PrimarySalesAndStockService",
        model_path="src.db.models.PrimarySalesAndStock",
        serializer_path="src.schemas.sale.PrimarySalesAndStockResponse",
        load_options_paths=["sku.brand", "distributor"],
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
    "/primary/create",
    response_model=sale.PrimarySalesAndStockResponse,
    dependencies=[Depends(current_operator_user)],
)
async def create_primary_sales(
    new_primary_sales: sale.PrimarySalesAndStockCreate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [
        joinedload(PrimarySalesAndStock.sku).joinedload(SKU.brand),
        joinedload(PrimarySalesAndStock.distributor),
    ]
    return await primary_sales_service.create(
        session, new_primary_sales, load_options=load_options
    )


@router.post("/primary/import-excel", dependencies=[Depends(current_operator_user)])
async def bulk_insert_primary_sales(
    file: ExcelFile,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_active_user)],
):
    result = await primary_sales_service.import_sales(
        session, file, user_id=current_user.id
    )
    return result


@router.post(
    "/primary/reports/stock-levels", dependencies=[Depends(can_view_primary_sales)]
)
async def get_primary_stock(
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    filters: sale.ShipmentStockFilter,
    current_user: Annotated[User, Depends(current_active_user)],
):
    result = await primary_sales_service.get_shipment_stock_report(
        session,
        filters=filters,
        indicator="остат",
        company_id=current_user.company_id,
    )
    body = orjson.dumps(result)
    return Response(content=body, media_type="application/json")


@router.post("/primary/reports/sales", dependencies=[Depends(can_view_primary_sales)])
async def get_primary_sales(
    filters: sale.ShipmentStockFilter,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_active_user)],
):
    result = await primary_sales_service.get_shipment_stock_report(
        session,
        filters=filters,
        indicator="продаж",
        company_id=current_user.company_id,
    )
    body = orjson.dumps(result)
    return Response(content=body, media_type="application/json")


@router.post("/primary/reports/chart", dependencies=[Depends(can_view_primary_sales)])
async def get_primary_sales_chart(
    filters: sale.PeriodFilter,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_active_user)],
):
    return await primary_sales_service.get_period_totals(
        session, filters, company_id=current_user.company_id
    )


@router.post(
    "/primary/reports/stock-coverages", dependencies=[Depends(can_view_primary_sales)]
)
async def get_inventory(
    filters: sale.StockCoverageFilter,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_active_user)],
):
    result = await primary_sales_service.get_stock_coverage(
        session,
        filters,
        company_id=current_user.company_id,
    )
    body = orjson.dumps(result)
    return Response(content=body, media_type="application/json")


@router.post(
    "/primary/reports/distributor-shares",
    dependencies=[Depends(can_view_primary_sales)],
)
async def get_distributor_share(
    filters: sale.DistributorShareFilter,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_active_user)],
):
    result = await primary_sales_service.get_distributor_share_report(
        session,
        filters,
        company_id=current_user.company_id,
    )
    body = orjson.dumps(result)
    return Response(content=body, media_type="application/json")


@router.post(
    "/primary/reports/distributor-shares/chart",
    dependencies=[Depends(can_view_primary_sales)],
)
async def get_distributor_shares_chart(
    filters: sale.SalesReportFilter,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_active_user)],
):
    rows = await primary_sales_service.get_distributor_share_chart(
        session,
        filters,
        company_id=current_user.company_id,
    )
    result = pivot_distributor_share(rows)
    body = orjson.dumps(result)
    return Response(content=body, media_type="application/json")


@router.get(
    "/primary/{primary_sales_id}",
    response_model=sale.PrimarySalesAndStockResponse,
    dependencies=[Depends(current_operator_user)],
)
async def get_primary_sale(
    primary_sales_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [
        joinedload(PrimarySalesAndStock.sku).joinedload(SKU.brand),
        joinedload(PrimarySalesAndStock.distributor),
    ]
    return await primary_sales_service.get_or_404(
        session, primary_sales_id, load_options=load_options
    )


@router.patch(
    "/primary/{primary_sales_id}",
    response_model=sale.PrimarySalesAndStockResponse,
    dependencies=[Depends(current_operator_user)],
)
async def update_primary_sales(
    primary_sales_id: int,
    updated_primary_sales: sale.PrimarySalesAndStockUpdate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [
        joinedload(PrimarySalesAndStock.sku).joinedload(SKU.brand),
        joinedload(PrimarySalesAndStock.distributor),
    ]
    return await primary_sales_service.update(
        session, primary_sales_id, updated_primary_sales, load_options=load_options
    )


@router.delete(
    "/primary/{primary_sales_id}", dependencies=[Depends(current_operator_user)]
)
async def delete_primary_sales(
    primary_sales_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await primary_sales_service.delete(session, primary_sales_id)


@router.post(
    "/secondary",
    response_model=PaginatedResponse[sale.SecondarySalesResponse],
    dependencies=[Depends(current_operator_user)],
)
async def list_secondary_sales(
    payload: sale.SecondaryTertiarySalesListRequest,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [
        joinedload(SecondarySales.pharmacy).joinedload(Pharmacy.geo_indicator),
        joinedload(SecondarySales.pharmacy).joinedload(Pharmacy.distributor),
        joinedload(SecondarySales.distributor),
        joinedload(SecondarySales.sku).joinedload(SKU.brand),
    ]

    return await secondary_sales_service.get_multi(
        session, payload, load_options=load_options
    )


@router.post("/secondary/export-excel")
async def export_secondary_excel(
    payload: ExportExcelRequest,
    current_user: Annotated[User, Depends(current_active_user)],
):
    from src.tasks.export_excel import create_export_task_record, export_excel_task

    task = export_excel_task.delay(
        user_id=current_user.id,
        file_name=payload.file_name,
        service_path="src.services.sale.SecondarySalesService",
        model_path="src.db.models.SecondarySales",
        serializer_path="src.schemas.sale.SecondarySalesResponse",
        load_options_paths=[
            "pharmacy.geo_indicator",
            "pharmacy.distributor",
            "distributor",
            "sku.brand",
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
    "/secondary/create",
    response_model=sale.SecondarySalesResponse,
    dependencies=[Depends(current_operator_user)],
)
async def create_secondary_sales(
    new_secondary_sales: sale.SecondarySalesCreate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [
        joinedload(SecondarySales.pharmacy).joinedload(Pharmacy.geo_indicator),
        joinedload(SecondarySales.pharmacy).joinedload(Pharmacy.distributor),
        joinedload(SecondarySales.sku).joinedload(SKU.brand),
    ]
    return await secondary_sales_service.create(
        session, new_secondary_sales, load_options=load_options
    )


@router.post("/secondary/import-excel", dependencies=[Depends(current_operator_user)])
async def bulk_insert_secondary_sales(
    file: ExcelFile,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_active_user)],
):
    result = await secondary_sales_service.import_sales(
        session, file, user_id=current_user.id
    )
    return result


@router.post(
    "/secondary/reports/chart", dependencies=[Depends(can_view_secondary_sales)]
)
async def get_secondary_sales_chart(
    filters: sale.SecTerSalesPeriodFilter,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_active_user)],
):
    return await secondary_sales_service.get_period_totals(
        session, filters, company_id=current_user.company_id
    )


@router.post(
    "/secondary/reports/sales", dependencies=[Depends(can_view_secondary_sales)]
)
async def get_secondary_sales_report(
    filters: sale.SecTerSalesReportFilter,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_active_user)],
):
    result = await secondary_sales_service.get_sales_report(
        session, filters=filters, company_id=current_user.company_id
    )
    body = orjson.dumps(result)
    return Response(content=body, media_type="application/json")


@router.get(
    "/secondary/reports/sales-by-distributors",
    dependencies=[Depends(can_view_secondary_sales)],
)
async def get_sales_report_by_distributors(
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_active_user)],
    filters: Annotated[sale.SalesByDistributorFilter, Query()],
):
    result = await secondary_sales_service.get_sales_by_distributor_report(
        session=session,
        company_id=current_user.company_id,
        filters=filters,
    )
    body = orjson.dumps(result)
    return Response(content=body, media_type="application/json")


@router.post(
    "/secondary/reports/sales-by-distributors/chart",
    dependencies=[Depends(can_view_secondary_sales)],
)
async def get_sales_report_by_distributors_chart(
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_active_user)],
    filters: sale.ChartSalesByDistributorFilter,
):
    rows = await secondary_sales_service.get_total_sales_by_distributor(
        session=session,
        company_id=current_user.company_id,
        filters=filters,
    )
    result = pivot_sales_by_distributors(rows)
    body = orjson.dumps(result)
    return Response(content=body, media_type="application/json")


@router.get(
    "/secondary/{secondary_sales_id}",
    response_model=sale.SecondarySalesResponse,
    dependencies=[Depends(current_operator_user)],
)
async def get_secondary_sales(
    secondary_sales_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [
        joinedload(SecondarySales.pharmacy).joinedload(Pharmacy.geo_indicator),
        joinedload(SecondarySales.pharmacy).joinedload(Pharmacy.distributor),
        joinedload(SecondarySales.sku).joinedload(SKU.brand),
    ]
    return await secondary_sales_service.get_or_404(
        session, secondary_sales_id, load_options=load_options
    )


@router.patch(
    "/secondary/{secondary_sales_id}",
    response_model=sale.SecondarySalesResponse,
    dependencies=[Depends(current_operator_user)],
)
async def update_secondary_sales(
    secondary_sales_id: int,
    updated_secondary_sales: sale.SecondarySalesUpdate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [
        joinedload(SecondarySales.pharmacy).joinedload(Pharmacy.geo_indicator),
        joinedload(SecondarySales.pharmacy).joinedload(Pharmacy.distributor),
        joinedload(SecondarySales.sku).joinedload(SKU.brand),
    ]
    return await secondary_sales_service.update(
        session, secondary_sales_id, updated_secondary_sales, load_options=load_options
    )


@router.delete(
    "/secondary/{secondary_sales_id}", dependencies=[Depends(current_operator_user)]
)
async def delete_secondary_sales(
    secondary_sales_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await secondary_sales_service.delete(session, secondary_sales_id)


@router.post(
    "/tertiary",
    response_model=PaginatedResponse[sale.TertiarySalesResponse],
    dependencies=[Depends(current_operator_user)],
)
async def list_tertiary_sales(
    payload: sale.SecondaryTertiarySalesListRequest,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [
        joinedload(TertiarySalesAndStock.pharmacy).joinedload(Pharmacy.geo_indicator),
        joinedload(TertiarySalesAndStock.pharmacy).joinedload(Pharmacy.distributor),
        joinedload(TertiarySalesAndStock.sku).joinedload(SKU.brand),
        joinedload(TertiarySalesAndStock.distributor),
    ]
    return await tertiary_sales_service.get_multi(
        session, payload, load_options=load_options
    )


@router.post("/tertiary/export-excel")
async def export_tertiary_excel(
    payload: ExportExcelRequest,
    current_user: Annotated[User, Depends(current_active_user)],
):
    from src.tasks.export_excel import create_export_task_record, export_excel_task

    task = export_excel_task.delay(
        user_id=current_user.id,
        file_name=payload.file_name,
        service_path="src.services.sale.TertiarySalesService",
        model_path="src.db.models.TertiarySalesAndStock",
        serializer_path="src.schemas.sale.TertiarySalesResponse",
        load_options_paths=[
            "pharmacy.geo_indicator",
            "pharmacy.distributor",
            "distributor",
            "sku.brand",
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
    "/tertiary/create",
    response_model=sale.TertiarySalesResponse,
    dependencies=[Depends(current_operator_user)],
)
async def create_tertiary_sales(
    new_tertiary_sales: sale.TertiarySalesCreate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [
        joinedload(TertiarySalesAndStock.pharmacy).joinedload(Pharmacy.geo_indicator),
        joinedload(TertiarySalesAndStock.pharmacy).joinedload(Pharmacy.distributor),
        joinedload(TertiarySalesAndStock.sku).joinedload(SKU.brand),
    ]
    return await tertiary_sales_service.create(
        session, new_tertiary_sales, load_options=load_options
    )


@router.post("/tertiary/import-excel", dependencies=[Depends(current_operator_user)])
async def bulk_insert_tertiary_sales(
    file: ExcelFile,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_active_user)],
):
    result = await tertiary_sales_service.import_sales(
        session, file, user_id=current_user.id
    )
    return result


@router.post("/tertiary/reports/sales", dependencies=[Depends(can_view_tertiary_sales)])
async def get_tertiary_sales_report(
    filters: sale.SecTerSalesReportFilter,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_active_user)],
):
    result = await tertiary_sales_service.get_sales_report(
        session, filters=filters, company_id=current_user.company_id
    )
    body = orjson.dumps(result)
    return Response(content=body, media_type="application/json")


@router.post("/tertiary/reports/chart", dependencies=[Depends(can_view_tertiary_sales)])
async def get_tertiary_sales_chart(
    filters: sale.SecTerSalesPeriodFilter,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_active_user)],
):
    return await tertiary_sales_service.get_period_totals(
        session, filters, company_id=current_user.company_id
    )


@router.get(
    "/tertiary/{tertiary_sales_id}",
    response_model=sale.TertiarySalesResponse,
    dependencies=[Depends(current_operator_user)],
)
async def get_tertiary_sales(
    tertiary_sales_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [
        joinedload(TertiarySalesAndStock.pharmacy).joinedload(Pharmacy.geo_indicator),
        joinedload(TertiarySalesAndStock.pharmacy).joinedload(Pharmacy.distributor),
        joinedload(TertiarySalesAndStock.sku).joinedload(SKU.brand),
    ]
    return await tertiary_sales_service.get_or_404(
        session, tertiary_sales_id, load_options=load_options
    )


@router.patch(
    "/tertiary/{tertiary_sales_id}",
    response_model=sale.TertiarySalesResponse,
    dependencies=[Depends(current_operator_user)],
)
async def update_tertiary_sales(
    tertiary_sales_id: int,
    updated_tertiary_sales: sale.TertiarySalesUpdate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [
        joinedload(TertiarySalesAndStock.pharmacy).joinedload(Pharmacy.geo_indicator),
        joinedload(TertiarySalesAndStock.pharmacy).joinedload(Pharmacy.distributor),
        joinedload(TertiarySalesAndStock.sku).joinedload(SKU.brand),
    ]
    return await tertiary_sales_service.update(
        session, tertiary_sales_id, updated_tertiary_sales, load_options=load_options
    )


@router.delete(
    "/tertiary/{tertiary_sales_id}", dependencies=[Depends(current_operator_user)]
)
async def delete_tertiary_sales(
    tertiary_sales_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await tertiary_sales_service.delete(session, tertiary_sales_id)


@router.post(
    "/tertiary/reports/numeric-distribution",
    dependencies=[Depends(can_view_tertiary_sales)],
)
async def get_numeric_distribution_report(
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    filters: sale.NumericDistributionFilter,
    current_user: Annotated[User, Depends(current_active_user)],
):
    return await tertiary_sales_service.get_numeric_distribution(
        session, filters, company_id=current_user.company_id
    )


@router.post(
    "/tertiary/reports/stock",
    dependencies=[Depends(can_view_tertiary_sales)],
)
async def get_tertiary_stock_report(
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    filters: sale.SecTerSalesReportFilter,
    current_user: Annotated[User, Depends(current_active_user)],
):
    result = await tertiary_sales_service.get_stock_report(
        session, filters=filters, company_id=current_user.company_id
    )
    body = orjson.dumps(result)
    return Response(content=body, media_type="application/json")


@router.get("/last-year", dependencies=[Depends(current_active_user)])
async def get_last_year(
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    from sqlalchemy import func, select

    stmt = select(
        select(func.max(PrimarySalesAndStock.year)).scalar_subquery().label("primary"),
        select(func.max(SecondarySales.year)).scalar_subquery().label("secondary"),
        select(func.max(TertiarySalesAndStock.year))
        .scalar_subquery()
        .label("tertiary"),
        select(func.max(Visit.year)).scalar_subquery().label("visits"),
    )
    row = (await session.execute(stmt)).one()

    return {
        "primary": row.primary,
        "secondary": row.secondary,
        "tertiary": row.tertiary,
        "visits": row.visits,
    }
