from typing import Annotated

from fastapi import APIRouter, Depends, UploadFile, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from src.db.session import db_session
from src.schemas import sale
from src.services.sale import primary_sales_service, secondary_sales_service, tertiary_sales_service
from src.api.dependencies.current_user import current_operator_user, current_active_user
from src.api.dependencies.company import (
    can_view_primary_sales, can_view_secondary_sales,
    can_view_tertiary_sales
)
from src.db.models import PrimarySalesAndStock, SecondarySales, TertiarySalesAndStock, SKU, Pharmacy, User, Company

router = APIRouter()


@router.get('/primary', response_model=list[sale.PrimarySalesAndStockResponse], dependencies=[Depends(current_operator_user)])
async def get_primary_sales(
        filters: Annotated[sale.PrimarySalesAndStockFilter, Query()],
        session: Annotated[AsyncSession, Depends(db_session.get_session)]
):
    load_options = [
        joinedload(PrimarySalesAndStock.sku).joinedload(SKU.brand),
        joinedload(PrimarySalesAndStock.distributor)
    ]
    return await primary_sales_service.get_multi(session, filters, load_options=load_options)


@router.post('/primary', response_model=sale.PrimarySalesAndStockResponse, dependencies=[Depends(current_operator_user)])
async def create_primary_sales(
        new_primary_sales: sale.PrimarySalesAndStockCreate,
        session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [
        joinedload(PrimarySalesAndStock.sku).joinedload(SKU.brand),
        joinedload(PrimarySalesAndStock.distributor)
    ]
    return await primary_sales_service.create(session, new_primary_sales, load_options=load_options)


@router.post('/primary/import-excel', dependencies=[Depends(current_operator_user)])
async def bulk_insert_primary_sales(
        file: UploadFile,
        session: Annotated[AsyncSession, Depends(db_session.get_session)],
        current_user: Annotated[User, Depends(current_active_user)]
):
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only Excel files are allowed"
        )
    result = await primary_sales_service.import_sales(session, file, user_id=current_user.id)
    return result


@router.get(
    '/primary/reports/stock-levels',
    dependencies=[Depends(can_view_primary_sales)]
)
async def get_primary_stock(
        session: Annotated[AsyncSession, Depends(db_session.get_session)],
        filters: Annotated[sale.ShipmentStockFilter, Query()],
        current_user: Annotated[User, Depends(current_active_user)]
):
    return await primary_sales_service.get_shipment_stock_report(
        session,
        filters,
        indicator='Остаток на складе',
        company_id=current_user.company_id,
    )


@router.get('/primary/reports/sales', dependencies=[Depends(can_view_primary_sales)])
async def get_primary_sales(
        filters: Annotated[sale.ShipmentStockFilter, Query()],
        session: Annotated[AsyncSession, Depends(db_session.get_session)],
        current_user: Annotated[User, Depends(current_active_user)],
):
    return await primary_sales_service.get_shipment_stock_report(
        session,
        filters,
        indicator='Первичная продажа',
        company_id=current_user.company_id,
    )


@router.get(
    '/primary/reports/chart',
    dependencies=[Depends(can_view_primary_sales)]
)
async def get_primary_sales_chart(
        filters: Annotated[sale.PeriodFilter, Query()],
        session: Annotated[AsyncSession, Depends(db_session.get_session)],
        current_user: Annotated[User, Depends(current_active_user)]
):
    return await primary_sales_service.get_period_totals(session, filters, company_id=current_user.company_id)


@router.get('/primary/reports/stock-coverages', dependencies=[Depends(can_view_primary_sales)])
async def get_inventory(
        filters: Annotated[sale.StockCoverageFilter, Query()],
        session: Annotated[AsyncSession, Depends(db_session.get_session)],
        current_user: Annotated[User, Depends(current_active_user)]
):
    return await primary_sales_service.get_stock_coverage(
        session,
        filters,
        company_id=current_user.company_id,
    )


@router.get('/primary/reports/distributor-shares', dependencies=[Depends(can_view_primary_sales)])
async def get_distributor_share(
        filters: Annotated[sale.DistributorShareFilter, Query()],
        session: Annotated[AsyncSession, Depends(db_session.get_session)],
        current_user: Annotated[User, Depends(current_active_user)]
):
    return await primary_sales_service.get_distributor_share_report(
        session,
        filters,
        company_id=current_user.company_id,
    )


@router.get('/primary/reports/distributor-shares/chart', dependencies=[Depends(can_view_primary_sales)])
async def get_distributor_shares_chart(
        filters: Annotated[sale.SalesReportFilter, Query()],
        session: Annotated[AsyncSession, Depends(db_session.get_session)],
        current_user: Annotated[User, Depends(current_active_user)]
):
    return await primary_sales_service.get_distributor_share_chart(
        session,
        filters,
        company_id=current_user.company_id,
    )


@router.get('/primary/{primary_sales_id}', response_model=sale.PrimarySalesAndStockResponse, dependencies=[Depends(current_operator_user)])
async def get_primary_sales(
        primary_sales_id: int,
        session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [
        joinedload(PrimarySalesAndStock.sku).joinedload(SKU.brand),
        joinedload(PrimarySalesAndStock.distributor)
    ]
    return await primary_sales_service.get_or_404(session, primary_sales_id, load_options=load_options)


@router.patch('/primary/{primary_sales_id}', response_model=sale.PrimarySalesAndStockResponse, dependencies=[Depends(current_operator_user)])
async def update_primary_sales(
        primary_sales_id: int,
        updated_primary_sales: sale.PrimarySalesAndStockUpdate,
        session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [
        joinedload(PrimarySalesAndStock.sku).joinedload(SKU.brand),
        joinedload(PrimarySalesAndStock.distributor)
    ]
    return await primary_sales_service.update(session, primary_sales_id, updated_primary_sales, load_options=load_options)


@router.delete('/primary/{primary_sales_id}', dependencies=[Depends(current_operator_user)])
async def delete_primary_sales(
        primary_sales_id: int,
        session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await primary_sales_service.delete(session, primary_sales_id)


@router.get('/secondary', response_model=list[sale.SecondarySalesResponse], dependencies=[Depends(current_operator_user)])
async def get_secondary_sales(
        filters: Annotated[sale.SecondaryTertiarySalesFilter, Query()],
        session: Annotated[AsyncSession, Depends(db_session.get_session)]
):
    load_options = [
        joinedload(SecondarySales.pharmacy).joinedload(Pharmacy.geo_indicator),
        joinedload(SecondarySales.sku).joinedload(SKU.brand)
    ]
    return await secondary_sales_service.get_multi(session, filters, load_options=load_options)


@router.post('/secondary', response_model=sale.SecondarySalesResponse, dependencies=[Depends(current_operator_user)])
async def create_secondary_sales(
        new_secondary_sales: sale.SecondarySalesCreate,
        session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [
        joinedload(SecondarySales.pharmacy).joinedload(Pharmacy.geo_indicator),
        joinedload(SecondarySales.sku).joinedload(SKU.brand)
    ]
    return await secondary_sales_service.create(session, new_secondary_sales, load_options=load_options)


@router.post('/secondary/import-excel', dependencies=[Depends(current_operator_user)])
async def bulk_insert_secondary_sales(
        file: UploadFile,
        session: Annotated[AsyncSession, Depends(db_session.get_session)],
        current_user: Annotated[User, Depends(current_active_user)]
):
    result = await secondary_sales_service.import_sales(session, file, user_id=current_user.id)
    return result


@router.get(
    '/secondary/reports/chart',
    dependencies=[Depends(can_view_secondary_sales)]
)
async def get_secondary_sales_chart(
        filters: Annotated[sale.SecTerSalesPeriodFilter, Query()],
        session: Annotated[AsyncSession, Depends(db_session.get_session)],
        current_user: Annotated[User, Depends(current_active_user)]
):
    return await secondary_sales_service.get_period_totals(session, filters, company_id=current_user.company_id)


@router.get(
    '/secondary/reports/sales',
    dependencies=[Depends(can_view_secondary_sales)]
)
async def get_secondary_sales_report(
        filters: Annotated[sale.SecTerSalesReportFilter, Query()],
        session: Annotated[AsyncSession, Depends(db_session.get_session)],
        current_user: Annotated[User, Depends(current_active_user)]
):
    return await secondary_sales_service.get_sales_report(session, filters, company_id=current_user.company_id)


@router.get(
    '/secondary/reports/sales-by-distributors',
    dependencies=[Depends(can_view_secondary_sales)]
)
async def get_sales_report_by_distributors(
        session: Annotated[AsyncSession, Depends(db_session.get_session)],
        current_user: Annotated[User, Depends(current_active_user)],
        filters: Annotated[sale.SalesByDistributorFilter, Query()]
):
    return await secondary_sales_service.get_sales_by_distributor_report(
        session=session,
        company_id=current_user.company_id,
        filters=filters,
    )


@router.get(
    '/secondary/reports/sales-by-distributors/chart',
    dependencies=[Depends(can_view_secondary_sales)]
)
async def get_sales_report_by_distributors_chart(
        session: Annotated[AsyncSession, Depends(db_session.get_session)],
        current_user: Annotated[User, Depends(current_active_user)],
        filters: Annotated[sale.ChartSalesByDistributorFilter, Query()]
):
    return await secondary_sales_service.get_total_sales_by_distributor(
        session=session,
        company_id=current_user.company_id,
        filters=filters,
    )


@router.get('/secondary/{secondary_sales_id}', response_model=sale.SecondarySalesResponse, dependencies=[Depends(current_operator_user)])
async def get_secondary_sales(
        secondary_sales_id: int,
        session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [
        joinedload(SecondarySales.pharmacy).joinedload(Pharmacy.geo_indicator),
        joinedload(SecondarySales.sku).joinedload(SKU.brand)
    ]
    return await secondary_sales_service.get_or_404(session, secondary_sales_id, load_options=load_options)


@router.patch('/secondary/{secondary_sales_id}', response_model=sale.SecondarySalesResponse, dependencies=[Depends(current_operator_user)])
async def update_secondary_sales(
        secondary_sales_id: int,
        updated_secondary_sales: sale.SecondarySalesUpdate,
        session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [
        joinedload(SecondarySales.pharmacy).joinedload(Pharmacy.geo_indicator),
        joinedload(SecondarySales.sku).joinedload(SKU.brand)
    ]
    return await secondary_sales_service.update(session, secondary_sales_id, updated_secondary_sales, load_options=load_options)


@router.delete('/secondary/{secondary_sales_id}', dependencies=[Depends(current_operator_user)])
async def delete_secondary_sales(
        secondary_sales_id: int,
        session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await secondary_sales_service.delete(session, secondary_sales_id)


@router.get('/tertiary', response_model=list[sale.TertiarySalesResponse], dependencies=[Depends(current_operator_user)])
async def get_tertiary_sales(
        filters: Annotated[sale.SecondaryTertiarySalesFilter, Query()],
        session: Annotated[AsyncSession, Depends(db_session.get_session)]
):
    load_options = [
        joinedload(TertiarySalesAndStock.pharmacy).joinedload(Pharmacy.geo_indicator),
        joinedload(TertiarySalesAndStock.sku).joinedload(SKU.brand)
    ]
    return await tertiary_sales_service.get_multi(session, filters, load_options=load_options)


@router.post('/tertiary', response_model=sale.TertiarySalesResponse, dependencies=[Depends(current_operator_user)])
async def create_tertiary_sales(
        new_tertiary_sales: sale.TertiarySalesCreate,
        session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [
        joinedload(TertiarySalesAndStock.pharmacy).joinedload(Pharmacy.geo_indicator),
        joinedload(TertiarySalesAndStock.sku).joinedload(SKU.brand)
    ]
    return await tertiary_sales_service.create(session, new_tertiary_sales, load_options=load_options)


@router.post('/tertiary/import-excel', dependencies=[Depends(current_operator_user)])
async def bulk_insert_tertiary_sales(
        file: UploadFile,
        session: Annotated[AsyncSession, Depends(db_session.get_session)],
        current_user: Annotated[User, Depends(current_active_user)]
):
    result = await tertiary_sales_service.import_sales(session, file, user_id=current_user.id)
    return result


@router.get(
    '/tertiary/reports/sales',
    dependencies=[Depends(can_view_tertiary_sales)]
)
async def get_tertiary_sales_report(
        filters: Annotated[sale.SecTerSalesReportFilter, Query()],
        session: Annotated[AsyncSession, Depends(db_session.get_session)],
        current_user: Annotated[User, Depends(current_active_user)]
):
    return await tertiary_sales_service.get_sales_report(session, filters, company_id=current_user.company_id)


@router.get(
    '/tertiary/reports/chart',
    dependencies=[Depends(can_view_tertiary_sales)]
)
async def get_tertiary_sales_chart(
        filters: Annotated[sale.SecTerSalesPeriodFilter, Query()],
        session: Annotated[AsyncSession, Depends(db_session.get_session)],
        current_user: Annotated[User, Depends(current_active_user)]
):
    return await tertiary_sales_service.get_period_totals(session, filters, company_id=current_user.company_id)


@router.get('/tertiary/{tertiary_sales_id}', response_model=sale.TertiarySalesResponse, dependencies=[Depends(current_operator_user)])
async def get_tertiary_sales(
    tertiary_sales_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [
        joinedload(TertiarySalesAndStock.pharmacy).joinedload(Pharmacy.geo_indicator),
        joinedload(TertiarySalesAndStock.sku).joinedload(SKU.brand)
    ]
    return await tertiary_sales_service.get_or_404(session, tertiary_sales_id, load_options=load_options)


@router.patch('/tertiary/{tertiary_sales_id}', response_model=sale.TertiarySalesResponse, dependencies=[Depends(current_operator_user)])
async def update_tertiary_sales(
    tertiary_sales_id: int,
    updated_tertiary_sales: sale.TertiarySalesUpdate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [
        joinedload(TertiarySalesAndStock.pharmacy).joinedload(Pharmacy.geo_indicator),
        joinedload(TertiarySalesAndStock.sku).joinedload(SKU.brand)
    ]
    return await tertiary_sales_service.update(session, tertiary_sales_id, updated_tertiary_sales, load_options=load_options)


@router.delete('/tertiary/{tertiary_sales_id}', dependencies=[Depends(current_operator_user)])
async def delete_tertiary_sales(
    tertiary_sales_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await tertiary_sales_service.delete(session, tertiary_sales_id)


@router.get(
    '/tertiary/reports/numeric-distribution',
    dependencies=[Depends(can_view_tertiary_sales)]
)
async def get_numeric_distribution_report(
        session: Annotated[AsyncSession, Depends(db_session.get_session)],
        filters: Annotated[sale.NumericDistributionFilter, Query()],
        current_user: Annotated[User, Depends(current_active_user)]
):
    return await tertiary_sales_service.get_numeric_distribution(session, filters, company_id=current_user.company_id)


@router.get(
    '/tertiary/reports/low-stock-pharmacies',
    dependencies=[Depends(can_view_tertiary_sales)]
)
async def get_low_stock_pharmacies(
        session: Annotated[AsyncSession, Depends(db_session.get_session)],
        filters: Annotated[sale.LowStockLevelFilter, Query()],
        current_user: Annotated[User, Depends(current_active_user)]
):
    return await tertiary_sales_service.get_low_stock(session, filters=filters, company_id=current_user.company_id)
