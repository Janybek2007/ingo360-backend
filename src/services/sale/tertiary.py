import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, AsyncIterator
from uuid import uuid4

from sqlalchemy import Float, Numeric, and_, case, func, or_, select

from src.db.models import (
    SKU,
    Brand,
    Distributor,
    GeoIndicator,
    ImportLogs,
    Pharmacy,
    ProductGroup,
    PromotionType,
    Segment,
    TertiarySalesAndStock,
)
from src.import_fields import sale
from src.mapping.dimension_mapping.sale import (
    BASE_SALE_DIMENSTION_MAPPING_WITH_GEO_INDICATOR,
    BASE_SALE_DIMENSTION_MAPPING_WITH_GEO_INDICATOR_AND_SEGMENT,
)
from src.mapping.sales import tertiary_sales_mapping
from src.schemas import sale as sale_schema
from src.schemas.base_filter import PaginatedResponse
from src.services.base import BaseService, ModelType
from src.services.sale.utils import RelationSpec, import_sales_from_excel
from src.utils.build_dimensions import build_dimensions
from src.utils.build_period_key import build_period_key
from src.utils.build_period_values import build_period_values
from src.utils.list_query_helper import (
    InOrNullSpec,
    ListQueryHelper,
    NumberTypedSpec,
    SearchSpec,
)

if TYPE_CHECKING:
    from fastapi import UploadFile
    from sqlalchemy.ext.asyncio import AsyncSession


class TertiarySalesService(
    BaseService[
        TertiarySalesAndStock,
        sale_schema.TertiarySalesCreate,
        sale_schema.TertiarySalesUpdate,
    ]
):
    async def import_sales(
        self,
        session: "AsyncSession",
        file: "UploadFile",
        user_id: int,
        batch_size: int = 10_000,
    ):
        from src.db.models.excel_tasks import ExcelTaskType
        from src.tasks.sale_imports import create_excel_task_record, import_sales_task

        upload_dir = Path("temp")
        upload_dir.mkdir(exist_ok=True)
        file_id = str(uuid4())

        file_path = upload_dir / f"{file_id}_{file.filename}"

        try:
            with open(file_path, "wb") as f:
                content = await file.read()
                f.write(content)

            task = import_sales_task.delay(
                file_path=str(file_path),
                user_id=user_id,
                service_path="src.services.sale.TertiarySalesService",
                model_path="src.db.models.TertiarySalesAndStock",
                batch_size=batch_size,
            )

            await create_excel_task_record(
                task_id=task.id,
                started_by=user_id,
                file_path="",
                task_type=ExcelTaskType.IMPORT,
            )

            return {"task_id": task.id}
        except Exception:
            if file_path.exists():
                os.remove(file_path)
            raise

    async def _import_excel_from_file(
        self,
        session: "AsyncSession",
        file_path: str,
        user_id: int,
        batch_size: int = 5000,
    ):
        try:
            return await import_sales_from_excel(
                session=session,
                file_path=file_path,
                user_id=user_id,
                batch_size=batch_size,
                model=self.model,
                import_log_model=ImportLogs,
                target_table="Третичные продажи",
                required_fields=sale.tertiary_sales_fields,
                mapping=tertiary_sales_mapping,
                key_fields=(
                    "pharmacy_id",
                    "sku_id",
                    "month",
                    "year",
                    "indicator",
                ),
                constraint_name="uq_tertiary_sales_business_key",
                relations=[
                    RelationSpec(
                        model=Pharmacy,
                        name_key="аптека",
                        missing_label="аптека",
                        id_field="pharmacy_id",
                    ),
                    RelationSpec(
                        model=Distributor,
                        name_key="дистрибьютор",
                        missing_label="дистрибьютор",
                        id_field="distributor_id",
                        required=True,
                    ),
                ],
                get_id_map=self.get_id_map,
            )
        finally:
            pass

    async def get_multi(
        self,
        session: "AsyncSession",
        filters: sale_schema.SecondaryTertiarySalesListRequest | None = None,
        load_options: list[Any] | None = None,
    ) -> PaginatedResponse[sale_schema.TertiarySalesResponse]:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        sort_map = {
            "pharmacy": self.model.pharmacy_id,
            "distributor": self.model.distributor_id,
            "brand": SKU.brand_id,
            "sku": self.model.sku_id,
            "month": self.model.month,
            "year": self.model.year,
            "indicator": self.model.indicator,
            "packages": self.model.packages,
            "amount": self.model.amount,
        }
        stmt = ListQueryHelper.apply_sorting_with_default(
            stmt,
            getattr(filters, "sort_by", None),
            getattr(filters, "sort_order", None),
            sort_map,
            self.model.created_at.desc(),
        )

        if filters:
            stmt = ListQueryHelper.apply_specs(
                stmt,
                [
                    InOrNullSpec(self.model.pharmacy_id, filters.pharmacy_ids),
                    InOrNullSpec(self.model.sku_id, filters.sku_ids),
                    InOrNullSpec(self.model.distributor_id, filters.distributor_ids),
                    InOrNullSpec(self.model.month, filters.months),
                    InOrNullSpec(self.model.quarter, filters.quarters),
                    NumberTypedSpec(self.model.year, filters.year),
                    NumberTypedSpec(self.model.packages, filters.packages),
                    NumberTypedSpec(self.model.amount, filters.amount),
                ],
            )

            joined_sku = False
            if filters.brand_ids:
                stmt = stmt.join(SKU, self.model.sku_id == SKU.id)
                joined_sku = True
                stmt = ListQueryHelper.apply_in_or_null(
                    stmt, SKU.brand_id, filters.brand_ids
                )

            if filters.indicators:
                raw = (
                    filters.indicators
                    if isinstance(filters.indicators, list)
                    else [filters.indicators]
                )

                stmt = stmt.where(
                    or_(*(self.model.indicator.ilike(f"%{v}%") for v in raw))
                )

            if filters.sort_by == "brands" and not joined_sku:
                stmt = stmt.join(SKU, self.model.sku_id == SKU.id)

            # Count before pagination
            count_stmt = select(func.count()).select_from(stmt.subquery())
            total_count = await session.scalar(count_stmt)

            stmt = ListQueryHelper.apply_pagination(stmt, filters.limit, filters.offset)
        else:
            count_stmt = select(func.count()).select_from(stmt.subquery())
            total_count = await session.scalar(count_stmt)

        result = await session.execute(stmt)

        items = result.unique().scalars().all()

        hasPrev = filters.offset > 0 if filters else False
        hasNext = len(items) == filters.limit if filters and filters.limit else False

        return PaginatedResponse(
            result=items,
            hasPrev=hasPrev,
            hasNext=hasNext,
            count=total_count,
        )

    async def iter_multi(
        self,
        session: "AsyncSession",
        load_options: list[Any] | None = None,
        chunk_size: int = 1000,
    ) -> AsyncIterator[ModelType]:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        stmt = ListQueryHelper.apply_sorting_with_created(
            stmt,
            self.model.created_at.desc(),
        )

        stream = await session.stream_scalars(
            stmt.execution_options(yield_per=chunk_size)
        )
        async for item in stream:
            yield item

    @staticmethod
    async def get_sales_report(
        session: "AsyncSession",
        filters: sale_schema.SecTerSalesReportFilter | None = None,
        company_id: int | None = None,
    ):
        period_key = build_period_key(filters.group_by_period, TertiarySalesAndStock)
        period_values = build_period_values(
            filters.group_by_period, filters.period_values
        )

        select_fields, group_by_fields, search_cols = build_dimensions(
            BASE_SALE_DIMENSTION_MAPPING_WITH_GEO_INDICATOR, filters.group_by_dimensions
        )

        indicator_filter = TertiarySalesAndStock.indicator.ilike("%продаж%")

        base_stmt = (
            select(
                *select_fields,
                period_key.label("period"),
                func.sum(TertiarySalesAndStock.packages).label("packages"),
                func.round(func.sum(TertiarySalesAndStock.amount)).label("amount"),
            )
            .select_from(TertiarySalesAndStock)
            .join(SKU, TertiarySalesAndStock.sku_id == SKU.id)
            .join(Brand, SKU.brand_id == Brand.id)
            .join(PromotionType, SKU.promotion_type_id == PromotionType.id)
            .join(ProductGroup, SKU.product_group_id == ProductGroup.id)
            .join(Pharmacy, TertiarySalesAndStock.pharmacy_id == Pharmacy.id)
            .outerjoin(
                Distributor, TertiarySalesAndStock.distributor_id == Distributor.id
            )
            .outerjoin(GeoIndicator, Pharmacy.geo_indicator_id == GeoIndicator.id)
            .where(indicator_filter)
        )

        if company_id is not None:
            base_stmt = base_stmt.where(SKU.company_id == company_id)

        base_stmt = ListQueryHelper.apply_specs(
            base_stmt,
            [
                InOrNullSpec(Brand.id, filters.brand_ids),
                InOrNullSpec(ProductGroup.id, filters.product_group_ids),
                InOrNullSpec(PromotionType.id, filters.promotion_type_ids),
                InOrNullSpec(
                    TertiarySalesAndStock.distributor_id, filters.distributor_ids
                ),
                InOrNullSpec(SKU.id, filters.sku_ids),
                InOrNullSpec(GeoIndicator.id, filters.geo_indicator_ids),
                InOrNullSpec(TertiarySalesAndStock.pharmacy_id, filters.pharmacy_ids),
                SearchSpec(
                    filters.search if filters.group_by_dimensions else None, search_cols
                ),
            ],
        )

        base_stmt = ListQueryHelper.apply_period_values(
            base_stmt,
            period_values,
            year_col=TertiarySalesAndStock.year,
            month_col=TertiarySalesAndStock.month,
            quarter_col=TertiarySalesAndStock.quarter,
        )

        group_by_fields.append(period_key)
        period_agg = base_stmt.group_by(*group_by_fields).cte("period_agg")

        final_select_fields = []
        final_group_by_fields = []

        if filters.group_by_dimensions:
            for dim in filters.group_by_dimensions:
                final_select_fields.extend(
                    [
                        getattr(period_agg.c, f"{dim}_id"),
                        getattr(period_agg.c, f"{dim}_name"),
                    ]
                )
                final_group_by_fields.extend(
                    [
                        getattr(period_agg.c, f"{dim}_id"),
                        getattr(period_agg.c, f"{dim}_name"),
                    ]
                )

        final_stmt = select(
            *final_select_fields,
            func.json_object_agg(
                period_agg.c.period,
                func.json_build_object(
                    "packages", period_agg.c.packages, "amount", period_agg.c.amount
                ),
            ).label("periods_data"),
        ).select_from(period_agg)

        if final_group_by_fields:
            final_stmt = final_stmt.group_by(*final_group_by_fields)

        sort_map = {
            "sku": getattr(period_agg.c, "sku_name", None),
            "brand": getattr(period_agg.c, "brand_name", None),
            "promotion": getattr(period_agg.c, "promotion_type_name", None),
            "product_group": getattr(period_agg.c, "product_group_name", None),
            "distributor": getattr(period_agg.c, "distributor_name", None),
            "geo_indicator": getattr(period_agg.c, "geo_indicator_name", None),
            "pharmacy": getattr(period_agg.c, "pharmacy_name", None),
        }

        final_stmt = ListQueryHelper.apply_sorting_with_default(
            final_stmt,
            getattr(filters, "sort_by", None),
            getattr(filters, "sort_order", None),
            sort_map,
        )

        final_stmt = ListQueryHelper.apply_pagination(
            final_stmt, filters.limit, filters.offset
        )

        result = await session.execute(final_stmt)
        return result.mappings().all()

    @staticmethod
    async def get_period_totals(
        session: "AsyncSession",
        filters: sale_schema.SecTerSalesPeriodFilter,
        company_id: int | None,
    ):
        period_key, period_columns = build_period_key(
            filters.group_by_period, TertiarySalesAndStock, with_group_fields=True
        )
        period_values = build_period_values(
            filters.group_by_period, filters.period_values
        )

        sales_packages = func.sum(
            case(
                (
                    TertiarySalesAndStock.indicator.ilike("%продаж%"),
                    TertiarySalesAndStock.packages,
                ),
                else_=0,
            )
        )
        sales_amount = func.sum(
            case(
                (
                    TertiarySalesAndStock.indicator.ilike("%продаж%"),
                    TertiarySalesAndStock.amount,
                ),
                else_=0,
            )
        )

        stmt = (
            select(
                period_key.label("period"),
                sales_packages.label("sales_packages"),
                func.round(sales_amount).label("sales_amount"),
            )
            .select_from(TertiarySalesAndStock)
            .join(SKU, TertiarySalesAndStock.sku_id == SKU.id)
        )

        if company_id is not None:
            stmt = stmt.where(SKU.company_id == company_id)

        need_pharmacy_join = bool(filters.geo_indicator_ids)

        if need_pharmacy_join:
            stmt = stmt.join(Pharmacy, TertiarySalesAndStock.pharmacy_id == Pharmacy.id)

        stmt = ListQueryHelper.apply_specs(
            stmt,
            [
                InOrNullSpec(
                    TertiarySalesAndStock.distributor_id, filters.distributor_ids
                ),
                (
                    InOrNullSpec(Pharmacy.geo_indicator_id, filters.geo_indicator_ids)
                    if need_pharmacy_join
                    else None
                ),
                InOrNullSpec(SKU.brand_id, filters.brand_ids),
                InOrNullSpec(SKU.product_group_id, filters.product_group_ids),
                InOrNullSpec(SKU.id, filters.sku_ids),
            ],
        )

        stmt = ListQueryHelper.apply_period_values(
            stmt,
            period_values,
            year_col=TertiarySalesAndStock.year,
            month_col=TertiarySalesAndStock.month,
            quarter_col=TertiarySalesAndStock.quarter,
        )

        stmt = (
            stmt.group_by(*period_columns)
            .having(sales_packages > 0)
            .order_by(period_key.desc())
        )

        result = await session.execute(stmt)
        return result.mappings().all()

    @staticmethod
    async def get_numeric_distribution(
        session: "AsyncSession",
        filters: sale_schema.NumericDistributionFilter | None = None,
        company_id: int | None = None,
        explain: bool = False,
    ):
        period_key = build_period_key(filters.group_by_period, TertiarySalesAndStock)
        period_values = build_period_values(
            filters.group_by_period, filters.period_values
        )
        total_pharmacies_cte = (
            select(
                period_key.label("period"),
                TertiarySalesAndStock.distributor_id.label("distributor_id"),
                Pharmacy.geo_indicator_id.label("geo_indicator_id"),
                func.count(func.distinct(TertiarySalesAndStock.pharmacy_id)).label(
                    "total_pharmacies"
                ),
            )
            .select_from(TertiarySalesAndStock)
            .join(Pharmacy, TertiarySalesAndStock.pharmacy_id == Pharmacy.id)
        )

        total_pharmacies_cte = ListQueryHelper.apply_period_values(
            total_pharmacies_cte,
            period_values,
            year_col=TertiarySalesAndStock.year,
            month_col=TertiarySalesAndStock.month,
            quarter_col=TertiarySalesAndStock.quarter,
        )

        total_pharmacies_cte = total_pharmacies_cte.group_by(
            TertiarySalesAndStock.distributor_id,
            Pharmacy.geo_indicator_id,
            period_key,
        ).cte("total_pharmacies")

        select_fields, group_by_fields, search_cols = build_dimensions(
            BASE_SALE_DIMENSTION_MAPPING_WITH_GEO_INDICATOR_AND_SEGMENT,
            filters.group_by_dimensions,
        )

        base_stmt = (
            select(
                *select_fields,
                period_key.label("period"),
                func.count(func.distinct(TertiarySalesAndStock.pharmacy_id)).label(
                    "pharmacies_with_sku"
                ),
                total_pharmacies_cte.c.total_pharmacies,
                func.cast(
                    (
                        func.count(
                            func.distinct(TertiarySalesAndStock.pharmacy_id)
                        ).cast(Float)
                        / func.nullif(total_pharmacies_cte.c.total_pharmacies, 0)
                        * 100
                    ),
                    Numeric(10, 2),
                ).label("nd_percent"),
            )
            .select_from(TertiarySalesAndStock)
            .join(SKU, TertiarySalesAndStock.sku_id == SKU.id)
            .join(Brand, SKU.brand_id == Brand.id)
            .join(ProductGroup, SKU.product_group_id == ProductGroup.id)
            .join(Segment, SKU.segment_id == Segment.id)
            .join(Pharmacy, TertiarySalesAndStock.pharmacy_id == Pharmacy.id)
            .outerjoin(
                Distributor, TertiarySalesAndStock.distributor_id == Distributor.id
            )
            .outerjoin(GeoIndicator, Pharmacy.geo_indicator_id == GeoIndicator.id)
            .join(
                total_pharmacies_cte,
                and_(
                    TertiarySalesAndStock.distributor_id
                    == total_pharmacies_cte.c.distributor_id,
                    func.coalesce(Pharmacy.geo_indicator_id, 0)
                    == func.coalesce(total_pharmacies_cte.c.geo_indicator_id, 0),
                    period_key == total_pharmacies_cte.c.period,
                ),
            )
            .where(
                TertiarySalesAndStock.indicator.ilike("%остат%"),
                TertiarySalesAndStock.packages > 0,
            )
        )

        if company_id is not None:
            base_stmt = base_stmt.where(SKU.company_id == company_id)

        base_stmt = ListQueryHelper.apply_specs(
            base_stmt,
            [
                InOrNullSpec(Brand.id, filters.brand_ids),
                InOrNullSpec(ProductGroup.id, filters.product_group_ids),
                InOrNullSpec(Segment.id, filters.segment_ids),
                InOrNullSpec(
                    TertiarySalesAndStock.distributor_id, filters.distributor_ids
                ),
                InOrNullSpec(SKU.id, filters.sku_ids),
                InOrNullSpec(GeoIndicator.id, filters.geo_indicator_ids),
                SearchSpec(
                    filters.search if filters.group_by_dimensions else None, search_cols
                ),
            ],
        )

        base_stmt = ListQueryHelper.apply_period_values(
            base_stmt,
            period_values,
            year_col=TertiarySalesAndStock.year,
            month_col=TertiarySalesAndStock.month,
            quarter_col=TertiarySalesAndStock.quarter,
        )

        group_by_fields.extend(
            [
                TertiarySalesAndStock.distributor_id,
                Pharmacy.geo_indicator_id,
                period_key,
                total_pharmacies_cte.c.total_pharmacies,
            ]
        )
        period_agg = base_stmt.group_by(*group_by_fields).cte("period_agg")

        final_select_fields = []
        final_group_by_fields = []

        if filters.group_by_dimensions:
            for dim in filters.group_by_dimensions:
                final_select_fields.extend(
                    [
                        getattr(period_agg.c, f"{dim}_id"),
                        getattr(period_agg.c, f"{dim}_name"),
                    ]
                )
                final_group_by_fields.extend(
                    [
                        getattr(period_agg.c, f"{dim}_id"),
                        getattr(period_agg.c, f"{dim}_name"),
                    ]
                )

        final_select_fields.append(period_agg.c.distributor_id)
        final_group_by_fields.append(period_agg.c.distributor_id)

        final_stmt = select(
            *final_select_fields,
            func.json_object_agg(
                period_agg.c.period,
                func.json_build_object("nd_percent", period_agg.c.nd_percent),
            ).label("periods_data"),
        )

        if final_group_by_fields:
            final_stmt = final_stmt.group_by(*final_group_by_fields)

        sort_map = {
            "sku": getattr(period_agg.c, "sku_name", None),
            "brand": getattr(period_agg.c, "brand_name", None),
            "promotion": getattr(period_agg.c, "promotion_type_name", None),
            "product_group": getattr(period_agg.c, "product_group_name", None),
            "distributor": getattr(period_agg.c, "distributor_name", None),
            "geo_indicator": getattr(period_agg.c, "geo_indicator_name", None),
        }

        final_stmt = ListQueryHelper.apply_sorting_with_default(
            final_stmt,
            getattr(filters, "sort_by", None),
            getattr(filters, "sort_order", None),
            sort_map,
        )

        final_stmt = ListQueryHelper.apply_pagination(
            final_stmt, filters.limit, filters.offset
        )

        if explain:
            from src.utils.explain_analyze import explain_analyze

            return await explain_analyze(session, final_stmt)

        result = await session.execute(final_stmt)
        return result.mappings().all()

    @staticmethod
    async def get_stock_report(
        session: "AsyncSession",
        filters: sale_schema.SecTerSalesReportFilter | None = None,
        company_id: int | None = None,
        explain: bool = False,
    ):
        period_key = build_period_key(filters.group_by_period, TertiarySalesAndStock)
        period_values = build_period_values(
            filters.group_by_period, filters.period_values
        )

        select_fields, group_by_fields, search_cols = build_dimensions(
            BASE_SALE_DIMENSTION_MAPPING_WITH_GEO_INDICATOR, filters.group_by_dimensions
        )

        indicator_filter = TertiarySalesAndStock.indicator.ilike("%остат%")

        base_stmt = (
            select(
                *select_fields,
                period_key.label("period"),
                func.sum(TertiarySalesAndStock.packages).label("packages"),
                func.round(func.sum(TertiarySalesAndStock.amount)).label("amount"),
            )
            .select_from(TertiarySalesAndStock)
            .join(SKU, TertiarySalesAndStock.sku_id == SKU.id)
            .join(Brand, SKU.brand_id == Brand.id)
            .join(PromotionType, SKU.promotion_type_id == PromotionType.id)
            .join(ProductGroup, SKU.product_group_id == ProductGroup.id)
            .join(Pharmacy, TertiarySalesAndStock.pharmacy_id == Pharmacy.id)
            .outerjoin(
                Distributor, TertiarySalesAndStock.distributor_id == Distributor.id
            )
            .outerjoin(GeoIndicator, Pharmacy.geo_indicator_id == GeoIndicator.id)
            .where(indicator_filter)
        )

        if company_id is not None:
            base_stmt = base_stmt.where(SKU.company_id == company_id)

        base_stmt = ListQueryHelper.apply_specs(
            base_stmt,
            [
                InOrNullSpec(Brand.id, filters.brand_ids),
                InOrNullSpec(ProductGroup.id, filters.product_group_ids),
                InOrNullSpec(PromotionType.id, filters.promotion_type_ids),
                InOrNullSpec(
                    TertiarySalesAndStock.distributor_id, filters.distributor_ids
                ),
                InOrNullSpec(SKU.id, filters.sku_ids),
                InOrNullSpec(GeoIndicator.id, filters.geo_indicator_ids),
                InOrNullSpec(TertiarySalesAndStock.pharmacy_id, filters.pharmacy_ids),
                SearchSpec(
                    filters.search if filters.group_by_dimensions else None, search_cols
                ),
            ],
        )

        base_stmt = ListQueryHelper.apply_period_values(
            base_stmt,
            period_values,
            year_col=TertiarySalesAndStock.year,
            month_col=TertiarySalesAndStock.month,
            quarter_col=TertiarySalesAndStock.quarter,
        )

        group_by_fields.append(period_key)
        period_agg = base_stmt.group_by(*group_by_fields).cte("period_agg")

        final_select_fields = []
        final_group_by_fields = []

        if filters.group_by_dimensions:
            for dim in filters.group_by_dimensions:
                final_select_fields.extend(
                    [
                        getattr(period_agg.c, f"{dim}_id"),
                        getattr(period_agg.c, f"{dim}_name"),
                    ]
                )
                final_group_by_fields.extend(
                    [
                        getattr(period_agg.c, f"{dim}_id"),
                        getattr(period_agg.c, f"{dim}_name"),
                    ]
                )

        final_stmt = select(
            *final_select_fields,
            func.json_object_agg(
                period_agg.c.period,
                func.json_build_object(
                    "packages", period_agg.c.packages, "amount", period_agg.c.amount
                ),
            ).label("periods_data"),
        ).select_from(period_agg)

        if final_group_by_fields:
            final_stmt = final_stmt.group_by(*final_group_by_fields)

        sort_map = {
            "sku": getattr(period_agg.c, "sku_name", None),
            "brand": getattr(period_agg.c, "brand_name", None),
            "promotion": getattr(period_agg.c, "promotion_type_name", None),
            "product_group": getattr(period_agg.c, "product_group_name", None),
            "distributor": getattr(period_agg.c, "distributor_name", None),
            "geo_indicator": getattr(period_agg.c, "geo_indicator_name", None),
            "pharmacy": getattr(period_agg.c, "pharmacy_name", None),
        }

        final_stmt = ListQueryHelper.apply_sorting_with_default(
            final_stmt,
            getattr(filters, "sort_by", None),
            getattr(filters, "sort_order", None),
            sort_map,
        )

        final_stmt = ListQueryHelper.apply_pagination(
            final_stmt, filters.limit, filters.offset
        )

        if explain:
            from src.utils.explain_analyze import explain_analyze

            return await explain_analyze(session, final_stmt)

        result = await session.execute(final_stmt)
        return result.mappings().all()
