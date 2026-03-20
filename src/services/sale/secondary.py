import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, AsyncIterator
from uuid import uuid4

from fastapi import HTTPException, status
from sqlalchemy import func, or_, select, update

from src.db.models import (
    SKU,
    Brand,
    Distributor,
    GeoIndicator,
    ImportLogs,
    Pharmacy,
    ProductGroup,
    PromotionType,
    SecondarySales,
)
from src.import_fields import sale
from src.mapping.dimension_mapping.sale import (
    BASE_SALE_DIMENSTION_MAPPING_WITH_GEO_INDICATOR,
)
from src.mapping.sales import secondary_sales_mapping
from src.schemas import sale as sale_schema
from src.schemas.base_filter import PaginatedResponse
from src.services.base import BaseService, ModelType
from src.services.sale.utils import RelationSpec, import_sales_from_excel
from src.utils.build_dimensions import build_dimensions
from src.utils.build_period_key import build_period_key
from src.utils.build_period_values import build_period_values
from src.utils.list_query_helper import (
    BoolListSpec,
    InOrNullSpec,
    ListQueryHelper,
    NumberTypedSpec,
    SearchSpec,
)

if TYPE_CHECKING:
    from fastapi import UploadFile
    from sqlalchemy.ext.asyncio import AsyncSession


class SecondarySalesService(
    BaseService[
        SecondarySales,
        sale_schema.SecondarySalesCreate,
        sale_schema.SecondarySalesUpdate,
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
                service_path="src.services.sale.SecondarySalesService",
                model_path="src.db.models.SecondarySales",
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
        batch_size: int = 2000,
    ):
        try:
            return await import_sales_from_excel(
                session=session,
                file_path=file_path,
                user_id=user_id,
                batch_size=batch_size,
                model=self.model,
                import_log_model=ImportLogs,
                target_table="Вторичные продажи",
                required_fields=sale.secondary_sales_fields,
                mapping=secondary_sales_mapping,
                key_fields=(
                    "pharmacy_id",
                    "sku_id",
                    "month",
                    "year",
                    "indicator",
                ),
                constraint_name="uq_secondary_sales_business_key",
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
    ) -> PaginatedResponse[sale_schema.SecondarySalesResponse]:
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
            "published": self.model.published,
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
                    BoolListSpec(
                        self.model.published,
                        filters.published,
                    ),
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
        for item in items:
            if item.distributor and item.pharmacy and item.pharmacy.distributor is None:
                item.pharmacy.distributor = item.distributor

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
        company_id: int | None,
        filters: sale_schema.SecTerSalesReportFilter | None = None,
    ):
        period_key = build_period_key(filters.group_by_period, SecondarySales)
        period_values = build_period_values(
            filters.group_by_period, filters.period_values
        )

        select_fields, group_by_fields, search_cols = build_dimensions(
            BASE_SALE_DIMENSTION_MAPPING_WITH_GEO_INDICATOR, filters.group_by_dimensions
        )

        base_stmt = (
            select(
                *select_fields,
                period_key.label("period"),
                func.sum(SecondarySales.packages).label("packages"),
                func.round(func.sum(SecondarySales.amount)).label("amount"),
            )
            .select_from(SecondarySales)
            .join(SKU, SecondarySales.sku_id == SKU.id)
            .join(Brand, SKU.brand_id == Brand.id)
            .join(PromotionType, SKU.promotion_type_id == PromotionType.id)
            .join(ProductGroup, SKU.product_group_id == ProductGroup.id)
            .join(Pharmacy, SecondarySales.pharmacy_id == Pharmacy.id)
            .outerjoin(Distributor, SecondarySales.distributor_id == Distributor.id)
            .outerjoin(GeoIndicator, Pharmacy.geo_indicator_id == GeoIndicator.id)
            .where(
                SecondarySales.indicator.ilike("%продаж%"),
            )
        )

        if company_id is not None:
            base_stmt = base_stmt.where(SKU.company_id == company_id)

        base_stmt = ListQueryHelper.apply_specs(
            base_stmt,
            [
                InOrNullSpec(Brand.id, filters.brand_ids),
                InOrNullSpec(ProductGroup.id, filters.product_group_ids),
                InOrNullSpec(PromotionType.id, filters.promotion_type_ids),
                InOrNullSpec(SecondarySales.distributor_id, filters.distributor_ids),
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
            year_col=SecondarySales.year,
            month_col=SecondarySales.month,
            quarter_col=SecondarySales.quarter,
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

        result = await session.execute(final_stmt)
        return result.mappings().all()

    @staticmethod
    async def get_period_totals(
        session: "AsyncSession",
        filters: sale_schema.SecTerSalesPeriodFilter,
        company_id: int | None,
    ):
        period_key, period_columns = build_period_key(
            filters.group_by_period, SecondarySales, with_group_fields=True
        )
        period_values = build_period_values(
            filters.group_by_period, filters.period_values
        )
        stmt = (
            select(
                period_key.label("period"),
                func.sum(SecondarySales.packages).label("packages"),
                func.round(func.sum(SecondarySales.amount)).label("sales"),
            )
            .select_from(SecondarySales)
            .join(SKU, SecondarySales.sku_id == SKU.id)
            .join(Pharmacy, SecondarySales.pharmacy_id == Pharmacy.id)
            .where(SecondarySales.indicator.ilike("%продаж%"))
        )

        if company_id is not None:
            stmt = stmt.where(SKU.company_id == company_id)

        stmt = ListQueryHelper.apply_specs(
            stmt,
            [
                InOrNullSpec(SKU.brand_id, filters.brand_ids),
                InOrNullSpec(SKU.product_group_id, filters.product_group_ids),
                InOrNullSpec(SecondarySales.distributor_id, filters.distributor_ids),
                InOrNullSpec(SKU.id, filters.sku_ids),
                InOrNullSpec(Pharmacy.geo_indicator_id, filters.geo_indicator_ids),
            ],
        )

        stmt = ListQueryHelper.apply_period_values(
            stmt,
            period_values,
            year_col=SecondarySales.year,
            month_col=SecondarySales.month,
            quarter_col=SecondarySales.quarter,
        )

        stmt = stmt.group_by(*period_columns).order_by(period_key.desc())

        result = await session.execute(stmt)
        return result.mappings().all()

    @staticmethod
    async def get_sales_by_distributor_report(
        session: "AsyncSession",
        company_id: int | None,
        filters: sale_schema.SalesByDistributorFilter,
    ):
        period_key = build_period_key(filters.group_by_period, SecondarySales)
        period_values = build_period_values(
            filters.group_by_period, filters.period_values
        )

        dimension_mapping = {
            "distributor": {
                "id": Distributor.id.label("distributor_id"),
                "name": Distributor.name.label("distributor_name"),
                "group_fields": [Distributor.id, Distributor.name],
            },
            "brand": {
                "id": Brand.id.label("brand_id"),
                "name": Brand.name.label("brand_name"),
                "group_fields": [Brand.id, Brand.name],
            },
            "product_group": {
                "id": ProductGroup.id.label("product_group_id"),
                "name": ProductGroup.name.label("product_group_name"),
                "group_fields": [ProductGroup.id, ProductGroup.name],
            },
        }

        select_fields = []
        group_by_fields = []

        if filters.group_by_dimensions:
            for dim in filters.group_by_dimensions:
                dim_config = dimension_mapping[dim]
                select_fields.extend([dim_config["id"], dim_config["name"]])
                group_by_fields.extend(dim_config["group_fields"])

        base_stmt = (
            select(
                *select_fields,
                period_key.label("period"),
                func.sum(SecondarySales.packages).label("total_packages"),
                func.round(func.sum(SecondarySales.amount)).label("total_amount"),
            )
            .select_from(SecondarySales)
            .outerjoin(Distributor, SecondarySales.distributor_id == Distributor.id)
            .join(SKU, SecondarySales.sku_id == SKU.id)
            .join(Brand, SKU.brand_id == Brand.id)
            .join(ProductGroup, SKU.product_group_id == ProductGroup.id)
        )

        if company_id is not None:
            base_stmt = base_stmt.where(SKU.company_id == company_id)

        base_stmt = ListQueryHelper.apply_period_values(
            base_stmt,
            period_values,
            year_col=SecondarySales.year,
            month_col=SecondarySales.month,
            quarter_col=SecondarySales.quarter,
        )

        if filters.distributor_ids:
            base_stmt = base_stmt.where(
                SecondarySales.distributor_id.in_(filters.distributor_ids)
            )

        if filters.brand_ids:
            base_stmt = base_stmt.where(Brand.id.in_(filters.brand_ids))

        if filters.product_group_ids:
            base_stmt = base_stmt.where(ProductGroup.id.in_(filters.product_group_ids))

        if filters.search and filters.group_by_dimensions:
            search_term = f"%{filters.search}%"
            search_conditions = []
            if "distributor" in filters.group_by_dimensions:
                search_conditions.append(Distributor.name.ilike(search_term))
            if "brand" in filters.group_by_dimensions:
                search_conditions.append(Brand.name.ilike(search_term))
            if "product_group" in filters.group_by_dimensions:
                search_conditions.append(ProductGroup.name.ilike(search_term))

            if search_conditions:
                base_stmt = base_stmt.where(or_(*search_conditions))

        group_by_fields.append(period_key)
        base_stmt = base_stmt.group_by(*group_by_fields).cte("period_agg")

        final_select_fields = []
        final_group_by_fields = []

        if filters.group_by_dimensions:
            for dim in filters.group_by_dimensions:
                final_select_fields.extend(
                    [
                        getattr(base_stmt.c, f"{dim}_id"),
                        getattr(base_stmt.c, f"{dim}_name"),
                    ]
                )
                final_group_by_fields.extend(
                    [
                        getattr(base_stmt.c, f"{dim}_id"),
                        getattr(base_stmt.c, f"{dim}_name"),
                    ]
                )

        final_stmt = select(
            *final_select_fields,
            func.json_object_agg(
                base_stmt.c.period,
                func.json_build_object(
                    "total_packages",
                    base_stmt.c.total_packages,
                    "total_amount",
                    base_stmt.c.total_amount,
                ),
            ).label("periods_data"),
        )

        if final_group_by_fields:
            final_stmt = final_stmt.group_by(*final_group_by_fields)

        sort_map = {
            "sku": getattr(base_stmt.c, "sku_name", None),
            "brand": getattr(base_stmt.c, "brand_name", None),
            "promotion": getattr(base_stmt.c, "promotion_type_name", None),
            "product_group": getattr(base_stmt.c, "product_group_name", None),
            "distributor": getattr(base_stmt.c, "distributor_name", None),
            "geo_indicator": getattr(base_stmt.c, "geo_indicator_name", None),
        }

        final_stmt = ListQueryHelper.apply_sorting_with_default(
            final_stmt,
            filters.sort_by,
            filters.sort_order,
            sort_map,
        )

        final_stmt = ListQueryHelper.apply_pagination(
            final_stmt, filters.limit, filters.offset
        )

        result = await session.execute(final_stmt)
        return result.mappings().all()

    @staticmethod
    async def get_total_sales_by_distributor(
        session: "AsyncSession",
        company_id: int | None,
        filters: sale_schema.ChartSalesByDistributorFilter | None = None,
    ):
        period_key = build_period_key(filters.group_by_period, SecondarySales)
        period_values = build_period_values(
            filters.group_by_period, filters.period_values
        )

        base_stmt = (
            select(
                Distributor.id.label("distributor_id"),
                Distributor.name.label("distributor_name"),
                period_key.label("period"),
                func.sum(SecondarySales.packages).label("total_packages"),
                func.round(func.sum(SecondarySales.amount)).label("total_amount"),
            )
            .select_from(SecondarySales)
            .outerjoin(Distributor, SecondarySales.distributor_id == Distributor.id)
            .join(SKU, SecondarySales.sku_id == SKU.id)
        )

        base_stmt = ListQueryHelper.apply_specs(
            base_stmt,
            [
                InOrNullSpec(SKU.brand_id, filters.brand_ids),
                InOrNullSpec(SKU.product_group_id, filters.product_group_ids),
                InOrNullSpec(SecondarySales.distributor_id, filters.distributor_ids),
                InOrNullSpec(Pharmacy.geo_indicator_id, filters.geo_indicator_ids),
            ],
        )

        base_stmt = ListQueryHelper.apply_period_values(
            base_stmt,
            period_values,
            year_col=SecondarySales.year,
            month_col=SecondarySales.month,
            quarter_col=SecondarySales.quarter,
        )

        base_stmt = base_stmt.group_by(
            Distributor.id, Distributor.name, period_key
        ).cte("base_agg")

        final_stmt = select(
            base_stmt.c.distributor_id,
            base_stmt.c.distributor_name,
            func.json_object_agg(
                base_stmt.c.period,
                func.json_build_object(
                    "total_packages",
                    base_stmt.c.total_packages,
                    "total_amount",
                    base_stmt.c.total_amount,
                ),
            ).label("periods_data"),
        ).group_by(base_stmt.c.distributor_id, base_stmt.c.distributor_name)

        result = await session.execute(final_stmt)
        return result.mappings().all()

    @staticmethod
    async def get_unpublish(session: "AsyncSession") -> ModelType:
        stmt = select(SecondarySales).where(~SecondarySales.published)
        result = await session.execute(stmt)

        return result.scalars().all()

    @staticmethod
    async def publish_unpublished(
        session: "AsyncSession", ids: list[int], batch_size: int = 1000
    ) -> list[dict[str, int | bool]]:
        if not ids:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Список ids пуст"
            )

        updated_items: list[dict[str, int | bool]] = []
        for start in range(0, len(ids), batch_size):
            batch_ids = ids[start : start + batch_size]
            stmt = (
                update(SecondarySales)
                .where(
                    SecondarySales.id.in_(batch_ids),
                    SecondarySales.published.is_(False),
                )
                .values(published=True)
                .returning(SecondarySales.id, SecondarySales.published)
            )
            result = await session.execute(stmt)
            updated_items.extend(result.mappings().all())

        if not updated_items:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Нет неопубликованных записей для указанных ids",
            )

        await session.commit()
        return updated_items
