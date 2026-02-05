import asyncio
from typing import TYPE_CHECKING, Any, Sequence

from fastapi import HTTPException, status
from sqlalchemy import (
    Float,
    Numeric,
    String,
    and_,
    case,
    cast,
    func,
    insert,
    or_,
    select,
    update,
)

from src.db.models import (
    SKU,
    Brand,
    Distributor,
    District,
    Employee,
    GeoIndicator,
    ImportLogs,
    Pharmacy,
    PrimarySalesAndStock,
    ProductGroup,
    PromotionType,
    SecondarySales,
    Segment,
    Settlement,
    TertiarySalesAndStock,
)
from src.mapping.sales import (
    primary_sales_mapping,
    secondary_sales_mapping,
    tertiary_sales_mapping,
)
from src.schemas import sale
from src.utils.excel_parser import iter_excel_records, save_upload_to_temp
from src.utils.mapping import map_record

from .base import BaseService, ModelType

if TYPE_CHECKING:
    from fastapi import UploadFile
    from sqlalchemy.ext.asyncio import AsyncSession


class PrimarySalesAndStockService(
    BaseService[
        PrimarySalesAndStock,
        sale.PrimarySalesAndStockCreate,
        sale.PrimarySalesAndStockUpdate,
    ]
):
    async def import_sales(
        self,
        session: "AsyncSession",
        file: "UploadFile",
        user_id: int,
        batch_size: int = 2000,
    ):
        temp = await save_upload_to_temp(file)
        try:
            distributor_names: set[str] = set()
            sku_names: set[str] = set()
            total_records = 0

            for _, record in iter_excel_records(temp):
                total_records += 1
                distributor_name = record.get("дистрибьютор")
                sku_name = record.get("sku")
                if distributor_name:
                    distributor_names.add(distributor_name)
                if sku_name:
                    sku_names.add(sku_name)

            import_log = ImportLogs(
                uploaded_by=user_id,
                target_table="Первичные продажи",
                records_count=total_records,
            )
            session.add(import_log)
            await session.flush()

            results = await asyncio.gather(
                self.get_id_map(session, Distributor, "name", distributor_names),
                self.get_id_map(session, SKU, "name", sku_names),
                return_exceptions=True,
            )

            distributor_map, missing_distributors = results[0]
            sku_map, missing_skus = results[1]

            skipped_records = []
            data_to_insert = []
            imported = 0

            for row_index, record in iter_excel_records(temp):
                missing_keys = []
                distributor_name = record.get("дистрибьютор")
                sku_name = record.get("sku")
                month_value = record.get("месяц")
                record["квартал"] = (
                    (int(month_value) - 1) // 3 + 1 if month_value else None
                )

                if distributor_name in missing_distributors:
                    missing_keys.append(f"дистрибьютор: {distributor_name}")

                if sku_name in missing_skus:
                    missing_keys.append(f"SKU: {sku_name}")

                if missing_keys:
                    skipped_records.append({"row": row_index, "missing": missing_keys})
                    continue

                relation_fields = {
                    "distributor_id": distributor_map[distributor_name],
                    "sku_id": sku_map[sku_name],
                    "import_log_id": import_log.id,
                }
                data_to_insert.append(
                    map_record(record, primary_sales_mapping, relation_fields)
                )

                if len(data_to_insert) >= batch_size:
                    await session.execute(insert(self.model), data_to_insert)
                    imported += len(data_to_insert)
                    data_to_insert = []

            if data_to_insert:
                await session.execute(insert(self.model), data_to_insert)
                imported += len(data_to_insert)

            await session.commit()

            return {
                "imported": imported,
                "skipped": len(skipped_records),
                "total": total_records,
                "skipped_records": skipped_records,
            }
        finally:
            temp.close()

    async def get_multi(
        self,
        session: "AsyncSession",
        filters: sale.PrimarySalesAndStockFilter | None = None,
        load_options: list[Any] | None = None,
    ) -> Sequence[ModelType]:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        if filters.published:
            stmt = stmt.where(self.model.published)
        if filters.years:
            stmt = stmt.where(self.model.year.in_(filters.years))
        if filters.quarters:
            stmt = stmt.where(self.model.quarter.in_(filters.quarters))
        if filters.months:
            stmt = stmt.where(self.model.month.in_(filters.months))
        if filters.distributor_ids:
            stmt = stmt.where(self.model.distributor_id.in_(filters.distributor_ids))
        if filters.sku_ids:
            stmt = stmt.where(self.model.sku_id.in_(filters.sku_ids))
        if filters.indicator:
            stmt = stmt.where(self.model.indicator == filters.indicator)

        stmt = stmt.order_by(self.model.created_at.desc())
        stmt = stmt.limit(filters.limit).offset(filters.offset)

        result = await session.execute(stmt)

        if result is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Продажи не найдены"
            )
        return result.scalars().all()

    @staticmethod
    async def get_shipment_stock_report(
        session: "AsyncSession",
        filters: sale.ShipmentStockFilter,
        indicator: str,
        company_id: int | None,
    ):
        if filters.group_by_period == "year":
            period_key = cast(PrimarySalesAndStock.year, String)
        elif filters.group_by_period == "quarter":
            period_key = func.concat(
                cast(PrimarySalesAndStock.year, String),
                "-Q",
                cast(PrimarySalesAndStock.quarter, String),
            )
        else:
            period_key = func.concat(
                cast(PrimarySalesAndStock.year, String),
                "-",
                func.lpad(cast(PrimarySalesAndStock.month, String), 2, "0"),
            )

        dimension_mapping = {
            "sku": {
                "id": SKU.id.label("sku_id"),
                "name": SKU.name.label("sku_name"),
                "group_fields": [SKU.id, SKU.name],
            },
            "brand": {
                "id": Brand.id.label("brand_id"),
                "name": Brand.name.label("brand_name"),
                "group_fields": [Brand.id, Brand.name],
            },
            "promotion_type": {
                "id": PromotionType.id.label("promotion_type_id"),
                "name": PromotionType.name.label("promotion_type_name"),
                "group_fields": [PromotionType.id, PromotionType.name],
            },
            "product_group": {
                "id": ProductGroup.id.label("product_group_id"),
                "name": ProductGroup.name.label("product_group_name"),
                "group_fields": [ProductGroup.id, ProductGroup.name],
            },
            "distributor": {
                "id": Distributor.id.label("distributor_id"),
                "name": Distributor.name.label("distributor_name"),
                "group_fields": [Distributor.id, Distributor.name],
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
                func.sum(PrimarySalesAndStock.packages).label("packages"),
                func.round(func.sum(PrimarySalesAndStock.amount)).label("amount"),
            )
            .select_from(PrimarySalesAndStock)
            .join(SKU, PrimarySalesAndStock.sku_id == SKU.id)
            .join(Brand, SKU.brand_id == Brand.id)
            .join(PromotionType, SKU.promotion_type_id == PromotionType.id)
            .join(ProductGroup, SKU.product_group_id == ProductGroup.id)
            .join(Distributor, PrimarySalesAndStock.distributor_id == Distributor.id)
            .where(
                PrimarySalesAndStock.indicator == indicator,
                PrimarySalesAndStock.year.in_(filters.years),
            )
        )

        if company_id is not None:
            base_stmt = base_stmt.where(SKU.company_id == company_id)

        if filters.months:
            base_stmt = base_stmt.where(PrimarySalesAndStock.month.in_(filters.months))

        if filters.quarters:
            base_stmt = base_stmt.where(
                PrimarySalesAndStock.quarter.in_(filters.quarters)
            )

        if filters.brand_ids:
            base_stmt = base_stmt.where(Brand.id.in_(filters.brand_ids))

        if filters.group_ids:
            base_stmt = base_stmt.where(ProductGroup.id.in_(filters.group_ids))

        if filters.promo_type_id:
            base_stmt = base_stmt.where(PromotionType.id == filters.promo_type_id)

        if filters.distributor_ids:
            base_stmt = base_stmt.where(Distributor.id.in_(filters.distributor_ids))

        if filters.sku_ids:
            base_stmt = base_stmt.where(SKU.id.in_(filters.sku_ids))

        if filters.search and filters.group_by_dimensions:
            search_term = f"%{filters.search}%"
            search_conditions = []
            if "sku" in filters.group_by_dimensions:
                search_conditions.append(SKU.name.ilike(search_term))
            if "brand" in filters.group_by_dimensions:
                search_conditions.append(Brand.name.ilike(search_term))
            if "promotion_type" in filters.group_by_dimensions:
                search_conditions.append(PromotionType.name.ilike(search_term))
            if "product_group" in filters.group_by_dimensions:
                search_conditions.append(ProductGroup.name.ilike(search_term))
            if "distributor" in filters.group_by_dimensions:
                search_conditions.append(Distributor.name.ilike(search_term))

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
                    "packages", base_stmt.c.packages, "amount", base_stmt.c.amount
                ),
            ).label("periods_data"),
        )

        if final_group_by_fields:
            final_stmt = final_stmt.group_by(*final_group_by_fields)

        if filters.limit:
            final_stmt = final_stmt.limit(filters.limit)
        if filters.offset:
            final_stmt = final_stmt.offset(filters.offset)

        result = await session.execute(final_stmt)
        return result.mappings().all()

    @staticmethod
    async def get_period_totals(
        session: "AsyncSession",
        filters: sale.PeriodFilter,
        company_id: int | None,
    ):
        if filters.group_by_period == "year":
            period_key = cast(PrimarySalesAndStock.year, String)
        elif filters.group_by_period == "quarter":
            period_key = func.concat(
                cast(PrimarySalesAndStock.year, String),
                "-Q",
                cast(PrimarySalesAndStock.quarter, String),
            )
        else:
            period_key = func.concat(
                cast(PrimarySalesAndStock.year, String),
                "-",
                func.lpad(cast(PrimarySalesAndStock.month, String), 2, "0"),
            )

        if filters.group_by_period == "quarter":
            sales_divisor = func.nullif(
                func.sum(
                    case(
                        (
                            PrimarySalesAndStock.indicator == "Первичная продажа",
                            PrimarySalesAndStock.packages,
                        ),
                        else_=0,
                    )
                )
                / 3.0,
                0,
            )
        elif filters.group_by_period == "year":
            sales_divisor = func.nullif(
                func.sum(
                    case(
                        (
                            PrimarySalesAndStock.indicator == "Первичная продажа",
                            PrimarySalesAndStock.packages,
                        ),
                        else_=0,
                    )
                )
                / 12.0,
                0,
            )
        else:
            sales_divisor = func.nullif(
                func.sum(
                    case(
                        (
                            PrimarySalesAndStock.indicator == "Первичная продажа",
                            PrimarySalesAndStock.packages,
                        ),
                        else_=0,
                    )
                ),
                0,
            )

        select_cols = [
            period_key.label("period"),
            func.cast(
                case(
                    (
                        func.sum(
                            case(
                                (
                                    PrimarySalesAndStock.indicator
                                    == "Первичная продажа",
                                    PrimarySalesAndStock.packages,
                                ),
                                else_=0,
                            )
                        )
                        > 0,
                        func.sum(
                            case(
                                (
                                    PrimarySalesAndStock.indicator
                                    == "Остаток на складе",
                                    PrimarySalesAndStock.packages,
                                ),
                                else_=0,
                            )
                        )
                        / sales_divisor,
                    ),
                    else_=None,
                ),
                Numeric(10, 2),
            ).label("coverage_months"),
        ]

        if filters.group_by_period not in ("quarter", "year"):
            select_cols.extend(
                [
                    func.sum(
                        case(
                            (
                                PrimarySalesAndStock.indicator == "Остаток на складе",
                                PrimarySalesAndStock.packages,
                            ),
                            else_=0,
                        )
                    ).label("stock_packages"),
                    func.round(
                        func.sum(
                            case(
                                (
                                    PrimarySalesAndStock.indicator
                                    == "Остаток на складе",
                                    PrimarySalesAndStock.amount,
                                ),
                                else_=0,
                            )
                        )
                    ).label("stock_amount"),
                ]
            )

        select_cols.extend(
            [
                func.sum(
                    case(
                        (
                            PrimarySalesAndStock.indicator == "Первичная продажа",
                            PrimarySalesAndStock.packages,
                        ),
                        else_=0,
                    )
                ).label("sales_packages"),
                func.round(
                    func.sum(
                        case(
                            (
                                PrimarySalesAndStock.indicator == "Первичная продажа",
                                PrimarySalesAndStock.amount,
                            ),
                            else_=0,
                        )
                    )
                ).label("sales_amount"),
            ]
        )

        stmt = (
            select(*select_cols)
            .select_from(PrimarySalesAndStock)
            .join(SKU, PrimarySalesAndStock.sku_id == SKU.id)
            .where(
                PrimarySalesAndStock.indicator.in_(
                    ["Остаток на складе", "Первичная продажа"]
                ),
                PrimarySalesAndStock.year.in_(filters.years),
            )
        )

        if company_id is not None:
            stmt = stmt.where(SKU.company_id == company_id)

        if filters.months:
            stmt = stmt.where(PrimarySalesAndStock.month.in_(filters.months))

        if filters.quarters:
            stmt = stmt.where(PrimarySalesAndStock.quarter.in_(filters.quarters))

        if filters.brand_ids:
            stmt = stmt.where(SKU.brand_id.in_(filters.brand_ids))

        if filters.product_group_ids:
            stmt = stmt.where(SKU.product_group_id.in_(filters.product_group_ids))

        if filters.distributor_ids:
            stmt = stmt.where(
                PrimarySalesAndStock.distributor_id.in_(filters.distributor_ids)
            )

        if filters.sku_ids:
            stmt = stmt.where(SKU.id.in_(filters.sku_ids))

        stmt = stmt.group_by(period_key).order_by(period_key.desc())

        if filters.group_by_period not in ("year", "quarter"):
            stmt = stmt.having(
                or_(
                    func.sum(
                        case(
                            (
                                PrimarySalesAndStock.indicator == "Остаток на складе",
                                PrimarySalesAndStock.packages,
                            ),
                            else_=0,
                        )
                    )
                    > 0,
                    func.sum(
                        case(
                            (
                                PrimarySalesAndStock.indicator == "Первичная продажа",
                                PrimarySalesAndStock.packages,
                            ),
                            else_=0,
                        )
                    )
                    > 0,
                )
            )
        else:
            stmt = stmt.having(
                func.sum(
                    case(
                        (
                            PrimarySalesAndStock.indicator == "Первичная продажа",
                            PrimarySalesAndStock.packages,
                        ),
                        else_=0,
                    )
                )
                > 0
            )

        result = await session.execute(stmt)
        return result.mappings().all()

    @staticmethod
    async def get_stock_coverage(
        session: "AsyncSession",
        filters: sale.StockCoverageFilter,
        company_id: int | None,
    ):
        if filters.group_by_period == "year":
            period_key = cast(PrimarySalesAndStock.year, String)
        elif filters.group_by_period == "quarter":
            period_key = func.concat(
                cast(PrimarySalesAndStock.year, String),
                "-Q",
                cast(PrimarySalesAndStock.quarter, String),
            )
        else:
            period_key = func.concat(
                cast(PrimarySalesAndStock.year, String),
                "-",
                func.lpad(cast(PrimarySalesAndStock.month, String), 2, "0"),
            )

        dimension_mapping = {
            "sku": {
                "id": SKU.id.label("sku_id"),
                "name": SKU.name.label("sku_name"),
                "group_fields": [SKU.id, SKU.name],
            },
            "brand": {
                "id": Brand.id.label("brand_id"),
                "name": Brand.name.label("brand_name"),
                "group_fields": [Brand.id, Brand.name],
            },
            "promotion_type": {
                "id": PromotionType.id.label("promotion_type_id"),
                "name": PromotionType.name.label("promotion_type_name"),
                "group_fields": [PromotionType.id, PromotionType.name],
            },
            "product_group": {
                "id": ProductGroup.id.label("product_group_id"),
                "name": ProductGroup.name.label("product_group_name"),
                "group_fields": [ProductGroup.id, ProductGroup.name],
            },
            "distributor": {
                "id": Distributor.id.label("distributor_id"),
                "name": Distributor.name.label("distributor_name"),
                "group_fields": [Distributor.id, Distributor.name],
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
                func.cast(
                    case(
                        (
                            func.sum(
                                case(
                                    (
                                        PrimarySalesAndStock.indicator
                                        == "Первичная продажа",
                                        PrimarySalesAndStock.packages,
                                    ),
                                    else_=0,
                                )
                            )
                            > 0,
                            func.sum(
                                case(
                                    (
                                        PrimarySalesAndStock.indicator
                                        == "Остаток на складе",
                                        PrimarySalesAndStock.packages,
                                    ),
                                    else_=0,
                                )
                            )
                            / func.nullif(
                                func.sum(
                                    case(
                                        (
                                            PrimarySalesAndStock.indicator
                                            == "Первичная продажа",
                                            PrimarySalesAndStock.packages,
                                        ),
                                        else_=0,
                                    )
                                ),
                                0,
                            ),
                        ),
                        else_=0,
                    ),
                    Numeric(10, 2),
                ).label("coverage_months"),
            )
            .select_from(PrimarySalesAndStock)
            .join(SKU, PrimarySalesAndStock.sku_id == SKU.id)
            .join(Brand, SKU.brand_id == Brand.id)
            .join(PromotionType, SKU.promotion_type_id == PromotionType.id)
            .join(ProductGroup, SKU.product_group_id == ProductGroup.id)
            .join(Distributor, PrimarySalesAndStock.distributor_id == Distributor.id)
            .where(
                PrimarySalesAndStock.indicator.in_(
                    ["Остаток на складе", "Первичная продажа"]
                ),
                PrimarySalesAndStock.year.in_(filters.years),
            )
        )

        if company_id is not None:
            base_stmt = base_stmt.where(SKU.company_id == company_id)

        if filters.months:
            base_stmt = base_stmt.where(PrimarySalesAndStock.month.in_(filters.months))

        if filters.quarters:
            base_stmt = base_stmt.where(
                PrimarySalesAndStock.quarter.in_(filters.quarters)
            )

        if filters.brand_ids:
            base_stmt = base_stmt.where(Brand.id.in_(filters.brand_ids))

        if filters.group_ids:
            base_stmt = base_stmt.where(ProductGroup.id.in_(filters.group_ids))

        if filters.promo_type_id:
            base_stmt = base_stmt.where(PromotionType.id == filters.promo_type_id)

        if filters.distributor_ids:
            base_stmt = base_stmt.where(Distributor.id.in_(filters.distributor_ids))

        if filters.sku_ids:
            base_stmt = base_stmt.where(SKU.id.in_(filters.sku_ids))

        if filters.search and filters.group_by_dimensions:
            search_term = f"%{filters.search}%"
            search_conditions = []
            if "sku" in filters.group_by_dimensions:
                search_conditions.append(SKU.name.ilike(search_term))
            if "brand" in filters.group_by_dimensions:
                search_conditions.append(Brand.name.ilike(search_term))
            if "promotion_type" in filters.group_by_dimensions:
                search_conditions.append(PromotionType.name.ilike(search_term))
            if "product_group" in filters.group_by_dimensions:
                search_conditions.append(ProductGroup.name.ilike(search_term))
            if "distributor" in filters.group_by_dimensions:
                search_conditions.append(Distributor.name.ilike(search_term))

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
                func.json_build_object("coverage_months", base_stmt.c.coverage_months),
            ).label("periods_data"),
        )

        if final_group_by_fields:
            final_stmt = final_stmt.group_by(*final_group_by_fields)

        if filters.limit:
            final_stmt = final_stmt.limit(filters.limit)
        if filters.offset:
            final_stmt = final_stmt.offset(filters.offset)

        result = await session.execute(final_stmt)
        return result.mappings().all()

    @staticmethod
    async def get_distributor_share_report(
        session: "AsyncSession",
        filters: sale.DistributorShareFilter,
        company_id: int | None,
    ):
        if filters.group_by_period == "year":
            period_key = cast(PrimarySalesAndStock.year, String)
        elif filters.group_by_period == "quarter":
            period_key = func.concat(
                cast(PrimarySalesAndStock.year, String),
                "-Q",
                cast(PrimarySalesAndStock.quarter, String),
            )
        else:
            period_key = func.concat(
                cast(PrimarySalesAndStock.year, String),
                "-",
                func.lpad(cast(PrimarySalesAndStock.month, String), 2, "0"),
            )

        dimension_mapping = {
            "sku": {
                "id": SKU.id.label("sku_id"),
                "name": SKU.name.label("sku_name"),
                "group_fields": [SKU.id, SKU.name],
            },
            "brand": {
                "id": Brand.id.label("brand_id"),
                "name": Brand.name.label("brand_name"),
                "group_fields": [Brand.id, Brand.name],
            },
            "promotion_type": {
                "id": PromotionType.id.label("promotion_type_id"),
                "name": PromotionType.name.label("promotion_type_name"),
                "group_fields": [PromotionType.id, PromotionType.name],
            },
            "product_group": {
                "id": ProductGroup.id.label("product_group_id"),
                "name": ProductGroup.name.label("product_group_name"),
                "group_fields": [ProductGroup.id, ProductGroup.name],
            },
            "distributor": {
                "id": Distributor.id.label("distributor_id"),
                "name": Distributor.name.label("distributor_name"),
                "group_fields": [Distributor.id, Distributor.name],
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
                func.round(func.sum(PrimarySalesAndStock.amount)).label("amount"),
            )
            .select_from(PrimarySalesAndStock)
            .join(SKU, PrimarySalesAndStock.sku_id == SKU.id)
            .join(Brand, SKU.brand_id == Brand.id)
            .join(PromotionType, SKU.promotion_type_id == PromotionType.id)
            .join(ProductGroup, SKU.product_group_id == ProductGroup.id)
            .join(Distributor, PrimarySalesAndStock.distributor_id == Distributor.id)
            .where(
                PrimarySalesAndStock.indicator == "Первичная продажа",
                PrimarySalesAndStock.year.in_(filters.years),
            )
        )

        if company_id is not None:
            base_stmt = base_stmt.where(SKU.company_id == company_id)

        if filters.months:
            base_stmt = base_stmt.where(PrimarySalesAndStock.month.in_(filters.months))

        if filters.quarters:
            base_stmt = base_stmt.where(
                PrimarySalesAndStock.quarter.in_(filters.quarters)
            )

        if filters.brand_ids:
            base_stmt = base_stmt.where(Brand.id.in_(filters.brand_ids))

        if filters.group_ids:
            base_stmt = base_stmt.where(ProductGroup.id.in_(filters.group_ids))

        if filters.promo_type_id:
            base_stmt = base_stmt.where(PromotionType.id == filters.promo_type_id)

        if filters.distributor_ids:
            base_stmt = base_stmt.where(Distributor.id.in_(filters.distributor_ids))

        if filters.sku_ids:
            base_stmt = base_stmt.where(SKU.id.in_(filters.sku_ids))

        if filters.search and filters.group_by_dimensions:
            search_term = f"%{filters.search}%"
            search_conditions = []
            if "sku" in filters.group_by_dimensions:
                search_conditions.append(SKU.name.ilike(search_term))
            if "brand" in filters.group_by_dimensions:
                search_conditions.append(Brand.name.ilike(search_term))
            if "promotion_type" in filters.group_by_dimensions:
                search_conditions.append(PromotionType.name.ilike(search_term))
            if "product_group" in filters.group_by_dimensions:
                search_conditions.append(ProductGroup.name.ilike(search_term))
            if "distributor" in filters.group_by_dimensions:
                search_conditions.append(Distributor.name.ilike(search_term))

            if search_conditions:
                base_stmt = base_stmt.where(or_(*search_conditions))

        group_by_fields.append(period_key)
        base_stmt = base_stmt.group_by(*group_by_fields).cte("period_agg")

        period_totals = (
            select(
                base_stmt.c.period,
                func.round(func.sum(base_stmt.c.amount)).label("total_amount"),
            ).group_by(base_stmt.c.period)
        ).cte("period_totals")

        percentage_select_fields = []
        if filters.group_by_dimensions:
            for dim in filters.group_by_dimensions:
                percentage_select_fields.extend(
                    [
                        getattr(base_stmt.c, f"{dim}_id"),
                        getattr(base_stmt.c, f"{dim}_name"),
                    ]
                )

        with_percentages = (
            select(
                *percentage_select_fields,
                base_stmt.c.period,
                base_stmt.c.amount,
                func.round(
                    case(
                        (
                            period_totals.c.total_amount > 0,
                            (base_stmt.c.amount * 100.0)
                            / func.nullif(period_totals.c.total_amount, 0),
                        ),
                        else_=0,
                    )
                ).label("share_percent"),
            )
            .select_from(base_stmt)
            .join(period_totals, base_stmt.c.period == period_totals.c.period)
            .where(
                func.round(
                    case(
                        (
                            period_totals.c.total_amount > 0,
                            (base_stmt.c.amount * 100.0)
                            / func.nullif(period_totals.c.total_amount, 0),
                        ),
                        else_=0,
                    )
                )
                != 0
            )
        ).cte("with_percentages")

        final_select_fields = []
        final_group_by_fields = []

        if filters.group_by_dimensions:
            for dim in filters.group_by_dimensions:
                final_select_fields.extend(
                    [
                        getattr(with_percentages.c, f"{dim}_id"),
                        getattr(with_percentages.c, f"{dim}_name"),
                    ]
                )
                final_group_by_fields.extend(
                    [
                        getattr(with_percentages.c, f"{dim}_id"),
                        getattr(with_percentages.c, f"{dim}_name"),
                    ]
                )

        final_stmt = select(
            *final_select_fields,
            func.json_object_agg(
                with_percentages.c.period,
                func.json_build_object(
                    "amount",
                    with_percentages.c.amount,
                    "share_percent",
                    with_percentages.c.share_percent,
                ),
            ).label("periods_data"),
        )

        if final_group_by_fields:
            final_stmt = final_stmt.group_by(*final_group_by_fields)

        if filters.limit:
            final_stmt = final_stmt.limit(filters.limit)
        if filters.offset:
            final_stmt = final_stmt.offset(filters.offset)

        result = await session.execute(final_stmt)
        return result.mappings().all()

    @staticmethod
    async def get_distributor_share_chart(
        session: "AsyncSession",
        filters: sale.SalesReportFilter,
        company_id: int | None,
    ):
        if filters.group_by_period == "year":
            period_key = cast(PrimarySalesAndStock.year, String)
        elif filters.group_by_period == "quarter":
            period_key = func.concat(
                cast(PrimarySalesAndStock.year, String),
                "-Q",
                cast(PrimarySalesAndStock.quarter, String),
            )
        else:
            period_key = func.concat(
                cast(PrimarySalesAndStock.year, String),
                "-",
                func.lpad(cast(PrimarySalesAndStock.month, String), 2, "0"),
            )

        period_totals = (
            select(
                period_key.label("period"),
                func.round(func.sum(PrimarySalesAndStock.amount)).label("total_amount"),
            )
            .select_from(PrimarySalesAndStock)
            .join(SKU, PrimarySalesAndStock.sku_id == SKU.id)
            .where(
                PrimarySalesAndStock.indicator == "Первичная продажа",
                PrimarySalesAndStock.year.in_(filters.years),
            )
        )

        if company_id is not None:
            period_totals = period_totals.where(SKU.company_id == company_id)

        if filters.months:
            period_totals = period_totals.where(
                PrimarySalesAndStock.month.in_(filters.months)
            )

        if filters.quarters:
            period_totals = period_totals.where(
                PrimarySalesAndStock.quarter.in_(filters.quarters)
            )

        period_totals = period_totals.group_by(period_key).cte("period_totals")

        base_stmt = (
            select(
                Distributor.id.label("distributor_id"),
                Distributor.name.label("distributor_name"),
                period_key.label("period"),
                func.round(func.sum(PrimarySalesAndStock.amount)).label("amount"),
            )
            .select_from(PrimarySalesAndStock)
            .join(Distributor, PrimarySalesAndStock.distributor_id == Distributor.id)
            .join(SKU, PrimarySalesAndStock.sku_id == SKU.id)
            .where(
                PrimarySalesAndStock.indicator == "Первичная продажа",
                PrimarySalesAndStock.year.in_(filters.years),
            )
        )

        if company_id is not None:
            base_stmt = base_stmt.where(SKU.company_id == company_id)

        if filters.months:
            base_stmt = base_stmt.where(PrimarySalesAndStock.month.in_(filters.months))

        if filters.quarters:
            base_stmt = base_stmt.where(
                PrimarySalesAndStock.quarter.in_(filters.quarters)
            )

        if filters.brand_ids:
            base_stmt = base_stmt.where(SKU.brand_id.in_(filters.brand_ids))

        if filters.group_ids:
            base_stmt = base_stmt.where(SKU.product_group_id.in_(filters.group_ids))

        if filters.promo_type_id:
            base_stmt = base_stmt.where(SKU.promotion_type_id == filters.promo_type_id)

        base_stmt = base_stmt.group_by(
            Distributor.id, Distributor.name, period_key
        ).cte("period_agg")

        with_percentages = (
            select(
                base_stmt.c.distributor_id,
                base_stmt.c.distributor_name,
                base_stmt.c.period,
                base_stmt.c.amount,
                func.round(
                    case(
                        (
                            period_totals.c.total_amount > 0,
                            (base_stmt.c.amount * 100.0)
                            / func.nullif(period_totals.c.total_amount, 0),
                        ),
                        else_=0,
                    )
                ).label("share_percent"),
            )
            .select_from(base_stmt)
            .join(period_totals, base_stmt.c.period == period_totals.c.period)
            .where(
                func.round(
                    case(
                        (
                            period_totals.c.total_amount > 0,
                            (base_stmt.c.amount * 100.0)
                            / func.nullif(period_totals.c.total_amount, 0),
                        ),
                        else_=0,
                    )
                )
                != 0
            )
        ).cte("with_percentages")

        final_stmt = select(
            with_percentages.c.distributor_id,
            with_percentages.c.distributor_name,
            func.json_object_agg(
                with_percentages.c.period,
                func.json_build_object(
                    "amount",
                    with_percentages.c.amount,
                    "share_percent",
                    with_percentages.c.share_percent,
                ),
            ).label("periods_data"),
        ).group_by(
            with_percentages.c.distributor_id, with_percentages.c.distributor_name
        )

        if filters.limit:
            final_stmt = final_stmt.limit(filters.limit)
        if filters.offset:
            final_stmt = final_stmt.offset(filters.offset)

        result = await session.execute(final_stmt)
        return result.mappings().all()

    @staticmethod
    async def get_unpublish(
        session: "AsyncSession", limit=100, offset=0
    ) -> Sequence[ModelType]:
        stmt = select(PrimarySalesAndStock).where(
            PrimarySalesAndStock.published.is_(False)
        )

        stmt = stmt.limit(limit).offset(offset)
        result = await session.execute(stmt)
        return result.scalars().all()

    @staticmethod
    async def publish(session: "AsyncSession") -> int:
        stmt = (
            update(PrimarySalesAndStock)
            .where(PrimarySalesAndStock.published.is_(False))
            .values(published=True)
            .returning(PrimarySalesAndStock.id)
        )
        result = await session.execute(stmt)
        updated_ids = result.scalars().all()

        if not updated_ids:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Нет неопубликованных записей",
            )

        await session.commit()
        return len(updated_ids)

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
                update(PrimarySalesAndStock)
                .where(
                    PrimarySalesAndStock.id.in_(batch_ids),
                    PrimarySalesAndStock.published.is_(False),
                )
                .values(published=True)
                .returning(PrimarySalesAndStock.id, PrimarySalesAndStock.published)
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


class SecondarySalesService(
    BaseService[SecondarySales, sale.SecondarySalesCreate, sale.SecondarySalesUpdate]
):
    async def import_sales(
        self,
        session: "AsyncSession",
        file: "UploadFile",
        user_id: int,
        batch_size: int = 2000,
    ):
        temp = await save_upload_to_temp(file)
        try:
            pharmacy_names: set[str] = set()
            sku_names: set[str] = set()
            total_records = 0

            for _, record in iter_excel_records(temp):
                total_records += 1
                pharmacy_name = record.get("аптека")
                sku_name = record.get("sku")
                if pharmacy_name:
                    pharmacy_names.add(pharmacy_name)
                if sku_name:
                    sku_names.add(sku_name)

            import_log = ImportLogs(
                uploaded_by=user_id,
                target_table="Вторичные продажи",
                records_count=total_records,
            )
            session.add(import_log)
            await session.flush()

            results = await asyncio.gather(
                self.get_id_map(session, Pharmacy, "name", pharmacy_names),
                self.get_id_map(session, SKU, "name", sku_names),
                return_exceptions=True,
            )

            pharmacy_map, missing_pharmacies = results[0]
            sku_map, missing_skus = results[1]

            skipped_records = []
            data_to_insert = []
            imported = 0

            for row_index, record in iter_excel_records(temp):
                missing_keys = []
                pharmacy_name = record.get("аптека")
                sku_name = record.get("sku")
                month_value = record.get("месяц")
                record["квартал"] = (
                    (int(month_value) - 1) // 3 + 1 if month_value else None
                )

                if pharmacy_name in missing_pharmacies:
                    missing_keys.append(f"аптека: {pharmacy_name}")

                if sku_name in missing_skus:
                    missing_keys.append(f"SKU: {sku_name}")

                if missing_keys:
                    skipped_records.append({"row": row_index, "missing": missing_keys})
                    continue

                relation_fields = {
                    "pharmacy_id": pharmacy_map[pharmacy_name],
                    "sku_id": sku_map[sku_name],
                    "import_log_id": import_log.id,
                }
                data_to_insert.append(
                    map_record(record, secondary_sales_mapping, relation_fields)
                )

                if len(data_to_insert) >= batch_size:
                    await session.execute(insert(self.model), data_to_insert)
                    imported += len(data_to_insert)
                    data_to_insert = []

            if data_to_insert:
                await session.execute(insert(self.model), data_to_insert)
                imported += len(data_to_insert)

            await session.commit()

            return {
                "imported": imported,
                "skipped": len(skipped_records),
                "total": total_records,
                "skipped_records": skipped_records,
            }
        finally:
            temp.close()

    async def get_multi(
        self,
        session: "AsyncSession",
        filters: sale.SecondaryTertiarySalesFilter | None = None,
        load_options: list[Any] | None = None,
    ) -> Sequence[ModelType]:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        if filters.published:
            stmt = stmt.where(self.model.published)
        if filters.years:
            stmt = stmt.where(self.model.year.in_(filters.years))
        if filters.quarters:
            stmt = stmt.where(self.model.quarter.in_(filters.quarters))
        if filters.months:
            stmt = stmt.where(self.model.month.in_(filters.months))
        if filters.pharmacy_ids:
            stmt = stmt.where(self.model.pharmacy_id.in_(filters.pharmacy_ids))
        if filters.sku_ids:
            stmt = stmt.where(self.model.sku_id.in_(filters.sku_ids))
        stmt = stmt.limit(filters.limit).offset(filters.offset)

        result = await session.execute(stmt)

        if result is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Продажи не найдены"
            )
        return result.scalars().all()

    @staticmethod
    async def get_sales_report(
        session: "AsyncSession",
        filters: sale.SecTerSalesReportFilter,
        company_id: int | None,
    ):
        if filters.group_by_period == "year":
            period_key = cast(SecondarySales.year, String)
        elif filters.group_by_period == "quarter":
            period_key = func.concat(
                cast(SecondarySales.year, String),
                "-Q",
                cast(SecondarySales.quarter, String),
            )
        else:
            period_key = func.concat(
                cast(SecondarySales.year, String),
                "-",
                func.lpad(cast(SecondarySales.month, String), 2, "0"),
            )

        dimension_mapping = {
            "sku": {
                "id": SKU.id.label("sku_id"),
                "name": SKU.name.label("sku_name"),
                "group_fields": [SKU.id, SKU.name],
            },
            "brand": {
                "id": Brand.id.label("brand_id"),
                "name": Brand.name.label("brand_name"),
                "group_fields": [Brand.id, Brand.name],
            },
            "promotion_type": {
                "id": PromotionType.id.label("promotion_type_id"),
                "name": PromotionType.name.label("promotion_type_name"),
                "group_fields": [PromotionType.id, PromotionType.name],
            },
            "product_group": {
                "id": ProductGroup.id.label("product_group_id"),
                "name": ProductGroup.name.label("product_group_name"),
                "group_fields": [ProductGroup.id, ProductGroup.name],
            },
            "distributor": {
                "id": Distributor.id.label("distributor_id"),
                "name": Distributor.name.label("distributor_name"),
                "group_fields": [Distributor.id, Distributor.name],
            },
            "geo_indicator": {
                "id": GeoIndicator.id.label("geo_indicator_id"),
                "name": GeoIndicator.name.label("geo_indicator_name"),
                "group_fields": [GeoIndicator.id, GeoIndicator.name],
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
                func.sum(SecondarySales.packages).label("packages"),
                func.round(func.sum(SecondarySales.amount)).label("amount"),
            )
            .select_from(SecondarySales)
            .join(SKU, SecondarySales.sku_id == SKU.id)
            .join(Brand, SKU.brand_id == Brand.id)
            .join(PromotionType, SKU.promotion_type_id == PromotionType.id)
            .join(ProductGroup, SKU.product_group_id == ProductGroup.id)
            .join(Pharmacy, SecondarySales.pharmacy_id == Pharmacy.id)
            .outerjoin(Distributor, Pharmacy.distributor_id == Distributor.id)
            .outerjoin(GeoIndicator, Pharmacy.geo_indicator_id == GeoIndicator.id)
            .where(
                SecondarySales.indicator == "Вторичные продажи",
                SecondarySales.year.in_(filters.years),
            )
        )

        if company_id is not None:
            base_stmt = base_stmt.where(SKU.company_id == company_id)

        if filters.months:
            base_stmt = base_stmt.where(SecondarySales.month.in_(filters.months))

        if filters.quarters:
            base_stmt = base_stmt.where(SecondarySales.quarter.in_(filters.quarters))

        if filters.brand_ids:
            base_stmt = base_stmt.where(Brand.id.in_(filters.brand_ids))

        if filters.group_ids:
            base_stmt = base_stmt.where(ProductGroup.id.in_(filters.group_ids))

        if filters.promo_type_id:
            base_stmt = base_stmt.where(PromotionType.id == filters.promo_type_id)

        if filters.distributor_ids:
            base_stmt = base_stmt.where(Distributor.id.in_(filters.distributor_ids))

        if filters.sku_ids:
            base_stmt = base_stmt.where(SKU.id.in_(filters.sku_ids))

        if filters.geo_indicators_ids:
            base_stmt = base_stmt.where(GeoIndicator.id.in_(filters.geo_indicators_ids))

        if filters.search and filters.group_by_dimensions:
            search_term = f"%{filters.search}%"
            search_conditions = []
            if "sku" in filters.group_by_dimensions:
                search_conditions.append(SKU.name.ilike(search_term))
            if "brand" in filters.group_by_dimensions:
                search_conditions.append(Brand.name.ilike(search_term))
            if "promotion_type" in filters.group_by_dimensions:
                search_conditions.append(PromotionType.name.ilike(search_term))
            if "product_group" in filters.group_by_dimensions:
                search_conditions.append(ProductGroup.name.ilike(search_term))
            if "distributor" in filters.group_by_dimensions:
                search_conditions.append(Distributor.name.ilike(search_term))
            if "geo_indicator" in filters.group_by_dimensions:
                search_conditions.append(GeoIndicator.name.ilike(search_term))

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
                    "packages", base_stmt.c.packages, "amount", base_stmt.c.amount
                ),
            ).label("periods_data"),
        )

        if final_group_by_fields:
            final_stmt = final_stmt.group_by(*final_group_by_fields)

        if filters.limit:
            final_stmt = final_stmt.limit(filters.limit)
        if filters.offset:
            final_stmt = final_stmt.offset(filters.offset)

        result = await session.execute(final_stmt)
        return result.mappings().all()

    @staticmethod
    async def get_period_totals(
        session: "AsyncSession",
        filters: sale.SecTerSalesPeriodFilter,
        company_id: int | None,
    ):
        if filters.group_by_period == "year":
            period_key = cast(SecondarySales.year, String)
            period_columns = [SecondarySales.year]
        elif filters.group_by_period == "quarter":
            period_key = func.concat(
                cast(SecondarySales.year, String),
                "-Q",
                cast(SecondarySales.quarter, String),
            )
            period_columns = [SecondarySales.year, SecondarySales.quarter]
        else:
            period_key = func.concat(
                cast(SecondarySales.year, String),
                "-",
                func.lpad(cast(SecondarySales.month, String), 2, "0"),
            )
            period_columns = [SecondarySales.year, SecondarySales.month]

        stmt = (
            select(
                period_key.label("period"),
                func.sum(SecondarySales.packages).label("packages"),
                func.round(func.sum(SecondarySales.amount)).label("sales"),
            )
            .select_from(SecondarySales)
            .join(SKU, SecondarySales.sku_id == SKU.id)
            .where(
                SecondarySales.indicator == "Вторичные продажи",
                SecondarySales.year.in_(filters.years),
            )
        )

        if company_id is not None:
            stmt = stmt.where(SKU.company_id == company_id)

        if filters.months:
            stmt = stmt.where(SecondarySales.month.in_(filters.months))

        if filters.quarters:
            stmt = stmt.where(SecondarySales.quarter.in_(filters.quarters))

        if filters.distributor_ids or filters.geo_indicator_ids:
            stmt = stmt.join(Pharmacy, SecondarySales.pharmacy_id == Pharmacy.id)

        if filters.distributor_ids:
            stmt = stmt.where(Pharmacy.distributor_id.in_(filters.distributor_ids))

        if filters.brand_ids:
            stmt = stmt.where(SKU.brand_id.in_(filters.brand_ids))

        if filters.product_group_ids:
            stmt = stmt.where(SKU.product_group_id.in_(filters.product_group_ids))

        if filters.sku_ids:
            stmt = stmt.where(SKU.id.in_(filters.sku_ids))

        if filters.geo_indicator_ids:
            stmt = stmt.where(Pharmacy.geo_indicator_id.in_(filters.geo_indicator_ids))

        stmt = stmt.group_by(*period_columns).order_by(period_key.desc())

        result = await session.execute(stmt)
        return result.mappings().all()

    @staticmethod
    async def get_sales_by_distributor_report(
        session: "AsyncSession",
        company_id: int | None,
        filters: sale.SalesByDistributorFilter,
    ):
        if filters.group_by_period == "year":
            period_key = cast(SecondarySales.year, String)
        elif filters.group_by_period == "quarter":
            period_key = func.concat(
                cast(SecondarySales.year, String),
                "-Q",
                cast(SecondarySales.quarter, String),
            )
        else:
            period_key = func.concat(
                cast(SecondarySales.year, String),
                "-",
                func.lpad(cast(SecondarySales.month, String), 2, "0"),
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
            .join(Pharmacy, SecondarySales.pharmacy_id == Pharmacy.id)
            .join(Distributor, Pharmacy.distributor_id == Distributor.id)
            .join(SKU, SecondarySales.sku_id == SKU.id)
            .join(Brand, SKU.brand_id == Brand.id)
            .join(ProductGroup, SKU.product_group_id == ProductGroup.id)
            .where(
                SecondarySales.year.in_(filters.years),
            )
        )

        if company_id is not None:
            base_stmt = base_stmt.where(SKU.company_id == company_id)

        if filters.months:
            base_stmt = base_stmt.where(SecondarySales.month.in_(filters.months))

        if filters.quarters:
            base_stmt = base_stmt.where(SecondarySales.quarter.in_(filters.quarters))

        if filters.distributor_ids:
            base_stmt = base_stmt.where(Distributor.id.in_(filters.distributor_ids))

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

        if filters.limit:
            final_stmt = final_stmt.limit(filters.limit)
        if filters.offset:
            final_stmt = final_stmt.offset(filters.offset)

        result = await session.execute(final_stmt)
        return result.mappings().all()

    @staticmethod
    async def get_total_sales_by_distributor(
        session: "AsyncSession",
        company_id: int | None,
        filters: sale.ChartSalesByDistributorFilter,
    ):
        if filters.group_by_period == "year":
            period_key = cast(SecondarySales.year, String)
        elif filters.group_by_period == "quarter":
            period_key = func.concat(
                cast(SecondarySales.year, String),
                "-Q",
                cast(SecondarySales.quarter, String),
            )
        else:
            period_key = func.concat(
                cast(SecondarySales.year, String),
                "-",
                func.lpad(cast(SecondarySales.month, String), 2, "0"),
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
            .join(Pharmacy, SecondarySales.pharmacy_id == Pharmacy.id)
            .join(Distributor, Pharmacy.distributor_id == Distributor.id)
            .join(SKU, SecondarySales.sku_id == SKU.id)
            .where(
                SecondarySales.year.in_(filters.years),
            )
        )

        if company_id is not None:
            base_stmt = base_stmt.where(SKU.company_id == company_id)

        if filters.months:
            base_stmt = base_stmt.where(SecondarySales.month.in_(filters.months))

        if filters.quarters:
            base_stmt = base_stmt.where(SecondarySales.quarter.in_(filters.quarters))

        if filters.distributor_ids:
            base_stmt = base_stmt.where(Distributor.id.in_(filters.distributor_ids))

        if filters.brand_ids:
            base_stmt = base_stmt.where(SKU.brand_id.in_(filters.brand_ids))

        if filters.product_group_ids:
            base_stmt = base_stmt.where(
                SKU.product_group_id.in_(filters.product_group_ids)
            )

        if filters.geo_indicator_ids:
            base_stmt = base_stmt.where(
                Pharmacy.geo_indicator_id.in_(filters.geo_indicator_ids)
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


class TertiarySalesService(
    BaseService[
        TertiarySalesAndStock, sale.TertiarySalesCreate, sale.TertiarySalesUpdate
    ]
):
    async def import_sales(
        self,
        session: "AsyncSession",
        file: "UploadFile",
        user_id: int,
        batch_size: int = 2000,
    ):
        temp = await save_upload_to_temp(file)
        try:
            pharmacy_names: set[str] = set()
            sku_names: set[str] = set()
            total_records = 0

            for _, record in iter_excel_records(temp):
                total_records += 1
                pharmacy_name = record.get("аптека")
                sku_name = record.get("sku")
                if pharmacy_name:
                    pharmacy_names.add(pharmacy_name)
                if sku_name:
                    sku_names.add(sku_name)

            import_log = ImportLogs(
                uploaded_by=user_id,
                target_table="Третичные продажи",
                records_count=total_records,
            )
            session.add(import_log)
            await session.flush()

            results = await asyncio.gather(
                self.get_id_map(session, Pharmacy, "name", pharmacy_names),
                self.get_id_map(session, SKU, "name", sku_names),
                return_exceptions=True,
            )

            pharmacy_map, missing_pharmacies = results[0]
            sku_map, missing_skus = results[1]

            skipped_records = []
            data_to_insert = []
            imported = 0

            for row_index, record in iter_excel_records(temp):
                missing_keys = []
                pharmacy_name = record.get("аптека")
                sku_name = record.get("sku")
                month_value = record.get("месяц")
                record["квартал"] = (
                    (int(month_value) - 1) // 3 + 1 if month_value else None
                )

                if pharmacy_name in missing_pharmacies:
                    missing_keys.append(f"аптека: {pharmacy_name}")

                if sku_name in missing_skus:
                    missing_keys.append(f"SKU: {sku_name}")

                if missing_keys:
                    skipped_records.append({"row": row_index, "missing": missing_keys})
                    continue

                relation_fields = {
                    "pharmacy_id": pharmacy_map[pharmacy_name],
                    "sku_id": sku_map[sku_name],
                    "import_log_id": import_log.id,
                }
                data_to_insert.append(
                    map_record(record, secondary_sales_mapping, relation_fields)
                )

                if len(data_to_insert) >= batch_size:
                    await session.execute(insert(self.model), data_to_insert)
                    imported += len(data_to_insert)
                    data_to_insert = []

            if data_to_insert:
                await session.execute(insert(self.model), data_to_insert)
                imported += len(data_to_insert)

            await session.commit()

            return {
                "imported": imported,
                "skipped": len(skipped_records),
                "total": total_records,
                "skipped_records": skipped_records,
            }
        finally:
            temp.close()

    async def get_multi(
        self,
        session: "AsyncSession",
        filters: sale.SecondaryTertiarySalesFilter | None = None,
        load_options: list[Any] | None = None,
    ) -> Sequence[ModelType]:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        if filters.published:
            stmt = stmt.where(self.model.published)
        if filters.years:
            stmt = stmt.where(self.model.year.in_(filters.years))
        if filters.quarters:
            stmt = stmt.where(self.model.quarter.in_(filters.quarters))
        if filters.months:
            stmt = stmt.where(self.model.month.in_(filters.months))
        if filters.pharmacy_ids:
            stmt = stmt.where(self.model.pharmacy_id.in_(filters.pharmacy_ids))
        if filters.sku_ids:
            stmt = stmt.where(self.model.sku_id.in_(filters.sku_ids))
        if filters.indicator:
            stmt = stmt.where(self.model.indicator == filters.indicator)
        stmt = stmt.limit(filters.limit).offset(filters.offset)

        result = await session.execute(stmt)
        return result.scalars().all()

    @staticmethod
    async def get_sales_report(
        session: "AsyncSession",
        filters: sale.SecTerSalesReportFilter,
        company_id: int | None,
    ):
        if filters.group_by_period == "year":
            period_key = cast(TertiarySalesAndStock.year, String)
        elif filters.group_by_period == "quarter":
            period_key = func.concat(
                cast(TertiarySalesAndStock.year, String),
                "-Q",
                cast(TertiarySalesAndStock.quarter, String),
            )
        else:
            period_key = func.concat(
                cast(TertiarySalesAndStock.year, String),
                "-",
                func.lpad(cast(TertiarySalesAndStock.month, String), 2, "0"),
            )

        dimension_mapping = {
            "sku": {
                "id": SKU.id.label("sku_id"),
                "name": SKU.name.label("sku_name"),
                "group_fields": [SKU.id, SKU.name],
            },
            "brand": {
                "id": Brand.id.label("brand_id"),
                "name": Brand.name.label("brand_name"),
                "group_fields": [Brand.id, Brand.name],
            },
            "promotion_type": {
                "id": PromotionType.id.label("promotion_type_id"),
                "name": PromotionType.name.label("promotion_type_name"),
                "group_fields": [PromotionType.id, PromotionType.name],
            },
            "product_group": {
                "id": ProductGroup.id.label("product_group_id"),
                "name": ProductGroup.name.label("product_group_name"),
                "group_fields": [ProductGroup.id, ProductGroup.name],
            },
            "distributor": {
                "id": Distributor.id.label("distributor_id"),
                "name": Distributor.name.label("distributor_name"),
                "group_fields": [Distributor.id, Distributor.name],
            },
            "geo_indicator": {
                "id": GeoIndicator.id.label("geo_indicator_id"),
                "name": GeoIndicator.name.label("geo_indicator_name"),
                "group_fields": [GeoIndicator.id, GeoIndicator.name],
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
                func.sum(TertiarySalesAndStock.packages).label("packages"),
                func.round(func.sum(TertiarySalesAndStock.amount)).label("amount"),
            )
            .select_from(TertiarySalesAndStock)
            .join(SKU, TertiarySalesAndStock.sku_id == SKU.id)
            .join(Brand, SKU.brand_id == Brand.id)
            .join(PromotionType, SKU.promotion_type_id == PromotionType.id)
            .join(ProductGroup, SKU.product_group_id == ProductGroup.id)
            .join(Pharmacy, TertiarySalesAndStock.pharmacy_id == Pharmacy.id)
            .outerjoin(Distributor, Pharmacy.distributor_id == Distributor.id)
            .outerjoin(GeoIndicator, Pharmacy.geo_indicator_id == GeoIndicator.id)
            .where(
                TertiarySalesAndStock.indicator == "Третичные продажи",
                TertiarySalesAndStock.year.in_(filters.years),
            )
        )

        if company_id is not None:
            base_stmt = base_stmt.where(SKU.company_id == company_id)

        if filters.months:
            base_stmt = base_stmt.where(TertiarySalesAndStock.month.in_(filters.months))

        if filters.quarters:
            base_stmt = base_stmt.where(
                TertiarySalesAndStock.quarter.in_(filters.quarters)
            )

        if filters.brand_ids:
            base_stmt = base_stmt.where(Brand.id.in_(filters.brand_ids))

        if filters.group_ids:
            base_stmt = base_stmt.where(ProductGroup.id.in_(filters.group_ids))

        if filters.promo_type_id:
            base_stmt = base_stmt.where(PromotionType.id == filters.promo_type_id)

        if filters.distributor_ids:
            base_stmt = base_stmt.where(Distributor.id.in_(filters.distributor_ids))

        if filters.sku_ids:
            base_stmt = base_stmt.where(SKU.id.in_(filters.sku_ids))

        if filters.geo_indicators_ids:
            base_stmt = base_stmt.where(GeoIndicator.id.in_(filters.geo_indicators_ids))

        if filters.search and filters.group_by_dimensions:
            search_term = f"%{filters.search}%"
            search_conditions = []
            if "sku" in filters.group_by_dimensions:
                search_conditions.append(SKU.name.ilike(search_term))
            if "brand" in filters.group_by_dimensions:
                search_conditions.append(Brand.name.ilike(search_term))
            if "promotion_type" in filters.group_by_dimensions:
                search_conditions.append(PromotionType.name.ilike(search_term))
            if "product_group" in filters.group_by_dimensions:
                search_conditions.append(ProductGroup.name.ilike(search_term))
            if "distributor" in filters.group_by_dimensions:
                search_conditions.append(Distributor.name.ilike(search_term))
            if "geo_indicator" in filters.group_by_dimensions:
                search_conditions.append(GeoIndicator.name.ilike(search_term))

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
                    "packages", base_stmt.c.packages, "amount", base_stmt.c.amount
                ),
            ).label("periods_data"),
        )

        if final_group_by_fields:
            final_stmt = final_stmt.group_by(*final_group_by_fields)

        if filters.limit:
            final_stmt = final_stmt.limit(filters.limit)
        if filters.offset:
            final_stmt = final_stmt.offset(filters.offset)

        result = await session.execute(final_stmt)
        return result.mappings().all()

    @staticmethod
    async def get_period_totals(
        session: "AsyncSession",
        filters: sale.SecTerSalesPeriodFilter,
        company_id: int | None,
    ):
        if filters.group_by_period == "year":
            period_key = cast(TertiarySalesAndStock.year, String)
            period_columns = [TertiarySalesAndStock.year]
        elif filters.group_by_period == "quarter":
            period_key = func.concat(
                cast(TertiarySalesAndStock.year, String),
                "-Q",
                cast(TertiarySalesAndStock.quarter, String),
            )
            period_columns = [TertiarySalesAndStock.year, TertiarySalesAndStock.quarter]
        else:
            period_key = func.concat(
                cast(TertiarySalesAndStock.year, String),
                "-",
                func.lpad(cast(TertiarySalesAndStock.month, String), 2, "0"),
            )
            period_columns = [TertiarySalesAndStock.year, TertiarySalesAndStock.month]

        sales_packages = func.sum(
            case(
                (
                    TertiarySalesAndStock.indicator == "Третичные продажи",
                    TertiarySalesAndStock.packages,
                ),
                else_=0,
            )
        )
        sales_amount = func.sum(
            case(
                (
                    TertiarySalesAndStock.indicator == "Третичные продажи",
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
            .where(
                TertiarySalesAndStock.year.in_(filters.years),
            )
        )

        if company_id is not None:
            stmt = stmt.where(SKU.company_id == company_id)

        if filters.months:
            stmt = stmt.where(TertiarySalesAndStock.month.in_(filters.months))

        if filters.quarters:
            stmt = stmt.where(TertiarySalesAndStock.quarter.in_(filters.quarters))

        if filters.distributor_ids or filters.geo_indicator_ids:
            stmt = stmt.join(Pharmacy, TertiarySalesAndStock.pharmacy_id == Pharmacy.id)

        if filters.distributor_ids:
            stmt = stmt.where(Pharmacy.distributor_id.in_(filters.distributor_ids))

        if filters.geo_indicator_ids:
            stmt = stmt.where(Pharmacy.geo_indicator_id.in_(filters.geo_indicator_ids))

        if filters.brand_ids:
            stmt = stmt.where(SKU.brand_id.in_(filters.brand_ids))

        if filters.product_group_ids:
            stmt = stmt.where(SKU.product_group_id.in_(filters.product_group_ids))

        if filters.sku_ids:
            stmt = stmt.where(SKU.id.in_(filters.sku_ids))

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
        filters: sale.NumericDistributionFilter,
        company_id: int | None,
    ):
        if filters.group_by_period == "year":
            period_key = cast(TertiarySalesAndStock.year, String)
        elif filters.group_by_period == "quarter":
            period_key = func.concat(
                cast(TertiarySalesAndStock.year, String),
                "-Q",
                cast(TertiarySalesAndStock.quarter, String),
            )
        else:
            period_key = func.concat(
                cast(TertiarySalesAndStock.year, String),
                "-",
                func.lpad(cast(TertiarySalesAndStock.month, String), 2, "0"),
            )

        total_pharmacies_cte = (
            select(
                period_key.label("period"),
                Pharmacy.distributor_id.label("distributor_id"),
                Pharmacy.geo_indicator_id.label("geo_indicator_id"),
                func.count(func.distinct(TertiarySalesAndStock.pharmacy_id)).label(
                    "total_pharmacies"
                ),
            )
            .select_from(TertiarySalesAndStock)
            .join(Pharmacy, TertiarySalesAndStock.pharmacy_id == Pharmacy.id)
            .where(TertiarySalesAndStock.year.in_(filters.years))
        )

        if filters.months:
            total_pharmacies_cte = total_pharmacies_cte.where(
                TertiarySalesAndStock.month.in_(filters.months)
            )
        if filters.quarters:
            total_pharmacies_cte = total_pharmacies_cte.where(
                TertiarySalesAndStock.quarter.in_(filters.quarters)
            )

        total_pharmacies_cte = total_pharmacies_cte.group_by(
            Pharmacy.distributor_id, Pharmacy.geo_indicator_id, period_key
        ).cte("total_pharmacies")

        dimension_mapping = {
            "sku": {
                "id": SKU.id.label("sku_id"),
                "name": SKU.name.label("sku_name"),
                "group_fields": [SKU.id, SKU.name],
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
            "segment": {
                "id": Segment.id.label("segment_id"),
                "name": Segment.name.label("segment_name"),
                "group_fields": [Segment.id, Segment.name],
            },
            "distributor": {
                "id": Distributor.id.label("distributor_id"),
                "name": Distributor.name.label("distributor_name"),
                "group_fields": [Distributor.id, Distributor.name],
            },
            "geo_indicator": {
                "id": GeoIndicator.id.label("geo_indicator_id"),
                "name": GeoIndicator.name.label("geo_indicator_name"),
                "group_fields": [GeoIndicator.id, GeoIndicator.name],
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
            .join(Distributor, Pharmacy.distributor_id == Distributor.id)
            .outerjoin(GeoIndicator, Pharmacy.geo_indicator_id == GeoIndicator.id)
            .join(
                total_pharmacies_cte,
                and_(
                    Distributor.id == total_pharmacies_cte.c.distributor_id,
                    or_(
                        and_(
                            Pharmacy.geo_indicator_id.is_not(None),
                            Pharmacy.geo_indicator_id
                            == total_pharmacies_cte.c.geo_indicator_id,
                        ),
                        and_(
                            Pharmacy.geo_indicator_id.is_(None),
                            total_pharmacies_cte.c.geo_indicator_id.is_(None),
                        ),
                    ),
                    period_key == total_pharmacies_cte.c.period,
                ),
            )
            .where(
                TertiarySalesAndStock.indicator == "Остаток",
                TertiarySalesAndStock.year.in_(filters.years),
                TertiarySalesAndStock.packages > 0,
            )
        )

        if company_id is not None:
            base_stmt = base_stmt.where(SKU.company_id == company_id)

        if filters.months:
            base_stmt = base_stmt.where(TertiarySalesAndStock.month.in_(filters.months))

        if filters.quarters:
            base_stmt = base_stmt.where(
                TertiarySalesAndStock.quarter.in_(filters.quarters)
            )

        if filters.sku_ids:
            base_stmt = base_stmt.where(SKU.id.in_(filters.sku_ids))

        if filters.brand_ids:
            base_stmt = base_stmt.where(Brand.id.in_(filters.brand_ids))

        if filters.product_group_ids:
            base_stmt = base_stmt.where(ProductGroup.id.in_(filters.product_group_ids))

        if filters.segment_ids:
            base_stmt = base_stmt.where(Segment.id.in_(filters.segment_ids))

        if filters.distributor_ids:
            base_stmt = base_stmt.where(Distributor.id.in_(filters.distributor_ids))

        if filters.geo_indicator_ids:
            base_stmt = base_stmt.where(GeoIndicator.id.in_(filters.geo_indicator_ids))

        if filters.search and filters.group_by_dimensions:
            search_term = f"%{filters.search}%"
            search_conditions = []
            if "sku" in filters.group_by_dimensions:
                search_conditions.append(SKU.name.ilike(search_term))
            if "brand" in filters.group_by_dimensions:
                search_conditions.append(Brand.name.ilike(search_term))
            if "product_group" in filters.group_by_dimensions:
                search_conditions.append(ProductGroup.name.ilike(search_term))
            if "segment" in filters.group_by_dimensions:
                search_conditions.append(Segment.name.ilike(search_term))

            if search_conditions:
                base_stmt = base_stmt.where(or_(*search_conditions))

        group_by_fields.extend(
            [
                Distributor.id,
                Pharmacy.geo_indicator_id,
                period_key,
                total_pharmacies_cte.c.total_pharmacies,
            ]
        )
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

        final_select_fields.append(base_stmt.c.distributor_id)
        final_group_by_fields.append(base_stmt.c.distributor_id)

        final_stmt = select(
            *final_select_fields,
            func.json_object_agg(
                base_stmt.c.period,
                func.json_build_object("nd_percent", base_stmt.c.nd_percent),
            ).label("periods_data"),
        )

        if final_group_by_fields:
            final_stmt = final_stmt.group_by(*final_group_by_fields)

        if filters.limit:
            final_stmt = final_stmt.limit(filters.limit)
        if filters.offset:
            final_stmt = final_stmt.offset(filters.offset)

        result = await session.execute(final_stmt)
        return result.mappings().all()

    @staticmethod
    async def get_low_stock(
        session: "AsyncSession",
        company_id: int | None,
        filters: sale.LowStockLevelFilter,
    ):
        if filters.group_by_period == "year":
            period_key = cast(TertiarySalesAndStock.year, String)
        elif filters.group_by_period == "quarter":
            period_key = func.concat(
                cast(TertiarySalesAndStock.year, String),
                "-Q",
                cast(TertiarySalesAndStock.quarter, String),
            )
        else:
            period_key = func.concat(
                cast(TertiarySalesAndStock.year, String),
                "-",
                func.lpad(cast(TertiarySalesAndStock.month, String), 2, "0"),
            )

        dimension_mapping = {
            "product_group": {
                "id": ProductGroup.id.label("product_group_id"),
                "name": ProductGroup.name.label("product_group_name"),
                "group_fields": [ProductGroup.id, ProductGroup.name],
            },
            "responsible_employee": {
                "id": Employee.id.label("responsible_employee_id"),
                "name": Employee.full_name.label("responsible_employee_name"),
                "group_fields": [Employee.id, Employee.full_name],
            },
            "sku": {
                "id": SKU.id.label("sku_id"),
                "name": SKU.name.label("sku_name"),
                "group_fields": [SKU.id, SKU.name],
            },
            "brand": {
                "id": Brand.id.label("brand_id"),
                "name": Brand.name.label("brand_name"),
                "group_fields": [Brand.id, Brand.name],
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
                func.cast(
                    func.sum(TertiarySalesAndStock.packages).label("total_packages"),
                    Numeric(10, 2),
                ),
            )
            .select_from(TertiarySalesAndStock)
            .join(SKU, TertiarySalesAndStock.sku_id == SKU.id)
            .join(Brand, SKU.brand_id == Brand.id)
            .join(ProductGroup, SKU.product_group_id == ProductGroup.id)
            .join(Pharmacy, TertiarySalesAndStock.pharmacy_id == Pharmacy.id)
            .outerjoin(Employee, Pharmacy.responsible_employee_id == Employee.id)
            .where(
                TertiarySalesAndStock.indicator == "Остаток",
                TertiarySalesAndStock.year.in_(filters.years),
            )
        )

        if company_id is not None:
            base_stmt = base_stmt.where(SKU.company_id == company_id)

        if filters.months:
            base_stmt = base_stmt.where(TertiarySalesAndStock.month.in_(filters.months))

        if filters.quarters:
            base_stmt = base_stmt.where(
                TertiarySalesAndStock.quarter.in_(filters.quarters)
            )

        if filters.sku_ids:
            base_stmt = base_stmt.where(SKU.id.in_(filters.sku_ids))

        if filters.brand_ids:
            base_stmt = base_stmt.where(Brand.id.in_(filters.brand_ids))

        if filters.product_group_ids:
            base_stmt = base_stmt.where(ProductGroup.id.in_(filters.product_group_ids))

        if filters.responsible_employee_ids:
            base_stmt = base_stmt.where(
                Employee.id.in_(filters.responsible_employee_ids)
            )

        if filters.pharmacy_ids:
            base_stmt = base_stmt.where(Pharmacy.id.in_(filters.pharmacy_ids))

        if filters.search and filters.group_by_dimensions:
            search_term = f"%{filters.search}%"
            search_conditions = []
            if "sku" in filters.group_by_dimensions:
                search_conditions.append(SKU.name.ilike(search_term))
            if "brand" in filters.group_by_dimensions:
                search_conditions.append(Brand.name.ilike(search_term))
            if "product_group" in filters.group_by_dimensions:
                search_conditions.append(ProductGroup.name.ilike(search_term))
            if "responsible_employee" in filters.group_by_dimensions:
                search_conditions.append(
                    Employee.full_name.ilike(search_term)
                    if Employee.full_name is not None
                    else False
                )

            if search_conditions:
                base_stmt = base_stmt.where(or_(*search_conditions))

        group_by_fields.append(period_key)
        base_stmt = (
            base_stmt.group_by(*group_by_fields)
            .having(
                and_(
                    func.sum(TertiarySalesAndStock.packages) > 0,
                    func.sum(TertiarySalesAndStock.packages) < 1.0,
                )
            )
            .cte("period_agg")
        )

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
                func.json_build_object("total_packages", base_stmt.c.total_packages),
            ).label("periods_data"),
        )

        if final_group_by_fields:
            final_stmt = final_stmt.group_by(*final_group_by_fields)

        if filters.limit:
            final_stmt = final_stmt.limit(filters.limit)
        if filters.offset:
            final_stmt = final_stmt.offset(filters.offset)

        result = await session.execute(final_stmt)
        return result.mappings().all()

    @staticmethod
    async def get_unpublish(session: "AsyncSession") -> ModelType:
        stmt = select(TertiarySalesAndStock).where(~TertiarySalesAndStock.published)
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
                update(TertiarySalesAndStock)
                .where(
                    TertiarySalesAndStock.id.in_(batch_ids),
                    TertiarySalesAndStock.published.is_(False),
                )
                .values(published=True)
                .returning(TertiarySalesAndStock.id, TertiarySalesAndStock.published)
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


primary_sales_service = PrimarySalesAndStockService(PrimarySalesAndStock)
secondary_sales_service = SecondarySalesService(SecondarySales)
tertiary_sales_service = TertiarySalesService(TertiarySalesAndStock)
