import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, Sequence
from uuid import uuid4

from fastapi import HTTPException, status
from sqlalchemy import Numeric, case, cast, func, or_, select, update

from src.db.models import (
    SKU,
    Brand,
    Distributor,
    ImportLogs,
    PrimarySalesAndStock,
    ProductGroup,
    PromotionType,
)
from src.mapping.dimension_mapping.sale import BASE_SALE_DIMENSTION_MAPPING
from src.mapping.sales import primary_sales_mapping
from src.schemas import sale
from src.utils.list_query_helper import (
    BoolListSpec,
    InOrNullSpec,
    NumberTypedSpec,
    SearchSpec,
)
from src.utils.build_dimensions import build_dimensions
from src.utils.build_period_key import build_period_key
from src.utils.excel_parser import iter_excel_records
from src.utils.import_result import build_import_result
from src.utils.mapping import map_record

from src.services.base import BaseService, ModelType
from src.utils.list_query_helper import ListQueryHelper

if TYPE_CHECKING:
    from fastapi import UploadFile
    from sqlalchemy.ext.asyncio import AsyncSession

from src.services.sale.utils import upsert_batch_with_stats


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
        from src.tasks.sale_imports import import_sales_task

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
                service_path="src.services.sale.PrimarySalesAndStockService",
                model_path="src.db.models.PrimarySalesAndStock",
                batch_size=batch_size,
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
            total_records = 0

            import_log = ImportLogs(
                uploaded_by=user_id,
                target_table="Первичные продажи",
                records_count=0,
            )
            session.add(import_log)
            await session.flush()

            distributor_cache: dict[str, int] = {}
            sku_cache: dict[str, int] = {}
            missing_distributors: set[str] = set()
            missing_skus: set[str] = set()

            skipped_records = []
            skipped_total = 0
            skipped_limit = 1000
            data_to_insert = []
            pending_records: list[tuple[int, dict[str, Any]]] = []
            pending_distributors: set[str] = set()
            pending_skus: set[str] = set()
            imported = 0
            inserted = 0
            updated = 0
            deduplicated_in_batch = 0

            async def resolve_pending_names():
                if pending_distributors:
                    distributor_map, missing = await self.get_id_map(
                        session, Distributor, "name", pending_distributors
                    )
                    distributor_cache.update(distributor_map)
                    missing_distributors.update(missing)
                    pending_distributors.clear()

                if pending_skus:
                    sku_map, missing = await self.get_id_map(
                        session, SKU, "name", pending_skus
                    )
                    sku_cache.update(sku_map)
                    missing_skus.update(missing)
                    pending_skus.clear()

            async def process_pending_records():
                nonlocal skipped_total, imported, inserted, updated, deduplicated_in_batch
                nonlocal data_to_insert

                if not pending_records:
                    return

                await resolve_pending_names()

                for row_index, record in pending_records:
                    missing_keys = []
                    distributor_name = record.get("дистрибьютор")
                    sku_name = record.get("sku")

                    if distributor_name in missing_distributors:
                        missing_keys.append(f"дистрибьютор: {distributor_name}")

                    if sku_name in missing_skus:
                        missing_keys.append(f"SKU: {sku_name}")

                    if missing_keys:
                        skipped_total += 1
                        if len(skipped_records) < skipped_limit:
                            skipped_records.append(
                                {"row": row_index, "missing": missing_keys}
                            )
                        continue

                    relation_fields = {
                        "distributor_id": distributor_cache[distributor_name],
                        "sku_id": sku_cache[sku_name],
                        "import_log_id": import_log.id,
                    }
                    data_to_insert.append(
                        map_record(record, primary_sales_mapping, relation_fields)
                    )

                    if len(data_to_insert) >= batch_size:
                        (
                            batch_imported,
                            batch_inserted,
                            batch_updated,
                            batch_deduplicated,
                        ) = await upsert_batch_with_stats(
                            session=session,
                            model=self.model,
                            rows=data_to_insert,
                            key_fields=(
                                "distributor_id",
                                "sku_id",
                                "month",
                                "year",
                                "indicator",
                            ),
                            constraint_name="uq_primary_sales_business_key",
                        )
                        imported += batch_imported
                        inserted += batch_inserted
                        updated += batch_updated
                        deduplicated_in_batch += batch_deduplicated
                        data_to_insert = []

                pending_records.clear()

            with open(file_path, "rb") as f:
                for row_index, record in iter_excel_records(f):
                    total_records += 1
                    distributor_name = record.get("дистрибьютор")
                    sku_name = record.get("sku")
                    month_value = record.get("месяц")
                    record["квартал"] = (
                        (int(month_value) - 1) // 3 + 1 if month_value else None
                    )

                    pending_records.append((row_index, record))
                    if (
                        distributor_name
                        and distributor_name not in distributor_cache
                        and distributor_name not in missing_distributors
                    ):
                        pending_distributors.add(distributor_name)
                    if (
                        sku_name
                        and sku_name not in sku_cache
                        and sku_name not in missing_skus
                    ):
                        pending_skus.add(sku_name)

                    if len(pending_records) >= batch_size:
                        await process_pending_records()

            await process_pending_records()

            if data_to_insert:
                (
                    batch_imported,
                    batch_inserted,
                    batch_updated,
                    batch_deduplicated,
                ) = await upsert_batch_with_stats(
                    session=session,
                    model=self.model,
                    rows=data_to_insert,
                    key_fields=(
                        "distributor_id",
                        "sku_id",
                        "month",
                        "year",
                        "indicator",
                    ),
                    constraint_name="uq_primary_sales_business_key",
                )
                imported += batch_imported
                inserted += batch_inserted
                updated += batch_updated
                deduplicated_in_batch += batch_deduplicated

            import_log.records_count = total_records
            await session.commit()

            return build_import_result(
                total=total_records,
                imported=imported,
                skipped_records=skipped_records,
                skipped_total=skipped_total,
                inserted=inserted,
                updated=updated,
                deduplicated_in_batch=deduplicated_in_batch,
            )
        finally:
            pass

    async def get_multi(
        self,
        session: "AsyncSession",
        filters: sale.PrimarySalesAndStockListRequest | None = None,
        load_options: list[Any] | None = None,
    ) -> Sequence[ModelType]:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        sort_map = {
            "distributor": self.model.distributor_id,
            "brand": SKU.brand_id,
            "sku": self.model.sku_id,
            "month": self.model.month,
            "year": self.model.year,
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
                    InOrNullSpec(self.model.distributor_id, filters.distributor_ids),
                    InOrNullSpec(self.model.sku_id, filters.sku_ids),
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

            if filters.indicator:
                stmt = stmt.where(self.model.indicator == filters.indicator)

            if filters.sort_by == "brands" and not joined_sku:
                stmt = stmt.join(SKU, self.model.sku_id == SKU.id)

            stmt = ListQueryHelper.apply_pagination(stmt, filters.limit, filters.offset)

        result = await session.execute(stmt)

        if result is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Продажи не найдены"
            )
        return result.unique().scalars().all()

    @staticmethod
    async def get_shipment_stock_report(
        session: "AsyncSession",
        indicator: str,
        company_id: int | None,
        filters: sale.ShipmentStockFilter | None = None,
    ):
        period_key = build_period_key(filters.group_by_period, PrimarySalesAndStock)

        select_fields, group_by_fields, search_cols = build_dimensions(
            BASE_SALE_DIMENSTION_MAPPING, filters.group_by_dimensions
        )

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

        base_stmt = ListQueryHelper.apply_specs(
            base_stmt,
            [
                InOrNullSpec(PrimarySalesAndStock.month, filters.months),
                InOrNullSpec(PrimarySalesAndStock.quarter, filters.quarters),
                InOrNullSpec(Brand.id, filters.brand_ids),
                InOrNullSpec(ProductGroup.id, filters.product_group_ids),
                InOrNullSpec(PromotionType.id, filters.promotion_type_ids),
                InOrNullSpec(Distributor.id, filters.distributor_ids),
                InOrNullSpec(SKU.id, filters.sku_ids),
                SearchSpec(
                    filters.search if filters.group_by_dimensions else None, search_cols
                ),
            ],
        )

        group_by_fields.append(period_key)
        period_agg = base_stmt.group_by(*group_by_fields).cte("period_agg")

        final_select_fields = []
        final_group_by_fields = []

        for dim in filters.group_by_dimensions or []:
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
                    "packages",
                    period_agg.c.packages,
                    "amount",
                    period_agg.c.amount,
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
        filters: sale.PeriodFilter,
        company_id: int | None,
    ):
        period_key = build_period_key(filters.group_by_period, PrimarySalesAndStock)

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

        stmt = ListQueryHelper.apply_specs(
            stmt,
            [
                InOrNullSpec(PrimarySalesAndStock.month, filters.months),
                InOrNullSpec(PrimarySalesAndStock.quarter, filters.quarters),
                InOrNullSpec(SKU.brand_id, filters.brand_ids),
                InOrNullSpec(SKU.product_group_id, filters.product_group_ids),
                InOrNullSpec(
                    PrimarySalesAndStock.distributor_id, filters.distributor_ids
                ),
                InOrNullSpec(SKU.id, filters.sku_ids),
            ],
        )

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
        filters: sale.StockCoverageFilter | None = None,
        company_id: int | None = None,
    ):
        period_key = build_period_key(filters.group_by_period, PrimarySalesAndStock)

        select_fields, group_by_fields, search_cols = build_dimensions(
            BASE_SALE_DIMENSTION_MAPPING, filters.group_by_dimensions
        )

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

        base_stmt = ListQueryHelper.apply_specs(
            base_stmt,
            [
                InOrNullSpec(PrimarySalesAndStock.month, filters.months),
                InOrNullSpec(PrimarySalesAndStock.quarter, filters.quarters),
                InOrNullSpec(Brand.id, filters.brand_ids),
                InOrNullSpec(ProductGroup.id, filters.product_group_ids),
                InOrNullSpec(PromotionType.id, filters.promotion_type_ids),
                InOrNullSpec(Distributor.id, filters.distributor_ids),
                InOrNullSpec(SKU.id, filters.sku_ids),
                SearchSpec(
                    filters.search if filters.group_by_dimensions else None, search_cols
                ),
            ],
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
                func.json_build_object("coverage_months", period_agg.c.coverage_months),
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
        }

        final_stmt = ListQueryHelper.apply_sorting_with_default(
            final_stmt,
            getattr(filters, "sort_by", None),
            getattr(filters, "sort_order", None),
            sort_map,
        )

        result = await session.execute(final_stmt)
        return result.mappings().all()

    @staticmethod
    async def get_distributor_share_report(
        session: "AsyncSession",
        filters: "sale.DistributorShareFilter",
        company_id: int | None,
    ):
        period_key = build_period_key(filters.group_by_period, PrimarySalesAndStock)

        select_fields, group_by_fields, search_cols = build_dimensions(
            BASE_SALE_DIMENSTION_MAPPING, filters.group_by_dimensions
        )

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

        base_stmt = ListQueryHelper.apply_specs(
            base_stmt,
            [
                InOrNullSpec(PrimarySalesAndStock.month, filters.months),
                InOrNullSpec(PrimarySalesAndStock.quarter, filters.quarters),
                InOrNullSpec(Brand.id, filters.brand_ids),
                InOrNullSpec(ProductGroup.id, filters.product_group_ids),
                InOrNullSpec(PromotionType.id, filters.promotion_type_ids),
                InOrNullSpec(Distributor.id, filters.distributor_ids),
                InOrNullSpec(SKU.id, filters.sku_ids),
                SearchSpec(
                    filters.search if filters.group_by_dimensions else None, search_cols
                ),
            ],
        )

        group_by_fields.append(period_key)
        period_agg = base_stmt.group_by(*group_by_fields).cte("period_agg")

        period_totals = (
            select(
                period_agg.c.period,
                cast(func.sum(period_agg.c.amount), Numeric(18, 2)).label(
                    "total_amount"
                ),
            ).group_by(period_agg.c.period)
        ).cte("period_totals")

        percentage_select_fields = []
        if filters.group_by_dimensions:
            for dim in filters.group_by_dimensions:
                percentage_select_fields.extend(
                    [
                        getattr(period_agg.c, f"{dim}_id"),
                        getattr(period_agg.c, f"{dim}_name"),
                    ]
                )

        share_value = cast(
            case(
                (
                    period_totals.c.total_amount > 0,
                    (cast(period_agg.c.amount, Numeric) * 100)
                    / func.nullif(cast(period_totals.c.total_amount, Numeric), 0),
                ),
                else_=None,
            ),
            Numeric(12, 4),
        )

        share_expr = func.round(share_value, 2)

        with_percentages = (
            select(
                *percentage_select_fields,
                period_agg.c.period,
                period_agg.c.amount,
                share_expr.label("share_percent"),
            )
            .select_from(period_agg)
            .join(period_totals, period_agg.c.period == period_totals.c.period)
            .where(share_expr != 0)
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

        sort_map = {
            "sku": getattr(with_percentages.c, "sku_name", None),
            "brand": getattr(with_percentages.c, "brand_name", None),
            "promotion": getattr(with_percentages.c, "promotion_type_name", None),
            "product_group": getattr(with_percentages.c, "product_group_name", None),
            "distributor": getattr(with_percentages.c, "distributor_name", None),
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
    async def get_distributor_share_chart(
        session: "AsyncSession",
        filters: sale.SalesReportFilter,
        company_id: int | None,
    ):
        period_key = build_period_key(filters.group_by_period, PrimarySalesAndStock)

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

        period_totals = ListQueryHelper.apply_specs(
            period_totals,
            [
                InOrNullSpec(PrimarySalesAndStock.month, filters.months),
                InOrNullSpec(PrimarySalesAndStock.quarter, filters.quarters),
            ],
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

        base_stmt = ListQueryHelper.apply_specs(
            base_stmt,
            [
                InOrNullSpec(PrimarySalesAndStock.month, filters.months),
                InOrNullSpec(PrimarySalesAndStock.quarter, filters.quarters),
                InOrNullSpec(SKU.brand_id, filters.brand_ids),
                InOrNullSpec(SKU.product_group_id, filters.product_group_ids),
                InOrNullSpec(PromotionType.id, filters.promotion_type_ids),
            ],
        )

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

        final_stmt = ListQueryHelper.apply_pagination(
            final_stmt, filters.limit, filters.offset
        )

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
