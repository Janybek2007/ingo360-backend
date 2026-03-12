import asyncio
from typing import TYPE_CHECKING, Any, AsyncIterator

from fastapi import UploadFile
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from src.db.models import (
    Brand,
    Company,
    Dosage,
    DosageForm,
    ImportLogs,
    ProductGroup,
    PromotionType,
    Segment,
    products,
)
from src.mapping.products import sku_mapping
from src.schemas import product
from src.services.base import BaseService
from src.utils.excel_parser import parse_excel_file
from src.utils.import_result import build_import_result
from src.utils.list_query_helper import InOrNullSpec, ListQueryHelper, StringTypedSpec
from src.utils.mapping import map_record
from src.utils.validate_required_columns import validate_required_columns

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

from src.schemas.base_filter import PaginatedResponse


class SKUService(BaseService[products.SKU, product.SKUCreate, product.SKUUpdate]):
    async def get_multi(
        self,
        session: "AsyncSession",
        filters: product.SKUListRequest | None = None,
        load_options: list[Any] | None = None,
    ) -> PaginatedResponse[product.SKUResponse]:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        sort_map = {
            "name": self.model.name,
            "brand": self.model.brand_id,
            "promotion_type": self.model.promotion_type_id,
            "product_group": self.model.product_group_id,
            "dosage_form": self.model.dosage_form_id,
            "dosage": self.model.dosage_id,
            "segment": self.model.segment_id,
            "company": self.model.company_id,
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
                    StringTypedSpec(self.model.name, filters.name),
                    InOrNullSpec(self.model.brand_id, filters.brand_ids),
                    InOrNullSpec(
                        self.model.promotion_type_id, filters.promotion_type_ids
                    ),
                    InOrNullSpec(
                        self.model.product_group_id, filters.product_group_ids
                    ),
                    InOrNullSpec(self.model.dosage_form_id, filters.dosage_form_ids),
                    InOrNullSpec(self.model.dosage_id, filters.dosage_ids),
                    InOrNullSpec(self.model.segment_id, filters.segment_ids),
                    InOrNullSpec(self.model.company_id, filters.company_ids),
                ],
            )
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
    ) -> AsyncIterator[products.SKU]:
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

    async def import_excel(
        self, session: "AsyncSession", file: "UploadFile", user_id: int
    ):
        records = await parse_excel_file(file)

        validate_required_columns(
            records,
            {
                "название|name",
                "бренд|brand",
                "форма выпуска|dosage_form|форма",
                "тип промоции|promotion_type|промоция",
                "компания|company",
                "группа|group|группа продуктов",
            },
        )

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table="SKU",
            records_count=len(records),
            target_table_name=self.model.__tablename__,
        )
        session.add(import_log)
        await session.flush()

        results = await asyncio.gather(
            self.get_id_map(session, Brand, "name", {r["бренд"] for r in records}),
            self.get_id_map(
                session, DosageForm, "name", {r["форма выпуска"] for r in records}
            ),
            self.get_id_map(
                session, PromotionType, "name", {r["тип промоции"] for r in records}
            ),
            self.get_id_map(session, Company, "name", {r["компания"] for r in records}),
            self.get_id_map(
                session, Segment, "name", {r.get("сегмент") for r in records}
            ),
            self.get_id_map(
                session, Dosage, "name", {r.get("дозировка") for r in records}
            ),
            return_exceptions=True,
        )

        brand_map, missing_brands = results[0]
        dosage_form_map, missing_dosage_forms = results[1]
        promotion_type_map, missing_promotion_types = results[2]
        company_map, missing_companies = results[3]
        segment_map, missing_segments = results[4]
        dosage_map, missing_dosages = results[5]

        product_group_pairs = {
            (r["группа"], company_map.get(r["компания"]))
            for r in records
            if r["компания"] in company_map
        }
        product_group_map, missing_product_groups = (
            await self.get_id_map(
                session,
                ProductGroup,
                "name",
                product_group_pairs,
                "company_id",
                set(company_map.values()),
            )
            if product_group_pairs
            else ({}, set())
        )

        skipped_records = []
        data_to_insert = []

        for idx, r in enumerate(records):
            missing_keys = []

            if r["бренд"] in missing_brands:
                missing_keys.append(f"бренд: {r['бренд']}")

            if r["форма выпуска"] in missing_dosage_forms:
                missing_keys.append(f"форма выпуска: {r['форма выпуска']}")

            if r["тип промоции"] in missing_promotion_types:
                missing_keys.append(f"тип промоции: {r['тип промоции']}")

            if r["компания"] in missing_companies:
                missing_keys.append(f"компания: {r['компания']}")

            segment_value = r.get("сегмент")
            if segment_value and segment_value in missing_segments:
                missing_keys.append(f"сегмент: {segment_value}")

            dosage_value = r.get("дозировка")
            if dosage_value and dosage_value in missing_dosages:
                missing_keys.append(f"дозировка: {dosage_value}")

            company_id = company_map.get(r["компания"])
            if company_id:
                product_group_key = (r["группа"], company_id)
                if product_group_key in missing_product_groups:
                    missing_keys.append(f"группа: {r['группа']}")

            if missing_keys:
                skipped_records.append({"row": idx + 1, "missing": missing_keys})
                continue

            relation_fields = {
                "brand_id": brand_map[r["бренд"]],
                "dosage_form_id": dosage_form_map[r["форма выпуска"]],
                "product_group_id": product_group_map[(r["группа"], company_id)],
                "promotion_type_id": promotion_type_map[r["тип промоции"]],
                "company_id": company_id,
                "import_log_id": import_log.id,
            }
            if dosage_value:
                relation_fields["dosage_id"] = dosage_map[dosage_value]
            if segment_value:
                relation_fields["segment_id"] = segment_map[segment_value]
            data_to_insert.append(map_record(r, sku_mapping, relation_fields))

        inserted_ids = []
        if data_to_insert:
            stmt = (
                insert(self.model)
                .values(data_to_insert)
                .on_conflict_do_nothing()
                .returning(self.model.id)
            )
            result = await session.execute(stmt)
            inserted_ids = result.scalars().all()

        await session.commit()

        return build_import_result(
            total=len(records),
            imported=len(inserted_ids),
            skipped_records=skipped_records,
            inserted=len(inserted_ids),
            deduplicated=len(data_to_insert) - len(inserted_ids),
        )
