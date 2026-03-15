from typing import TYPE_CHECKING, Any, AsyncIterator

from fastapi import UploadFile
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from src.db.models import (
    Brand,
    ImportLogs,
    ProductGroup,
    products,
)
from src.import_fields import product
from src.schemas import product as product_schema
from src.services.base import BaseService
from src.utils.excel_parser import parse_excel_file
from src.utils.import_result import build_import_result
from src.utils.list_query_helper import InOrNullSpec, ListQueryHelper, StringTypedSpec
from src.utils.records_resolver import resolve_records_fields
from src.utils.validate_required_columns import validate_required_columns

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

from src.schemas.base_filter import PaginatedResponse


class BrandService(
    BaseService[products.Brand, product_schema.BrandCreate, product_schema.BrandUpdate]
):
    async def get_multi(
        self,
        session: "AsyncSession",
        filters: product_schema.BrandListRequest | None = None,
        load_options: list[Any] | None = None,
    ) -> PaginatedResponse[product_schema.BrandResponse]:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        sort_map = {
            "name": self.model.name,
            "ims_name": self.model.ims_name,
            "promotion_type": self.model.promotion_type_id,
            "product_group": self.model.product_group_id,
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
                    StringTypedSpec(self.model.ims_name, filters.ims_name),
                    InOrNullSpec(
                        self.model.promotion_type_id, filters.promotion_type_ids
                    ),
                    InOrNullSpec(
                        self.model.product_group_id, filters.product_group_ids
                    ),
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
    ) -> AsyncIterator[products.Brand]:
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
        validate_required_columns(records, product.brand_fields)

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table="Бренды",
            records_count=len(records),
            target_table_name=self.model.__tablename__,
        )
        session.add(import_log)
        await session.flush()

        resolved = await resolve_records_fields(
            session, records, product.brand_fields, self.get_id_map
        )

        company_map = resolved.maps["компания"]
        product_group_pairs = {
            (r.get("группа"), company_map.get(r.get("компания")))
            for r in records
            if r.get("компания") in company_map
        }
        product_group_map, missing_product_groups = (
            await self.get_id_map(
                session,
                ProductGroup,
                "name",
                product_group_pairs,
                filter_field="company_id",
                filter_values=set(company_map.values()),
            )
            if product_group_pairs
            else ({}, set())
        )

        existing_rows = await session.execute(select(Brand.name, Brand.ims_name))
        existing_names = {row[0] for row in existing_rows if row[0]}
        existing_ims_names = {row[1] for row in existing_rows if row[1]}
        seen_names: set[str] = set()
        seen_ims_names: set[str] = set()

        skipped_records = []
        data_to_insert = []

        for idx, r in enumerate(records):
            missing_keys = resolved.collect_missing_keys(r, product.brand_fields)

            ids, null_keys = resolved.resolve_id_fields(r, product.brand_fields)
            if null_keys:
                missing_keys.extend(null_keys)

            company_id = ids.get("company_id")
            if company_id and (r.get("группа"), company_id) in missing_product_groups:
                missing_keys.append(f"группа: {r.get('группа')}")

            if missing_keys:
                skipped_records.append({"row": idx + 1, "missing": missing_keys})
                continue

            brand_name = r.get("название")
            ims_name_val = r.get("название ims")

            if brand_name and (
                brand_name in existing_names or brand_name in seen_names
            ):
                continue
            if ims_name_val and (
                ims_name_val in existing_ims_names or ims_name_val in seen_ims_names
            ):
                continue

            data_to_insert.append(
                {
                    "name": brand_name,
                    "ims_name": ims_name_val,
                    "promotion_type_id": ids.get("promotion_type_id"),
                    "product_group_id": product_group_map.get(
                        (r.get("группа"), company_id)
                    ),
                    "company_id": company_id,
                    "import_log_id": import_log.id,
                }
            )
            if brand_name:
                seen_names.add(brand_name)
            if ims_name_val:
                seen_ims_names.add(ims_name_val)

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
