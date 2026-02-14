from typing import TYPE_CHECKING, Any, AsyncIterator, Sequence

from fastapi import UploadFile
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from src.db.models import Company, ImportLogs, products
from src.mapping.products import product_group_mapping
from src.schemas import product
from src.services.base import BaseService
from src.utils.excel_parser import parse_excel_file
from src.utils.import_result import build_import_result
from src.utils.list_query_helper import InOrNullSpec, ListQueryHelper, StringTypedSpec
from src.utils.mapping import map_record

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


class ProductGroupService(
    BaseService[
        products.ProductGroup, product.ProductGroupCreate, product.ProductGroupUpdate
    ]
):
    async def get_multi(
        self,
        session: "AsyncSession",
        filters: product.ProductGroupListRequest | None = None,
        load_options: list[Any] | None = None,
    ) -> Sequence[products.ProductGroup]:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        sort_map = {
            "name": self.model.name,
            "companies": self.model.company_id,
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
                    InOrNullSpec(self.model.company_id, filters.company_ids),
                ],
            )

            stmt = ListQueryHelper.apply_pagination(stmt, filters.limit, filters.offset)

        result = await session.execute(stmt)
        return result.unique().scalars().all()

    async def iter_multi(
        self,
        session: "AsyncSession",
        load_options: list[Any] | None = None,
        chunk_size: int = 1000,
    ) -> AsyncIterator[products.ProductGroup]:
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

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table="Группы",
            records_count=len(records),
        )
        session.add(import_log)
        await session.flush()

        company_map, missing_companies = await self.get_id_map(
            session, Company, "name", {r["компания"] for r in records}
        )

        skipped_records = []
        data_to_insert = []

        for idx, r in enumerate(records):
            missing_keys = []

            if r["компания"] in missing_companies:
                missing_keys.append(f"компания: {r['компания']}")

            if missing_keys:
                skipped_records.append({"row": idx + 1, "missing": missing_keys})
                continue

            relation_fields = {
                "company_id": company_map[r["компания"]],
                "import_log_id": import_log.id,
            }
            data_to_insert.append(map_record(r, product_group_mapping, relation_fields))

        if data_to_insert:
            stmt = insert(self.model).on_conflict_do_nothing()
            await session.execute(stmt, data_to_insert)

        await session.commit()

        return build_import_result(
            total=len(records),
            imported=len(data_to_insert),
            skipped_records=skipped_records,
        )
