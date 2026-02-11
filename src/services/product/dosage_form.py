from typing import TYPE_CHECKING, Any, AsyncIterator, Sequence

from fastapi import UploadFile
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from src.db.models import ImportLogs, products
from src.mapping.products import dosage_form_mapping
from src.schemas import product
from src.services.base import BaseService
from src.utils.excel_parser import parse_excel_file
from src.utils.list_query_helper import ListQueryHelper
from src.utils.mapping import map_record

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


class DosageFormService(
    BaseService[products.DosageForm, product.DosageFormCreate, product.DosageFormUpdate]
):
    async def get_multi(
        self,
        session: "AsyncSession",
        filters: product.DosageFormListRequest | None = None,
        load_options: list[Any] | None = None,
    ) -> Sequence[products.DosageForm]:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        stmt = ListQueryHelper.apply_sorting_with_default(
            stmt,
            getattr(filters, "sort_by", None),
            getattr(filters, "sort_order", None),
            {"name": self.model.name},
            self.model.created_at.desc(),
        )

        if filters:
            if filters.name:
                stmt = stmt.where(self.model.name.ilike(f"%{filters.name}%"))

            stmt = ListQueryHelper.apply_pagination(stmt, filters.limit, filters.offset)

        result = await session.execute(stmt)
        return result.unique().scalars().all()

    async def iter_multi(
        self,
        session: "AsyncSession",
        load_options: list[Any] | None = None,
        chunk_size: int = 1000,
    ) -> AsyncIterator[products.DosageForm]:
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
            target_table="Формы выпуска",
            records_count=len(records),
        )
        session.add(import_log)
        await session.flush()

        data_to_insert = []
        for r in records:
            relation_fields = {
                "import_log_id": import_log.id,
            }
            data_to_insert.append(map_record(r, dosage_form_mapping, relation_fields))

        await session.execute(insert(self.model), data_to_insert)
        await session.commit()
