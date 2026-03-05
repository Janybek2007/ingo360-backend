from typing import TYPE_CHECKING, Any, AsyncIterator, Sequence

from fastapi import UploadFile
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from src.db.models import ImportLogs, clients
from src.mapping.clients import distributor_mapping
from src.schemas import client
from src.services.base import BaseService
from src.utils.excel_parser import parse_excel_file
from src.utils.import_result import build_import_result
from src.utils.list_query_helper import ListQueryHelper
from src.utils.mapping import map_record

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


class DistributorService(
    BaseService[clients.Distributor, client.DistributorCreate, client.DistributorUpdate]
):
    async def get_multi(
        self,
        session: "AsyncSession",
        filters: client.DistributorListRequest | None = None,
        load_options: list[Any] | None = None,
    ) -> Sequence[clients.Distributor]:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        sort_map = {"name": self.model.name}
        stmt = ListQueryHelper.apply_sorting_with_default(
            stmt,
            getattr(filters, "sort_by", None),
            getattr(filters, "sort_order", None),
            sort_map,
            self.model.id.desc(),
        )

        if filters:
            if filters.name:
                stmt = ListQueryHelper.apply_string_typed_filter(
                    stmt, self.model.name, filters.name
                )
            stmt = ListQueryHelper.apply_pagination(stmt, filters.limit, filters.offset)

        result = await session.execute(stmt)
        return result.unique().scalars().all()

    async def iter_multi(
        self,
        session: "AsyncSession",
        load_options: list[Any] | None = None,
        chunk_size: int = 1000,
    ) -> AsyncIterator[clients.Distributor]:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        stmt = ListQueryHelper.apply_sorting_with_created(
            stmt,
            self.model.id.desc(),
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
            target_table="Дистрибьюторы",
            records_count=len(records),
            target_table_name=self.model.__tablename__,
        )
        session.add(import_log)
        await session.flush()

        data_to_insert = []
        for r in records:
            relation_fields = {
                "import_log_id": import_log.id,
            }
            data_to_insert.append(map_record(r, distributor_mapping, relation_fields))
        if data_to_insert:
            await session.execute(insert(self.model), data_to_insert)
        await session.commit()

        imported = len(data_to_insert)
        return build_import_result(
            total=len(records),
            imported=imported,
            skipped_records=[],
            inserted=imported,
            deduplicated_in_batch=0,
        )
