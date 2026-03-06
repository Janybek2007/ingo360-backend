from typing import TYPE_CHECKING, Any, AsyncIterator

from fastapi import UploadFile
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from src.db.models import ImportLogs, geography
from src.mapping.geography import region_mapping
from src.schemas import geography as geography_schema
from src.services.base import BaseService
from src.utils.excel_parser import parse_excel_file
from src.utils.import_result import build_import_result
from src.utils.list_query_helper import ListQueryHelper
from src.utils.mapping import map_record
from src.utils.validate_required_columns import validate_required_columns

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

from src.schemas.base_filter import PaginatedResponse


class CountryService(
    BaseService[
        geography.Country,
        geography_schema.CountryCreate,
        geography_schema.CountryUpdate,
    ]
):
    async def import_excel(
        self, session: "AsyncSession", file: "UploadFile", user_id: int
    ):
        records = await parse_excel_file(file)

        validate_required_columns(records, {"название|name"})

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table="Страны",
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
            data_to_insert.append(map_record(r, region_mapping, relation_fields))
        stmt = insert(self.model).on_conflict_do_nothing()
        if data_to_insert:
            await session.execute(stmt, data_to_insert)
        await session.commit()

        imported = len(data_to_insert)
        return build_import_result(
            total=len(records),
            imported=imported,
            skipped_records=[],
            inserted=imported,
            deduplicated=0,
        )

    async def get_multi(
        self,
        session: "AsyncSession",
        filters: geography_schema.CountryListRequest | None = None,
        load_options: list[Any] | None = None,
    ) -> PaginatedResponse[geography_schema.CountryResponse]:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        sort_map = {"name": self.model.name}
        stmt = ListQueryHelper.apply_sorting_with_default(
            stmt,
            getattr(filters, "sort_by", None),
            getattr(filters, "sort_order", None),
            sort_map,
            self.model.created_at.desc(),
        )
        if filters:
            if filters.name:
                stmt = ListQueryHelper.apply_string_typed_filter(
                    stmt, self.model.name, filters.name
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
    ) -> AsyncIterator[geography.Country]:
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
