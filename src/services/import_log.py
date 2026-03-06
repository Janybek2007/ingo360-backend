from typing import TYPE_CHECKING, AsyncIterator, Sequence

from sqlalchemy import select

from src.db.models import ImportLogs, User
from src.schemas.import_log import (
    ImportLogCreate,
    ImportLogFormattedResponse,
    ImportLogResponse,
    ImportLogUpdate,
)
from src.utils.format_date import format_date
from src.utils.list_query_helper import ListQueryHelper

from .base import BaseService, FilterSchemaType, ModelType

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


class ImportLogService(BaseService[ImportLogs, ImportLogCreate, ImportLogUpdate]):
    async def get_multi(
        self,
        session: "AsyncSession",
        filters: FilterSchemaType | None = None,
    ) -> Sequence[ModelType]:
        stmt = (
            select(
                ImportLogs.id,
                ImportLogs.target_table,
                ImportLogs.records_count,
                ImportLogs.created_at,
                User.first_name.label("user_first_name"),
                User.last_name.label("user_last_name"),
            )
            .join(User, self.model.uploaded_by == User.id)
            .order_by(ImportLogs.created_at.desc())
        )

        if filters:
            stmt = ListQueryHelper.apply_pagination(stmt, filters.limit, filters.offset)

        result = await session.execute(stmt)
        return [ImportLogResponse(**row) for row in result.mappings().all()]

    async def iter_multi(
        self,
        session: "AsyncSession",
        load_options: list | None = None,
        chunk_size: int = 1000,
    ) -> AsyncIterator[ModelType]:
        stmt = (
            select(
                ImportLogs.id,
                ImportLogs.target_table,
                ImportLogs.records_count,
                ImportLogs.created_at,
                User.first_name.label("user_first_name"),
                User.last_name.label("user_last_name"),
            )
            .join(User, self.model.uploaded_by == User.id)
            .order_by(ImportLogs.created_at.desc())
        )

        stream = await session.stream(stmt.execution_options(yield_per=chunk_size))
        async for row in stream:
            data = dict(row._mapping)
            data["created_at"] = format_date(data["created_at"])
            yield ImportLogFormattedResponse(**data)


import_log_service = ImportLogService(ImportLogs)
