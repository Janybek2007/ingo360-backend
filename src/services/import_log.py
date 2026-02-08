from typing import Any, Sequence, TYPE_CHECKING

from sqlalchemy import select

from .base import BaseService, FilterSchemaType, ModelType
from src.db.models import ImportLogs, User
from src.schemas.import_log import ImportLogCreate, ImportLogUpdate, ImportLogResponse

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


class ImportLogService(BaseService[ImportLogs, ImportLogCreate, ImportLogUpdate]):
    async def get_multi(
        self,
        session: "AsyncSession",
        filters: FilterSchemaType | None = None,
        load_options: list[Any] | None = None,
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

        if filters.limit:
            stmt = stmt.limit(filters.limit)
        if filters.offset:
            stmt = stmt.offset(filters.offset)

        result = await session.execute(stmt)
        return [ImportLogResponse(**row) for row in result.mappings().all()]


import_log_service = ImportLogService(ImportLogs)
