from typing import TYPE_CHECKING, Any, AsyncIterator

from fastapi import UploadFile
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from src.db.models import (
    ImportLogs,
    clients,
)
from src.import_fields import client
from src.schemas.client import (
    DoctorCreate,
    DoctorListRequest,
    DoctorResponse,
    DoctorUpdate,
)
from src.services.base import BaseService
from src.utils.excel_parser import parse_excel_file
from src.utils.import_result import build_import_result
from src.utils.list_query_helper import InOrNullSpec, ListQueryHelper, StringTypedSpec
from src.utils.records_resolver import resolve_records_fields
from src.utils.validate_required_columns import validate_required_columns

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

from src.schemas.base_filter import PaginatedResponse


class DoctorService(BaseService[clients.Doctor, DoctorCreate, DoctorUpdate]):
    async def get_multi(
        self,
        session: "AsyncSession",
        filters: DoctorListRequest | None = None,
        load_options: list[Any] | None = None,
    ) -> PaginatedResponse[DoctorResponse]:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        sort_map = {
            "full_name": self.model.full_name,
            "medical_facility": self.model.medical_facility_id,
            "responsible_employee": self.model.responsible_employee_id,
            "speciality": self.model.speciality_id,
            "client_category": self.model.client_category_id,
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
                    StringTypedSpec(self.model.full_name, filters.full_name),
                    InOrNullSpec(
                        self.model.medical_facility_id, filters.medical_facility_ids
                    ),
                    InOrNullSpec(
                        self.model.responsible_employee_id,
                        filters.responsible_employee_ids,
                    ),
                    InOrNullSpec(self.model.speciality_id, filters.speciality_ids),
                    InOrNullSpec(
                        self.model.client_category_id, filters.client_category_ids
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
    ) -> AsyncIterator[clients.Doctor]:
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

        validate_required_columns(records, client.doctor_fields)

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table="Врачи",
            records_count=len(records),
            target_table_name=self.model.__tablename__,
        )
        session.add(import_log)
        await session.flush()

        resolved = await resolve_records_fields(
            session, records, client.doctor_fields, self.get_id_map
        )

        skipped_records = []
        data_to_insert = []

        for idx, r in enumerate(records):
            missing_keys = resolved.collect_missing_keys(r, client.doctor_fields)

            ids, null_keys = resolved.resolve_id_fields(r, client.doctor_fields)
            if null_keys:
                missing_keys.extend(null_keys)

            if missing_keys:
                skipped_records.append({"row": idx + 1, "missing": missing_keys})
                continue

            data_to_insert.append(
                {
                    "full_name": r.get("фио"),
                    "company_id": ids.get("company_id"),
                    "medical_facility_id": ids.get("medical_facility_id"),
                    "speciality_id": ids.get("speciality_id"),
                    "product_group_id": ids.get("product_group_id"),
                    "client_category_id": ids.get("client_category_id"),
                    "responsible_employee_id": ids.get("responsible_employee_id"),
                    "import_log_id": import_log.id,
                }
            )

        BATCH_SIZE = 3000
        inserted_ids = []
        if data_to_insert:
            for i in range(0, len(data_to_insert), BATCH_SIZE):
                batch = data_to_insert[i : i + BATCH_SIZE]
                stmt = (
                    insert(self.model)
                    .values(batch)
                    .on_conflict_do_nothing()
                    .returning(self.model.id)
                )
                result = await session.execute(stmt)
                inserted_ids.extend(result.scalars().all())

        await session.commit()

        return build_import_result(
            total=len(records),
            imported=len(inserted_ids),
            skipped_records=skipped_records,
            inserted=len(inserted_ids),
            deduplicated=len(data_to_insert) - len(inserted_ids),
        )
