from typing import TYPE_CHECKING, Any, AsyncIterator

from fastapi import UploadFile
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from src.db.models import (
    District,
    ImportLogs,
    employees,
)
from src.import_fields import employee
from src.schemas import employee as employee_schema
from src.utils.excel_parser import parse_excel_file
from src.utils.import_result import build_import_result
from src.utils.list_query_helper import InOrNullSpec, ListQueryHelper, StringTypedSpec
from src.utils.records_resolver import resolve_records_fields
from src.utils.validate_required_columns import validate_required_columns

from .base import BaseService

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

from src.schemas.base_filter import PaginatedResponse


class EmployeeService(
    BaseService[
        employees.Employee,
        employee_schema.EmployeeCreate,
        employee_schema.EmployeeUpdate,
    ]
):
    async def get_multi(
        self,
        session: "AsyncSession",
        filters: employee_schema.EmployeeListRequest | None = None,
        load_options: list[Any] | None = None,
    ) -> PaginatedResponse[employee_schema.EmployeeResponse]:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        sort_map = {
            "full_name": self.model.full_name,
            "position": self.model.position_id,
            "product_group": self.model.product_group_id,
            "region": self.model.region_id,
            "district": self.model.district_id,
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
                    StringTypedSpec(self.model.position_id, filters.position_ids),
                    InOrNullSpec(
                        self.model.product_group_id, filters.product_group_ids
                    ),
                    InOrNullSpec(self.model.region_id, filters.region_ids),
                    InOrNullSpec(self.model.district_id, filters.district_ids),
                    InOrNullSpec(self.model.company_id, filters.company_ids),
                ],
            )

        # COUNT(*) OVER() counts filtered rows without a separate query
        stmt = stmt.add_columns(func.count().over().label("_total"))

        if filters:
            stmt = ListQueryHelper.apply_pagination(stmt, filters.limit, filters.offset)

        result = await session.execute(stmt)
        rows = result.all()
        total_count = int(rows[0]._total) if rows else 0
        items = [row[0] for row in rows]

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
    ) -> AsyncIterator[employees.Employee]:
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

        validate_required_columns(records, employee.employee_fields)

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table="Сотрудники",
            records_count=len(records),
            target_table_name=self.model.__tablename__,
        )
        session.add(import_log)
        await session.flush()

        resolved = await resolve_records_fields(
            session, records, employee.employee_fields, self.get_id_map
        )

        region_map = resolved.maps.get("область")
        company_map = resolved.maps["компания"]

        district_triples = {
            (
                r.get("район"),
                region_map.get(r.get("область")),
                company_map.get(r.get("компания")),
            )
            for r in records
            if r.get("район") is not None
            and r.get("область") in region_map
            and r.get("компания") in company_map
        }

        district_map = {}
        missing_districts = set()

        if district_triples:
            district_names = {t[0] for t in district_triples}
            region_ids = {t[1] for t in district_triples}
            company_ids = {t[2] for t in district_triples}

            stmt = select(District).where(
                District.name.in_(district_names),
                District.region_id.in_(region_ids),
                District.company_id.in_(company_ids),
            )
            result = await session.execute(stmt)
            districts = result.scalars().all()

            district_map = {
                (d.name, d.region_id, d.company_id): d.id for d in districts
            }
            missing_districts = district_triples - district_map.keys()

        skipped_records = []
        data_to_insert = []

        for idx, r in enumerate(records):
            missing_keys = resolved.collect_missing_keys(r, employee.employee_fields)

            ids, null_keys = resolved.resolve_id_fields(r, employee.employee_fields)
            if null_keys:
                missing_keys.extend(null_keys)

            region_id = ids.get("region_id")
            company_id = ids.get("company_id")

            district_id = None
            if r.get("район") is not None and region_id and company_id:
                district_key = (r.get("район"), region_id, company_id)
                if district_key in missing_districts:
                    missing_keys.append(f"район: {r['район']}")
                else:
                    district_id = district_map.get(district_key)

            if missing_keys:
                skipped_records.append({"row": idx + 1, "missing": missing_keys})
                continue

            data_to_insert.append(
                {
                    "full_name": r.get("фио"),
                    "company_id": ids.get("company_id"),
                    "region_id": ids.get("region_id"),
                    "position_id": ids.get("position_id"),
                    "product_group_id": ids.get("product_group_id"),
                    "district_id": district_id,
                    "import_log_id": import_log.id,
                }
            )

        inserted_ids = []
        try:
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
        except Exception:
            await session.rollback()
            raise

        return build_import_result(
            total=len(records),
            imported=len(inserted_ids),
            skipped_records=skipped_records,
            inserted=len(inserted_ids),
            deduplicated=len(data_to_insert) - len(inserted_ids),
        )


class PositionService(
    BaseService[
        employees.Position,
        employee_schema.PositionCreate,
        employee_schema.PositionUpdate,
    ]
):
    async def get_multi(
        self,
        session: "AsyncSession",
        filters: employee_schema.PositionListRequest | None = None,
        load_options: list[Any] | None = None,
    ) -> PaginatedResponse[employee_schema.PositionResponse]:
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
    ) -> AsyncIterator[employees.Position]:
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

        validate_required_columns(records, employee.position_fields)

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table="Должности МП",
            records_count=len(records),
            target_table_name=self.model.__tablename__,
        )
        session.add(import_log)
        await session.flush()

        await resolve_records_fields(
            session, records, employee.position_fields, self.get_id_map
        )

        data_to_insert = [
            {"name": r.get("название"), "import_log_id": import_log.id} for r in records
        ]

        inserted_ids = []
        try:
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
        except Exception:
            await session.rollback()
            raise

        return build_import_result(
            total=len(records),
            imported=len(inserted_ids),
            skipped_records=[],
            inserted=len(inserted_ids),
            deduplicated=len(data_to_insert) - len(inserted_ids),
        )


employee_service = EmployeeService(employees.Employee)
position_service = PositionService(employees.Position)
