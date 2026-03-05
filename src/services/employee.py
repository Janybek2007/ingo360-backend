import asyncio
from typing import TYPE_CHECKING, Any, AsyncIterator

from fastapi import UploadFile
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from src.db.models import (
    Company,
    District,
    ImportLogs,
    Position,
    ProductGroup,
    Region,
    employees,
)
from src.mapping.employees import employee_mapping, position_mapping
from src.schemas import employee
from src.utils.excel_parser import parse_excel_file
from src.utils.import_result import build_import_result
from src.utils.list_query_helper import InOrNullSpec, ListQueryHelper, StringTypedSpec
from src.utils.mapping import map_record

from .base import BaseService

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

from src.schemas.base_filter import PaginatedResponse


class EmployeeService(
    BaseService[employees.Employee, employee.EmployeeCreate, employee.EmployeeUpdate]
):
    async def get_multi(
        self,
        session: "AsyncSession",
        filters: employee.EmployeeListRequest | None = None,
        load_options: list[Any] | None = None,
    ) -> PaginatedResponse[employee.EmployeeResponse]:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        sort_map = {
            "full_name": self.model.full_name,
            "positions": self.model.position_id,
            "product_groups": self.model.product_group_id,
            "regions": self.model.region_id,
            "districts": self.model.district_id,
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

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table="Сотрудники",
            records_count=len(records),
            target_table_name=self.model.__tablename__,
        )
        session.add(import_log)
        await session.flush()

        results = await asyncio.gather(
            self.get_id_map(session, Region, "name", {r["область"] for r in records}),
            self.get_id_map(session, Company, "name", {r["компания"] for r in records}),
            self.get_id_map(
                session, Position, "name", {r["должность"] for r in records}
            ),
            self.get_id_map(
                session, ProductGroup, "name", {r["группа"] for r in records}
            ),
            return_exceptions=True,
        )

        region_map, missing_regions = results[0]
        company_map, missing_companies = results[1]
        position_map, missing_positions = results[2]
        product_group_map, missing_product_groups = results[3]

        district_triples = {
            (r["район"], region_map.get(r["область"]), company_map.get(r["компания"]))
            for r in records
            if r["район"] is not None
            and r["область"] in region_map
            and r["компания"] in company_map
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
            missing_keys = []

            if r["область"] in missing_regions:
                missing_keys.append(f"область: {r['область']}")

            if r["компания"] in missing_companies:
                missing_keys.append(f"компания: {r['компания']}")

            if r["должность"] in missing_positions:
                missing_keys.append(f"должность: {r['должность']}")

            if r["группа"] in missing_product_groups:
                missing_keys.append(f"группа: {r['группа']}")

            region_id = region_map.get(r["область"])
            company_id = company_map.get(r["компания"])

            district_id = None
            if r["район"] is not None and region_id and company_id:
                district_key = (r["район"], region_id, company_id)
                if district_key in missing_districts:
                    missing_keys.append(f"район: {r['район']}")
                else:
                    district_id = district_map.get(district_key)

            if missing_keys:
                skipped_records.append({"row": idx + 1, "missing": missing_keys})
                continue

            relation_fields = {
                "company_id": company_id,
                "region_id": region_id,
                "position_id": position_map[r["должность"]],
                "product_group_id": product_group_map[r["группа"]],
                "district_id": district_id,
                "import_log_id": import_log.id,
            }
            data_to_insert.append(map_record(r, employee_mapping, relation_fields))

        if data_to_insert:
            stmt = insert(self.model).on_conflict_do_nothing()
            await session.execute(stmt, data_to_insert)

        await session.commit()

        return build_import_result(
            total=len(records),
            imported=len(data_to_insert),
            skipped_records=skipped_records,
            inserted=len(data_to_insert),
            deduplicated_in_batch=0,
        )


class PositionService(
    BaseService[employees.Position, employee.PositionCreate, employee.PositionUpdate]
):
    async def get_multi(
        self,
        session: "AsyncSession",
        filters: employee.PositionListRequest | None = None,
        load_options: list[Any] | None = None,
    ) -> PaginatedResponse[employee.PositionResponse]:
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

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table="Должности МП",
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
            data_to_insert.append(map_record(r, position_mapping, relation_fields))
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


employee_service = EmployeeService(employees.Employee)
position_service = PositionService(employees.Position)
