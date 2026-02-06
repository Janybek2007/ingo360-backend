import asyncio
from typing import TYPE_CHECKING, Any, Sequence

from fastapi import HTTPException, UploadFile, status
from sqlalchemy import select
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
from src.utils.mapping import map_record

from .base import BaseService
from .list_query_helper import ListQueryHelper

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


def _parse_csv_ids(value: str | None, field_name: str) -> list[int] | None:
    if not value:
        return None
    try:
        return [int(item.strip()) for item in value.split(",") if item.strip()]
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid {field_name}: expected comma-separated integers",
        ) from exc


class EmployeeService(
    BaseService[employees.Employee, employee.EmployeeCreate, employee.EmployeeUpdate]
):
    async def get_multi(
        self,
        session: "AsyncSession",
        filters: employee.EmployeeListRequest,
        load_options: list[Any] | None = None,
    ) -> Sequence[employees.Employee]:
        payload_filters = filters.filters
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        if payload_filters.full_name:
            stmt = stmt.where(
                self.model.full_name.ilike(f"%{payload_filters.full_name}%")
            )

        positions = _parse_csv_ids(payload_filters.positions, "positions")
        product_groups = _parse_csv_ids(
            payload_filters.product_groups, "product_groups"
        )
        regions = _parse_csv_ids(payload_filters.regions, "regions")
        districts = _parse_csv_ids(payload_filters.districts, "districts")
        companies = _parse_csv_ids(payload_filters.companies, "companies")

        stmt = ListQueryHelper.apply_in_or_null(stmt, self.model.position_id, positions)
        stmt = ListQueryHelper.apply_in_or_null(
            stmt, self.model.product_group_id, product_groups
        )
        stmt = ListQueryHelper.apply_in_or_null(stmt, self.model.region_id, regions)
        stmt = ListQueryHelper.apply_in_or_null(stmt, self.model.district_id, districts)
        stmt = ListQueryHelper.apply_in_or_null(stmt, self.model.company_id, companies)

        sort_map = {
            "full_name": self.model.full_name,
            "positions": self.model.position_id,
            "product_groups": self.model.product_group_id,
            "regions": self.model.region_id,
            "districts": self.model.district_id,
            "companies": self.model.company_id,
        }
        sort_payload = (
            {filters.sort_by: filters.sort_order}
            if filters.sort_by and filters.sort_order
            else None
        )
        stmt = ListQueryHelper.apply_sorting(
            stmt, sort_payload, sort_map, self.model.created_at.desc()
        )
        stmt = ListQueryHelper.apply_pagination(
            stmt, payload_filters.limit, payload_filters.offset
        )

        result = await session.execute(stmt)
        return result.unique().scalars().all()

    async def import_excel(
        self, session: "AsyncSession", file: "UploadFile", user_id: int
    ):
        records = await parse_excel_file(file)

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table="Сотрудники",
            records_count=len(records),
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

        return {
            "imported": len(data_to_insert),
            "skipped": len(skipped_records),
            "total": len(records),
            "skipped_records": skipped_records,
        }


class PositionService(
    BaseService[employees.Position, employee.PositionCreate, employee.PositionUpdate]
):
    async def get_multi(
        self,
        session: "AsyncSession",
        filters: employee.PositionListRequest,
        load_options: list[Any] | None = None,
    ) -> Sequence[employees.Position]:
        payload_filters = filters.filters
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        if payload_filters.name:
            stmt = stmt.where(self.model.name.ilike(f"%{payload_filters.name}%"))

        sort_payload = (
            {filters.sort_by: filters.sort_order}
            if filters.sort_by and filters.sort_order
            else None
        )
        stmt = ListQueryHelper.apply_sorting(
            stmt, sort_payload, {"name": self.model.name}, self.model.created_at.desc()
        )
        stmt = ListQueryHelper.apply_pagination(
            stmt, payload_filters.limit, payload_filters.offset
        )

        result = await session.execute(stmt)
        return result.unique().scalars().all()

    async def import_excel(
        self, session: "AsyncSession", file: "UploadFile", user_id: int
    ):
        records = await parse_excel_file(file)

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table="Должности МП",
            records_count=len(records),
        )
        session.add(import_log)
        await session.flush()

        data_to_insert = []
        for r in records:
            relation_fields = {
                "import_log_id": import_log.id,
            }
            data_to_insert.append(map_record(r, position_mapping, relation_fields))
        await session.execute(insert(self.model), data_to_insert)
        await session.commit()


employee_service = EmployeeService(employees.Employee)
position_service = PositionService(employees.Position)
