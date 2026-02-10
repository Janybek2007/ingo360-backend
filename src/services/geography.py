from typing import TYPE_CHECKING, Any, Sequence

from fastapi import HTTPException, UploadFile, status
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from src.db.models import Company, Country, ImportLogs, Region, Settlement, geography
from src.mapping.geography import district_mapping, region_mapping
from src.schemas import geography as geography_schema
from src.utils.excel_parser import parse_excel_file
from src.utils.import_result import build_import_result
from src.utils.mapping import map_record

from .base import BaseService
from .list_query_helper import ListQueryHelper

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


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

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table="Страны",
            records_count=len(records),
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
        await session.execute(stmt, data_to_insert)
        await session.commit()

    async def get_multi(
        self,
        session: "AsyncSession",
        filters: geography_schema.CountryListRequest | None = None,
        load_options: list[Any] | None = None,
    ) -> Sequence[geography.Country]:
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
                stmt = stmt.where(self.model.name.ilike(f"%{filters.name}%"))

            stmt = ListQueryHelper.apply_pagination(stmt, filters.limit, filters.offset)

        result = await session.execute(stmt)
        return result.unique().scalars().all()


class RegionService(
    BaseService[
        geography.Region, geography_schema.RegionCreate, geography_schema.RegionUpdate
    ]
):
    async def import_excel(
        self, session: "AsyncSession", file: "UploadFile", user_id: int
    ):
        records = await parse_excel_file(file)

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table="Области",
            records_count=len(records),
        )
        session.add(import_log)
        await session.flush()

        country_map, _ = await self.get_id_map(
            session, Country, "name", {r["страна"] for r in records}
        )

        data_to_insert = []
        for r in records:
            relation_fields = {
                "country_id": country_map[r["страна"]],
                "import_log_id": import_log.id,
            }
            data_to_insert.append(map_record(r, region_mapping, relation_fields))
        stmt = insert(self.model).on_conflict_do_nothing()
        await session.execute(stmt, data_to_insert)
        await session.commit()

    @staticmethod
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

    async def get_multi(
        self,
        session: "AsyncSession",
        filters: geography_schema.RegionListRequest | None = None,
        load_options: list[Any] | None = None,
    ) -> Sequence[geography.Region]:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        sort_map = {
            "name": self.model.name,
            "country": self.model.country_id,
        }
        stmt = ListQueryHelper.apply_sorting_with_default(
            stmt,
            getattr(filters, "sort_by", None),
            getattr(filters, "sort_order", None),
            sort_map,
            self.model.created_at.desc(),
        )
        if filters:
            stmt = ListQueryHelper.apply_in_or_null(
                stmt, self.model.country_id, filters.country_ids
            )

            if filters.name:
                stmt = stmt.where(self.model.name.ilike(f"%{filters.name}%"))

            stmt = ListQueryHelper.apply_pagination(stmt, filters.limit, filters.offset)

        result = await session.execute(stmt)
        return result.unique().scalars().all()


class SettlementService(
    BaseService[
        geography.Settlement,
        geography_schema.SettlementCreate,
        geography_schema.SettlementUpdate,
    ]
):
    async def import_excel(
        self, session: "AsyncSession", file: "UploadFile", user_id: int
    ):
        records = await parse_excel_file(file)

        for r in records:
            if "name" in r and "название" not in r:
                r["название"] = r.get("name")
            if "region.name" in r and "область" not in r:
                r["область"] = r.get("region.name")

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table="Населенные пункты",
            records_count=len(records),
        )
        session.add(import_log)
        await session.flush()

        region_map, _ = await self.get_id_map(
            session, Region, "name", {r["область"] for r in records}
        )

        missing_regions = {r["область"] for r in records} - set(region_map.keys())
        skipped_records = []
        data_to_insert = []

        for idx, r in enumerate(records):
            missing_keys = []

            if r["область"] in missing_regions:
                missing_keys.append(f"область: {r['область']}")

            if missing_keys:
                skipped_records.append({"row": idx + 1, "missing": missing_keys})
                continue

            relation_fields = {
                "region_id": region_map[r["область"]],
                "import_log_id": import_log.id,
            }
            data_to_insert.append(map_record(r, region_mapping, relation_fields))

        if data_to_insert:
            stmt = insert(self.model).on_conflict_do_nothing()
            await session.execute(stmt, data_to_insert)

        await session.commit()

        return build_import_result(
            total=len(records),
            imported=len(data_to_insert),
            skipped_records=skipped_records,
        )

    @staticmethod
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

    async def get_multi(
        self,
        session: "AsyncSession",
        filters: geography_schema.SettlementListRequest | None = None,
        load_options: list[Any] | None = None,
    ) -> Sequence[geography.Settlement]:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        sort_map = {
            "name": self.model.name,
            "region": self.model.region_id,
        }
        stmt = ListQueryHelper.apply_sorting_with_default(
            stmt,
            getattr(filters, "sort_by", None),
            getattr(filters, "sort_order", None),
            sort_map,
            self.model.created_at.desc(),
        )

        if filters:
            if filters.name:
                stmt = stmt.where(self.model.name.ilike(f"%{filters.name}%"))

            stmt = ListQueryHelper.apply_in_or_null(
                stmt, self.model.region_id, filters.region_ids
            )

            stmt = ListQueryHelper.apply_pagination(stmt, filters.limit, filters.offset)

        result = await session.execute(stmt)
        return result.unique().scalars().all()


class DistrictService(
    BaseService[
        geography.District,
        geography_schema.DistrictCreate,
        geography_schema.DistrictUpdate,
    ]
):
    async def import_excel(
        self, session: "AsyncSession", file: "UploadFile", user_id: int
    ):
        records = await parse_excel_file(file)

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table="Районы",
            records_count=len(records),
        )
        session.add(import_log)
        await session.flush()

        region_map, _ = await self.get_id_map(
            session, Region, "name", {r["область"] for r in records}
        )
        company_map, _ = await self.get_id_map(
            session, Company, "name", {r["компания"] for r in records}
        )

        settlement_pairs = {
            (r["населенный пункт"], region_map[r["область"]])
            for r in records
            if r["населенный пункт"] is not None
        }

        settlement_map, _ = (
            await self.get_id_map(
                session,
                Settlement,
                "name",
                settlement_pairs,
                filter_field="region_id",
                filter_values=set(region_map.values()),
            )
            if settlement_pairs
            else ({}, set())
        )

        data_to_insert = []
        for r in records:
            region_id = region_map[r["область"]]

            settlement_id = None
            if r["населенный пункт"] is not None:
                settlement_key = (r["населенный пункт"], region_id)
                settlement_id = settlement_map.get(settlement_key)

            relation_fields = {
                "company_id": company_map[r["компания"]],
                "region_id": region_id,
                "settlement_id": settlement_id,
                "import_log_id": import_log.id,
            }
            data_to_insert.append(map_record(r, district_mapping, relation_fields))
        await session.execute(insert(self.model), data_to_insert)
        await session.commit()

    @staticmethod
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

    async def get_multi(
        self,
        session: "AsyncSession",
        filters: geography_schema.DistrictListRequest | None = None,
        load_options: list[Any] | None = None,
    ) -> Sequence[geography.District]:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        sort_map = {
            "name": self.model.name,
            "region": self.model.region_id,
            "settlement": self.model.settlement_id,
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
            if filters.name:
                stmt = stmt.where(self.model.name.ilike(f"%{filters.name}%"))

            stmt = ListQueryHelper.apply_in_or_null(
                stmt, self.model.region_id, filters.region_ids
            )
            stmt = ListQueryHelper.apply_in_or_null(
                stmt, self.model.settlement_id, filters.settlement_ids
            )
            stmt = ListQueryHelper.apply_in_or_null(
                stmt, self.model.company_id, filters.company_ids
            )

            stmt = ListQueryHelper.apply_pagination(stmt, filters.limit, filters.offset)

        result = await session.execute(stmt)
        return result.unique().scalars().all()


country_service = CountryService(geography.Country)
region_service = RegionService(geography.Region)
settlement_service = SettlementService(geography.Settlement)
district_service = DistrictService(geography.District)
