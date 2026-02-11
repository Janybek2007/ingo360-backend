from typing import TYPE_CHECKING, Any, AsyncIterator, Sequence

from fastapi import UploadFile
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from src.db.models import (
    Company,
    District,
    GeoIndicator,
    ImportLogs,
    Region,
    Settlement,
    clients,
)
from src.mapping.clients import medical_facility_mapping
from src.schemas import client
from src.services.base import BaseService
from src.utils.excel_parser import parse_excel_file
from src.utils.import_result import build_import_result
from src.utils.list_query_helper import InOrNullSpec, ListQueryHelper, StringTypedSpec
from src.utils.mapping import map_record

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


class MedicalFacilityService(
    BaseService[
        clients.MedicalFacility,
        client.MedicalFacilityCreate,
        client.MedicalFacilityUpdate,
    ]
):
    async def get_multi(
        self,
        session: "AsyncSession",
        filters: client.MedicalFacilityListRequest,
        load_options: list[Any] | None = None,
    ) -> Sequence[clients.MedicalFacility]:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        sort_map = {
            "name": self.model.name,
            "facility_type": self.model.facility_type,
            "settlements": self.model.settlement_id,
            "districts": self.model.district_id,
            "geo_indicators": self.model.geo_indicator_id,
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
                    StringTypedSpec(self.model.name, filters.name),
                    StringTypedSpec(self.model.facility_type, filters.facility_type),
                    InOrNullSpec(self.model.settlement_id, filters.settlement_ids),
                    InOrNullSpec(self.model.district_id, filters.district_ids),
                    InOrNullSpec(
                        self.model.geo_indicator_id, filters.geo_indicator_ids
                    ),
                ],
            )

            stmt = ListQueryHelper.apply_pagination(stmt, filters.limit, filters.offset)

        result = await session.execute(stmt)
        return result.unique().scalars().all()

    async def iter_multi(
        self,
        session: "AsyncSession",
        load_options: list[Any] | None = None,
        chunk_size: int = 1000,
    ) -> AsyncIterator[clients.MedicalFacility]:
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
            target_table="ЛПУ",
            records_count=len(records),
        )
        session.add(import_log)
        await session.flush()

        has_region_column = any("область" in r for r in records)
        has_company_column = any("компания" in r for r in records)

        region_values = (
            {r.get("область") for r in records if r.get("область") is not None}
            if has_region_column
            else set()
        )
        company_values = (
            {r.get("компания") for r in records if r.get("компания") is not None}
            if has_company_column
            else set()
        )

        if region_values:
            region_map, missing_regions = await self.get_id_map(
                session, Region, "name", region_values
            )
        else:
            region_map, missing_regions = ({}, set())

        if company_values:
            company_map, missing_companies = await self.get_id_map(
                session, Company, "name", company_values
            )
        else:
            company_map, missing_companies = ({}, set())

        geo_indicator_values = {
            r["индикатор"] for r in records if r["индикатор"] is not None
        }
        geo_indicator_map, missing_geo_indicators = (
            await self.get_id_map(session, GeoIndicator, "name", geo_indicator_values)
            if geo_indicator_values
            else ({}, set())
        )

        if has_region_column and region_map:
            settlement_pairs = {
                (r.get("населенный пункт"), region_map.get(r.get("область")))
                for r in records
                if r.get("населенный пункт") is not None
                and r.get("область") in region_map
            }
            settlement_map, missing_settlements = (
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
        else:
            settlement_values = {
                r.get("населенный пункт")
                for r in records
                if r.get("населенный пункт") is not None
            }
            settlement_map, missing_settlements = (
                await self.get_id_map(session, Settlement, "name", settlement_values)
                if settlement_values
                else ({}, set())
            )

        district_triples = (
            {
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
            if has_region_column and has_company_column and region_map and company_map
            else set()
        )

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

            if has_region_column and r.get("область") in missing_regions:
                missing_keys.append(f"область: {r['область']}")

            if has_company_column and r.get("компания") in missing_companies:
                missing_keys.append(f"компания: {r['компания']}")

            if r["индикатор"] and r["индикатор"] in missing_geo_indicators:
                missing_keys.append(f"индикатор: {r['индикатор']}")

            region_id = region_map.get(r.get("область")) if has_region_column else None
            company_id = (
                company_map.get(r.get("компания")) if has_company_column else None
            )

            settlement_id = None
            if r.get("населенный пункт") is not None:
                if has_region_column and region_id:
                    settlement_key = (r.get("населенный пункт"), region_id)
                    if settlement_key in missing_settlements:
                        missing_keys.append(
                            f"населенный пункт: {r['населенный пункт']}"
                        )
                    else:
                        settlement_id = settlement_map.get(settlement_key)
                else:
                    if r.get("населенный пункт") in missing_settlements:
                        missing_keys.append(
                            f"населенный пункт: {r['населенный пункт']}"
                        )
                    else:
                        settlement_id = settlement_map.get(r.get("населенный пункт"))

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

            relation_fields = {
                "district_id": district_id,
                "settlement_id": settlement_id,
                "import_log_id": import_log.id,
                "geo_indicator_id": geo_indicator_map.get(r["индикатор"]),
            }
            data_to_insert.append(
                map_record(r, medical_facility_mapping, relation_fields)
            )

        if data_to_insert:
            stmt = insert(self.model).on_conflict_do_nothing()
            await session.execute(stmt, data_to_insert)

        await session.commit()

        return build_import_result(
            total=len(records),
            imported=len(data_to_insert),
            skipped_records=skipped_records,
        )
