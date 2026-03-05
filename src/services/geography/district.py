from typing import TYPE_CHECKING, Any, AsyncIterator

from fastapi import HTTPException, UploadFile, status
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from src.db.models import Company, ImportLogs, Region, Settlement, geography
from src.mapping.geography import district_mapping
from src.schemas import geography as geography_schema
from src.services.base import BaseService
from src.utils.excel_parser import parse_excel_file
from src.utils.import_result import build_import_result
from src.utils.list_query_helper import InOrNullSpec, ListQueryHelper, StringTypedSpec
from src.utils.mapping import map_record

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

from src.schemas.base_filter import PaginatedResponse


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
            target_table_name=self.model.__tablename__,
        )
        session.add(import_log)
        await session.flush()

        region_map, missing_regions = await self.get_id_map(
            session, Region, "name", {r["область"] for r in records}
        )
        company_map, missing_companies = await self.get_id_map(
            session, Company, "name", {r["компания"] for r in records}
        )

        settlement_pairs = {
            (r["населенный пункт"], region_map.get(r["область"]))
            for r in records
            if r["населенный пункт"] is not None and r["область"] in region_map
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

        skipped_records = []
        data_to_insert = []

        for idx, r in enumerate(records):
            missing_keys = []

            if r["область"] in missing_regions:
                missing_keys.append(f"область: {r['область']}")
            if r["компания"] in missing_companies:
                missing_keys.append(f"компания: {r['компания']}")

            region_id = region_map.get(r["область"])
            if r["населенный пункт"] is not None and region_id:
                settlement_key = (r["населенный пункт"], region_id)
                if settlement_key in missing_settlements:
                    missing_keys.append(f"населенный пункт: {r['населенный пункт']}")

            if missing_keys:
                skipped_records.append({"row": idx + 1, "missing": missing_keys})
                continue

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
    ) -> PaginatedResponse[geography_schema.DistrictResponse]:
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
            stmt = ListQueryHelper.apply_specs(
                stmt,
                [
                    StringTypedSpec(self.model.name, filters.name),
                    InOrNullSpec(self.model.region_id, filters.region_ids),
                    InOrNullSpec(self.model.settlement_id, filters.settlement_ids),
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
    ) -> AsyncIterator[geography.District]:
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
