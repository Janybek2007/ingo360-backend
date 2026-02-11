from typing import TYPE_CHECKING, Any, AsyncIterator, Sequence

from fastapi import HTTPException, UploadFile, status
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from src.db.models import Company, ImportLogs, Region, Settlement, geography
from src.mapping.geography import district_mapping
from src.schemas import geography as geography_schema
from src.services.base import BaseService
from src.utils.excel_parser import parse_excel_file
from src.utils.list_query_helper import InOrNullSpec, ListQueryHelper, StringTypedSpec
from src.utils.mapping import map_record

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


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
            stmt = ListQueryHelper.apply_specs(
                stmt,
                [
                    StringTypedSpec(self.model.name, filters.name),
                    InOrNullSpec(self.model.region_id, filters.region_ids),
                    InOrNullSpec(self.model.settlement_id, filters.settlement_ids),
                    InOrNullSpec(self.model.company_id, filters.company_ids),
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
