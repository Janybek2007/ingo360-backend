from typing import TYPE_CHECKING, Any, AsyncIterator, Sequence

from fastapi import HTTPException, UploadFile, status
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from src.db.models import Country, ImportLogs, geography
from src.mapping.geography import region_mapping
from src.schemas import geography as geography_schema
from src.services.base import BaseService
from src.utils.excel_parser import parse_excel_file
from src.utils.import_result import build_import_result
from src.utils.list_query_helper import InOrNullSpec, ListQueryHelper, StringTypedSpec
from src.utils.mapping import map_record

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


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
            target_table_name=self.model.__tablename__,
        )
        session.add(import_log)
        await session.flush()

        country_map, missing_countries = await self.get_id_map(
            session, Country, "name", {r["страна"] for r in records}
        )

        skipped_records = []
        data_to_insert = []

        for idx, r in enumerate(records):
            missing_keys = []

            if r["страна"] in missing_countries:
                missing_keys.append(f"страна: {r['страна']}")

            if missing_keys:
                skipped_records.append({"row": idx + 1, "missing": missing_keys})
                continue

            relation_fields = {
                "country_id": country_map[r["страна"]],
                "import_log_id": import_log.id,
            }
            data_to_insert.append(map_record(r, region_mapping, relation_fields))
        stmt = insert(self.model).on_conflict_do_nothing()
        if data_to_insert:
            await session.execute(stmt, data_to_insert)
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
            stmt = ListQueryHelper.apply_specs(
                stmt,
                [
                    StringTypedSpec(self.model.name, filters.name),
                    InOrNullSpec(self.model.country_id, filters.country_ids),
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
    ) -> AsyncIterator[geography.Region]:
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
