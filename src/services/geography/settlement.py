from typing import TYPE_CHECKING, Any, AsyncIterator, Sequence

from fastapi import HTTPException, UploadFile, status
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from src.db.models import ImportLogs, Region, geography
from src.mapping.geography import region_mapping
from src.schemas import geography as geography_schema
from src.services.base import BaseService
from src.utils.excel_parser import parse_excel_file
from src.utils.import_result import build_import_result
from src.utils.list_query_helper import InOrNullSpec, ListQueryHelper, StringTypedSpec
from src.utils.mapping import map_record

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


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

        region_map, missing_regions = await self.get_id_map(
            session, Region, "name", {r["область"] for r in records}
        )

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
            stmt = ListQueryHelper.apply_specs(
                stmt,
                [
                    StringTypedSpec(self.model.name, filters.name),
                    InOrNullSpec(self.model.region_id, filters.region_ids),
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
    ) -> AsyncIterator[geography.Settlement]:
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
