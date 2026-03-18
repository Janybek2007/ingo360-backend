from typing import TYPE_CHECKING, Any, AsyncIterator

from fastapi import UploadFile
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from src.db.models import (
    District,
    ImportLogs,
    Settlement,
    clients,
)
from src.import_fields import client
from src.schemas import client as client_schema
from src.services.base import BaseService
from src.utils.excel_parser import parse_excel_file
from src.utils.import_result import build_import_result
from src.utils.list_query_helper import InOrNullSpec, ListQueryHelper, StringTypedSpec
from src.utils.records_resolver import resolve_records_fields
from src.utils.validate_required_columns import validate_required_columns

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

from src.schemas.base_filter import PaginatedResponse


class PharmacyService(
    BaseService[
        clients.Pharmacy, client_schema.PharmacyCreate, client_schema.PharmacyUpdate
    ]
):
    async def get_multi(
        self,
        session: "AsyncSession",
        filters: client_schema.PharmacyListRequest | None = None,
        load_options: list[Any] | None = None,
    ) -> PaginatedResponse[client_schema.PharmacyResponse]:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        sort_map = {
            "name": self.model.name,
            "company": self.model.company_id,
            "distributor": self.model.distributor_id,
            "responsible_employe": self.model.responsible_employee_id,
            "settlement": self.model.settlement_id,
            "district": self.model.district_id,
            "client_category": self.model.client_category_id,
            "product_group": self.model.product_group_id,
            "geo_indicator": self.model.geo_indicator_id,
        }

        stmt = ListQueryHelper.apply_sorting_with_default(
            stmt,
            getattr(filters, "sort_by", None),
            getattr(filters, "sort_order", None),
            sort_map,
            self.model.id.desc(),
        )

        if filters:
            stmt = ListQueryHelper.apply_specs(
                stmt,
                [
                    StringTypedSpec(self.model.name, filters.name),
                    InOrNullSpec(self.model.company_id, filters.company_ids),
                    InOrNullSpec(self.model.distributor_id, filters.distributor_ids),
                    InOrNullSpec(
                        self.model.responsible_employee_id,
                        filters.responsible_employee_ids,
                    ),
                    InOrNullSpec(self.model.settlement_id, filters.settlement_ids),
                    InOrNullSpec(self.model.district_id, filters.district_ids),
                    InOrNullSpec(
                        self.model.client_category_id, filters.client_category_ids
                    ),
                    InOrNullSpec(
                        self.model.product_group_id, filters.product_group_ids
                    ),
                    InOrNullSpec(
                        self.model.geo_indicator_id, filters.geo_indicator_ids
                    ),
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
    ) -> AsyncIterator[clients.Pharmacy]:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        stmt = ListQueryHelper.apply_sorting_with_created(
            stmt,
            self.model.id.desc(),
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
        validate_required_columns(records, client.pharmacy_fields)

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table="Аптеки",
            records_count=len(records),
            target_table_name=self.model.__tablename__,
        )
        session.add(import_log)
        await session.flush()

        resolved = await resolve_records_fields(
            session, records, client.pharmacy_fields, self.get_id_map
        )

        region_map = resolved.maps.get("область", {})
        company_map = resolved.maps["компания"]

        settlement_pairs = {
            (r.get("населенный пункт"), region_map.get(r.get("область")))
            for r in records
            if r.get("населенный пункт") is not None and r.get("область") in region_map
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
            stmt = select(District).where(
                District.name.in_({t[0] for t in district_triples}),
                District.region_id.in_({t[1] for t in district_triples}),
                District.company_id.in_({t[2] for t in district_triples}),
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
            missing_keys = resolved.collect_missing_keys(r, client.pharmacy_fields)

            ids, null_keys = resolved.resolve_id_fields(r, client.pharmacy_fields)
            if null_keys:
                missing_keys.extend(null_keys)

            region_id = ids.get("region_id")
            company_id = ids.get("company_id")

            settlement_id = None
            if r.get("населенный пункт") is not None:
                if region_id:
                    settlement_key = (r.get("населенный пункт"), region_id)
                    if settlement_key in missing_settlements:
                        missing_keys.append(
                            f"населенный пункт: {r.get('населенный пункт')}"
                        )
                    else:
                        settlement_id = settlement_map.get(settlement_key)
                else:
                    if r.get("населенный пункт") in missing_settlements:
                        missing_keys.append(
                            f"населенный пункт: {r.get('населенный пункт')}"
                        )
                    else:
                        settlement_id = settlement_map.get(r.get("населенный пункт"))

            district_id = None
            if r.get("район") is not None and region_id and company_id:
                district_key = (r.get("район"), region_id, company_id)
                if district_key in missing_districts:
                    missing_keys.append(f"район: {r.get('район')}")
                else:
                    district_id = district_map.get(district_key)

            if missing_keys:
                skipped_records.append({"row": idx + 1, "missing": missing_keys})
                continue

            data_to_insert.append(
                {
                    "name": r.get("название"),
                    "company_id": company_id,
                    "product_group_id": ids.get("product_group_id"),
                    "client_category_id": ids.get("client_category_id"),
                    "distributor_id": ids.get("distributor_id"),
                    "responsible_employee_id": ids.get("responsible_employee_id"),
                    "settlement_id": settlement_id,
                    "district_id": district_id,
                    "geo_indicator_id": ids.get("geo_indicator_id"),
                    "import_log_id": import_log.id,
                }
            )

        BATCH_SIZE = 3000
        inserted_ids = []
        if data_to_insert:
            for i in range(0, len(data_to_insert), BATCH_SIZE):
                batch = data_to_insert[i:i + BATCH_SIZE]
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
