from typing import TYPE_CHECKING, Any, AsyncIterator

from fastapi import HTTPException, UploadFile, status
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from src.db.models import (
    ClientCategory,
    Company,
    Distributor,
    District,
    Employee,
    GeoIndicator,
    ImportLogs,
    ProductGroup,
    Region,
    Settlement,
    clients,
)
from src.mapping.clients import pharmacy_mapping
from src.schemas import client
from src.services.base import BaseService
from src.utils.excel_parser import parse_excel_file
from src.utils.import_result import build_import_result
from src.utils.list_query_helper import InOrNullSpec, ListQueryHelper, StringTypedSpec
from src.utils.mapping import map_record

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

from src.schemas.base_filter import PaginatedResponse


class PharmacyService(
    BaseService[clients.Pharmacy, client.PharmacyCreate, client.PharmacyUpdate]
):
    async def get_multi(
        self,
        session: "AsyncSession",
        filters: client.PharmacyListRequest | None = None,
        load_options: list[Any] | None = None,
    ) -> PaginatedResponse[client.PharmacyResponse]:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        sort_map = {
            "name": self.model.name,
            "companies": self.model.company_id,
            "distributors": self.model.distributor_id,
            "responsible_employees": self.model.responsible_employee_id,
            "settlements": self.model.settlement_id,
            "districts": self.model.district_id,
            "client_categories": self.model.client_category_id,
            "product_groups": self.model.product_group_id,
            "geo_indicators": self.model.geo_indicator_id,
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

        for r in records:
            if "дистрибьютор/сеть" in r and "дистрибьютор / сеть" not in r:
                r["дистрибьютор / сеть"] = r.get("дистрибьютор/сеть")
            if (
                "ответственный сотрудник" in r
                and "фио ответственного сотрудника" not in r
            ):
                r["фио ответственного сотрудника"] = r.get("ответственный сотрудник")

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table="Аптеки",
            records_count=len(records),
            target_table_name=self.model.__tablename__,
        )
        session.add(import_log)
        await session.flush()

        has_region_column = any("область" in r for r in records)
        has_company_column = any("компания" in r for r in records)
        has_product_group_column = any("группа" in r for r in records)

        if not has_company_column:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="В файле отсутствует обязательная колонка 'компания'",
            )

        if not has_product_group_column:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="В файле отсутствует обязательная колонка 'группа'",
            )

        group_values = {r.get("группа") for r in records if r.get("группа") is not None}
        company_values = {
            r.get("компания") for r in records if r.get("компания") is not None
        }
        region_values = (
            {r.get("область") for r in records if r.get("область") is not None}
            if has_region_column
            else set()
        )

        if group_values:
            product_group_map, missing_product_groups = await self.get_id_map(
                session, ProductGroup, "name", group_values
            )
        else:
            product_group_map, missing_product_groups = ({}, set())

        if company_values:
            company_map, missing_companies = await self.get_id_map(
                session, Company, "name", company_values
            )
        else:
            company_map, missing_companies = ({}, set())

        if region_values:
            region_map, missing_regions = await self.get_id_map(
                session, Region, "name", region_values
            )
        else:
            region_map, missing_regions = ({}, set())

        employee_values = {
            r.get("фио ответственного сотрудника")
            for r in records
            if r.get("фио ответственного сотрудника") is not None
        }
        employee_map, missing_employees = (
            await self.get_id_map(session, Employee, "full_name", employee_values)
            if employee_values
            else ({}, set())
        )

        client_category_values = {
            r.get("категория") for r in records if r.get("категория") is not None
        }
        client_category_map, missing_client_categories = (
            await self.get_id_map(
                session, ClientCategory, "name", client_category_values
            )
            if client_category_values
            else ({}, set())
        )

        distributor_values = {
            r.get("дистрибьютор / сеть")
            for r in records
            if r.get("дистрибьютор / сеть") is not None
        }
        distributor_map, missing_distributors = (
            await self.get_id_map(session, Distributor, "name", distributor_values)
            if distributor_values
            else ({}, set())
        )

        geo_indicator_values = {
            r.get("индикатор") for r in records if r.get("индикатор") is not None
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
            if has_region_column and region_map
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
            group_name = r.get("группа")
            company_name = r.get("компания")

            if not group_name:
                missing_keys.append("группа: значение не заполнено")

            if group_name in missing_product_groups:
                missing_keys.append(f"группа: {group_name}")

            if not company_name:
                missing_keys.append("компания: значение не заполнено")

            if company_name in missing_companies:
                missing_keys.append(f"компания: {company_name}")

            if has_region_column and r.get("область") in missing_regions:
                missing_keys.append(f"область: {r['область']}")

            if (
                r.get("фио ответственного сотрудника")
                and r.get("фио ответственного сотрудника") in missing_employees
            ):
                missing_keys.append(f"сотрудник: {r['фио ответственного сотрудника']}")

            if r.get("категория") and r.get("категория") in missing_client_categories:
                missing_keys.append(f"категория: {r['категория']}")

            if (
                r.get("дистрибьютор / сеть")
                and r.get("дистрибьютор / сеть") in missing_distributors
            ):
                missing_keys.append(f"дистрибьютор: {r['дистрибьютор / сеть']}")

            if r.get("индикатор") and r.get("индикатор") in missing_geo_indicators:
                missing_keys.append(f"индикатор: {r['индикатор']}")

            region_id = region_map.get(r.get("область")) if has_region_column else None
            company_id = company_map.get(r.get("компания"))

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
                "responsible_employee_id": employee_map.get(
                    r.get("фио ответственного сотрудника")
                ),
                "client_category_id": client_category_map.get(r.get("категория")),
                "product_group_id": product_group_map.get(group_name),
                "distributor_id": distributor_map.get(r.get("дистрибьютор / сеть")),
                "district_id": district_id,
                "settlement_id": settlement_id,
                "company_id": company_id,
                "import_log_id": import_log.id,
                "geo_indicator_id": geo_indicator_map.get(r.get("индикатор")),
            }
            data_to_insert.append(map_record(r, pharmacy_mapping, relation_fields))

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
