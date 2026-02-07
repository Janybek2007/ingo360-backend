import asyncio
from typing import TYPE_CHECKING, Any, Sequence

from fastapi import HTTPException, UploadFile, status
from sqlalchemy import asc, desc, or_, select
from sqlalchemy.dialects.postgresql import insert

from src.db.models import (
    ClientCategory,
    Company,
    Distributor,
    District,
    Employee,
    GeoIndicator,
    ImportLogs,
    MedicalFacility,
    ProductGroup,
    Region,
    Settlement,
    Speciality,
    clients,
)
from src.mapping.clients import (
    client_category_mapping,
    distributor_mapping,
    doctor_mapping,
    geo_indicator_mapping,
    medical_facility_mapping,
    pharmacy_mapping,
    speciality_mapping,
)
from src.schemas import client
from src.utils.excel_parser import parse_excel_file
from src.utils.mapping import map_record

from .base import BaseService
from .list_query_helper import ListQueryHelper

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


class ClientCategoryService(
    BaseService[
        clients.ClientCategory, client.ClientCategoryCreate, client.ClientCategoryUpdate
    ]
):
    async def get_multi(
        self,
        session: "AsyncSession",
        filters: client.ClientCategoryListRequest,
        load_options: list[Any] | None = None,
    ) -> Sequence[clients.ClientCategory]:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        if filters.name:
            stmt = stmt.where(self.model.name.ilike(f"%{filters.name}%"))

        sort_payload = (
            {filters.sort_by: filters.sort_order}
            if filters.sort_by and filters.sort_order
            else None
        )
        stmt = ListQueryHelper.apply_sorting(
            stmt, sort_payload, {"name": self.model.name}, self.model.created_at.desc()
        )
        stmt = ListQueryHelper.apply_pagination(stmt, filters.limit, filters.offset)

        result = await session.execute(stmt)
        return result.unique().scalars().all()

    async def import_excel(
        self, session: "AsyncSession", file: "UploadFile", user_id: int
    ):
        records = await parse_excel_file(file)

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table="Категории клиентов",
            records_count=len(records),
        )
        session.add(import_log)
        await session.flush()

        data_to_insert = []
        for r in records:
            relation_fields = {
                "import_log_id": import_log.id,
            }
            data_to_insert.append(
                map_record(r, client_category_mapping, relation_fields)
            )
        await session.execute(insert(self.model), data_to_insert)
        await session.commit()


class DoctorService(
    BaseService[clients.Doctor, client.DoctorCreate, client.DoctorUpdate]
):
    async def get_multi(
        self,
        session: "AsyncSession",
        filters: client.DoctorListRequest,
        load_options: list[Any] | None = None,
    ) -> Sequence[clients.Doctor]:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        full_name_filter = filters.full_name
        if full_name_filter:
            stmt = stmt.where(self.model.full_name.ilike(f"%{full_name_filter}%"))

        stmt = ListQueryHelper.apply_in_or_null(
            stmt, self.model.medical_facility_id, filters.medical_facility_ids
        )
        stmt = ListQueryHelper.apply_in_or_null(
            stmt, self.model.responsible_employee_id, filters.responsible_employee_ids
        )
        stmt = ListQueryHelper.apply_in_or_null(
            stmt, self.model.speciality_id, filters.speciality_ids
        )
        stmt = ListQueryHelper.apply_in_or_null(
            stmt, self.model.client_category_id, filters.client_category_ids
        )
        stmt = ListQueryHelper.apply_in_or_null(
            stmt, self.model.product_group_id, filters.product_group_ids
        )

        sort_map = {
            "full_name": self.model.full_name,
            "medical_facility": self.model.medical_facility_id,
            "responsible_employee": self.model.responsible_employee_id,
            "speciality": self.model.speciality_id,
            "client_category": self.model.client_category_id,
            "product_group": self.model.product_group_id,
        }
        sort_payload = (
            {filters.sort_by: filters.sort_order}
            if filters.sort_by and filters.sort_order
            else None
        )
        stmt = ListQueryHelper.apply_sorting(
            stmt, sort_payload, sort_map, self.model.created_at.desc()
        )
        stmt = ListQueryHelper.apply_pagination(stmt, filters.limit, filters.offset)

        result = await session.execute(stmt)
        return result.unique().scalars().all()

    async def import_excel(
        self, session: "AsyncSession", file: "UploadFile", user_id: int
    ):
        records = await parse_excel_file(file)

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table="Врачи",
            records_count=len(records),
        )
        session.add(import_log)
        await session.flush()

        for r in records:
            if "фио врача" in r and "фио" not in r:
                r["фио"] = r.get("фио врача")
            if (
                "ответственный сотрудник" in r
                and "фио ответственного сотрудника" not in r
            ):
                r["фио ответственного сотрудника"] = r.get("ответственный сотрудник")
            if (
                "фио ответственного сотрудника" in r
                and "ответственный сотрудник" not in r
            ):
                r["ответственный сотрудник"] = r.get("фио ответственного сотрудника")

        speciality_map, missing_specialities = await self.get_id_map(
            session, Speciality, "name", {r["специальность"] for r in records}
        )
        client_category_map, missing_client_categories = await self.get_id_map(
            session, ClientCategory, "name", {r["категория"] for r in records}
        )

        employee_values = {
            r["фио ответственного сотрудника"]
            for r in records
            if r["фио ответственного сотрудника"] is not None
        }
        employee_map, missing_employees = (
            await self.get_id_map(session, Employee, "full_name", employee_values)
            if employee_values
            else ({}, set())
        )

        medical_facility_values = {r["лпу"] for r in records if r["лпу"] is not None}
        medical_facility_map, missing_medical_facilities = (
            await self.get_id_map(
                session, MedicalFacility, "name", medical_facility_values
            )
            if medical_facility_values
            else ({}, set())
        )

        product_group_values = {r["группа"] for r in records if r["группа"] is not None}
        product_group_map, missing_product_groups = (
            await self.get_id_map(session, ProductGroup, "name", product_group_values)
            if product_group_values
            else ({}, set())
        )

        skipped_records = []
        data_to_insert = []

        for idx, r in enumerate(records):
            missing_keys = []

            if r["специальность"] in missing_specialities:
                missing_keys.append(f"специальность: {r['специальность']}")

            if r["категория"] in missing_client_categories:
                missing_keys.append(f"категория: {r['категория']}")

            if (
                r["фио ответственного сотрудника"]
                and r["фио ответственного сотрудника"] in missing_employees
            ):
                missing_keys.append(f"сотрудник: {r['фио ответственного сотрудника']}")

            if r["лпу"] and r["лпу"] in missing_medical_facilities:
                missing_keys.append(f"ЛПУ: {r['лпу']}")

            if r["группа"] and r["группа"] in missing_product_groups:
                missing_keys.append(f"группа: {r['группа']}")

            if missing_keys:
                skipped_records.append({"row": idx + 1, "missing": missing_keys})
                continue

            relation_fields = {
                "responsible_employee_id": employee_map.get(
                    r["фио ответственного сотрудника"]
                ),
                "medical_facility_id": medical_facility_map.get(r["лпу"]),
                "speciality_id": speciality_map[r["специальность"]],
                "client_category_id": client_category_map[r["категория"]],
                "product_group_id": product_group_map.get(r["группа"]),
                "import_log_id": import_log.id,
            }
            data_to_insert.append(map_record(r, doctor_mapping, relation_fields))

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


class PharmacyService(
    BaseService[clients.Pharmacy, client.PharmacyCreate, client.PharmacyUpdate]
):
    async def get_multi(
        self,
        session: "AsyncSession",
        filters: client.PharmacyListRequest,
        load_options: list[Any] | None = None,
    ) -> Sequence[clients.Pharmacy]:
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

        sort_payload = (
            {filters.sort_by: filters.sort_order}
            if filters.sort_by and filters.sort_order
            else None
        )
        stmt = ListQueryHelper.apply_sorting(
            stmt, sort_payload, sort_map, self.model.id.desc()
        )

        if filters.name:
            stmt = stmt.where(self.model.name.ilike(f"%{filters.name}%"))

        stmt = ListQueryHelper.apply_in_or_null(
            stmt, self.model.company_id, filters.company_ids
        )
        stmt = ListQueryHelper.apply_in_or_null(
            stmt, self.model.distributor_id, filters.distributor_ids
        )
        stmt = ListQueryHelper.apply_in_or_null(
            stmt, self.model.responsible_employee_id, filters.responsible_employee_ids
        )
        stmt = ListQueryHelper.apply_in_or_null(
            stmt, self.model.settlement_id, filters.settlement_ids
        )
        stmt = ListQueryHelper.apply_in_or_null(
            stmt, self.model.district_id, filters.district_ids
        )
        stmt = ListQueryHelper.apply_in_or_null(
            stmt, self.model.client_category_id, filters.client_category_ids
        )
        stmt = ListQueryHelper.apply_in_or_null(
            stmt, self.model.product_group_id, filters.product_group_ids
        )
        stmt = ListQueryHelper.apply_in_or_null(
            stmt, self.model.geo_indicator_id, filters.geo_indicator_ids
        )

        stmt = ListQueryHelper.apply_pagination(stmt, filters.limit, filters.offset)

        result = await session.execute(stmt)
        return result.unique().scalars().all()

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

        return {
            "imported": len(data_to_insert),
            "skipped": len(skipped_records),
            "total": len(records),
            "skipped_records": skipped_records,
        }


class SpecialityService(
    BaseService[clients.Speciality, client.SpecialityCreate, client.SpecialityUpdate]
):
    async def get_multi(
        self,
        session: "AsyncSession",
        filters: client.SpecialityListRequest,
        load_options: list[Any] | None = None,
    ) -> Sequence[clients.Speciality]:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        if filters.name:
            stmt = stmt.where(self.model.name.ilike(f"%{filters.name}%"))

        sort_payload = (
            {filters.sort_by: filters.sort_order}
            if filters.sort_by and filters.sort_order
            else None
        )
        stmt = ListQueryHelper.apply_sorting(
            stmt, sort_payload, {"name": self.model.name}, self.model.created_at.desc()
        )
        stmt = ListQueryHelper.apply_pagination(stmt, filters.limit, filters.offset)

        result = await session.execute(stmt)
        return result.unique().scalars().all()

    async def import_excel(
        self, session: "AsyncSession", file: "UploadFile", user_id: int
    ):
        records = await parse_excel_file(file)

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table="Специальности",
            records_count=len(records),
        )
        session.add(import_log)
        await session.flush()

        data_to_insert = []
        for r in records:
            relation_fields = {
                "import_log_id": import_log.id,
            }
            data_to_insert.append(map_record(r, speciality_mapping, relation_fields))
        await session.execute(insert(self.model), data_to_insert)
        await session.commit()


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

        if filters.name:
            stmt = stmt.where(self.model.name.ilike(f"%{filters.name}%"))

        if filters.facility_type:
            stmt = stmt.where(
                self.model.facility_type.ilike(f"%{filters.facility_type}%")
            )

        stmt = ListQueryHelper.apply_in_or_null(
            stmt, self.model.settlement_id, filters.settlement_ids
        )
        stmt = ListQueryHelper.apply_in_or_null(
            stmt, self.model.district_id, filters.district_ids
        )
        stmt = ListQueryHelper.apply_in_or_null(
            stmt, self.model.geo_indicator_id, filters.geo_indicator_ids
        )

        sort_map = {
            "name": self.model.name,
            "facility_type": self.model.facility_type,
            "settlements": self.model.settlement_id,
            "districts": self.model.district_id,
            "geo_indicators": self.model.geo_indicator_id,
        }
        sort_payload = (
            {filters.sort_by: filters.sort_order}
            if filters.sort_by and filters.sort_order
            else None
        )
        stmt = ListQueryHelper.apply_sorting(
            stmt, sort_payload, sort_map, self.model.created_at.desc()
        )
        stmt = ListQueryHelper.apply_pagination(stmt, filters.limit, filters.offset)

        result = await session.execute(stmt)
        return result.unique().scalars().all()

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

        return {
            "imported": len(data_to_insert),
            "skipped": len(skipped_records),
            "total": len(records),
            "skipped_records": skipped_records,
        }


class DistributorService(
    BaseService[clients.Distributor, client.DistributorCreate, client.DistributorUpdate]
):
    async def get_multi(
        self,
        session: "AsyncSession",
        filters: client.DistributorListRequest,
        load_options: list[Any] | None = None,
    ) -> Sequence[clients.Distributor]:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        if filters.name:
            stmt = stmt.where(self.model.name.ilike(f"%{filters.name}%"))

        sort_payload = (
            {filters.sort_by: filters.sort_order}
            if filters.sort_by and filters.sort_order
            else None
        )
        stmt = ListQueryHelper.apply_sorting(
            stmt, sort_payload, {"name": self.model.name}, self.model.created_at.desc()
        )
        stmt = ListQueryHelper.apply_pagination(stmt, filters.limit, filters.offset)

        result = await session.execute(stmt)
        return result.unique().scalars().all()

    async def import_excel(
        self, session: "AsyncSession", file: "UploadFile", user_id: int
    ):
        records = await parse_excel_file(file)

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table="Дистрибьюторы",
            records_count=len(records),
        )
        session.add(import_log)
        await session.flush()

        data_to_insert = []
        for r in records:
            relation_fields = {
                "import_log_id": import_log.id,
            }
            data_to_insert.append(map_record(r, distributor_mapping, relation_fields))
        await session.execute(insert(self.model), data_to_insert)
        await session.commit()


class GeoIndicatorService(
    BaseService[GeoIndicator, client.GeoIndicatorCreate, client.GeoIndicatorUpdate]
):
    async def get_multi(
        self,
        session: "AsyncSession",
        filters: client.GeoIndicatorListRequest,
        load_options: list[Any] | None = None,
    ) -> Sequence[GeoIndicator]:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        if filters.name:
            stmt = stmt.where(self.model.name.ilike(f"%{filters.name}%"))

        sort_payload = (
            {filters.sort_by: filters.sort_order}
            if filters.sort_by and filters.sort_order
            else None
        )
        stmt = ListQueryHelper.apply_sorting(
            stmt, sort_payload, {"name": self.model.name}, self.model.created_at.desc()
        )
        stmt = ListQueryHelper.apply_pagination(stmt, filters.limit, filters.offset)

        result = await session.execute(stmt)
        return result.unique().scalars().all()

    async def import_excel(
        self, session: "AsyncSession", file: "UploadFile", user_id: int
    ):
        records = await parse_excel_file(file)

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table="Индикаторы Аптек/ЛПУ",
            records_count=len(records),
        )
        session.add(import_log)
        await session.flush()

        data_to_insert = []
        for r in records:
            relation_fields = {
                "import_log_id": import_log.id,
            }
            data_to_insert.append(map_record(r, geo_indicator_mapping, relation_fields))
        await session.execute(insert(self.model), data_to_insert)
        await session.commit()


client_category_service = ClientCategoryService(clients.ClientCategory)
doctor_service = DoctorService(clients.Doctor)
pharmacy_service = PharmacyService(clients.Pharmacy)
speciality_service = SpecialityService(clients.Speciality)
medical_facility_service = MedicalFacilityService(clients.MedicalFacility)
distributor_service = DistributorService(clients.Distributor)
geo_indicator_service = GeoIndicatorService(GeoIndicator)
