from typing import TYPE_CHECKING, Any, AsyncIterator, Sequence

from fastapi import UploadFile
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from src.db.models import (
    ClientCategory,
    Employee,
    ImportLogs,
    MedicalFacility,
    ProductGroup,
    Speciality,
    clients,
    Company
)
from src.mapping.clients import doctor_mapping
from src.schemas import client
from src.services.base import BaseService
from src.utils.excel_parser import parse_excel_file
from src.utils.import_result import build_import_result
from src.utils.list_query_helper import InOrNullSpec, ListQueryHelper, StringTypedSpec
from src.utils.mapping import map_record

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


class DoctorService(
    BaseService[clients.Doctor, client.DoctorCreate, client.DoctorUpdate]
):
    async def get_multi(
        self,
        session: "AsyncSession",
        filters: client.DoctorListRequest | None = None,
        load_options: list[Any] | None = None,
    ) -> Sequence[clients.Doctor]:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        sort_map = {
            "full_name": self.model.full_name,
            "medical_facility": self.model.medical_facility_id,
            "responsible_employee": self.model.responsible_employee_id,
            "speciality": self.model.speciality_id,
            "client_category": self.model.client_category_id,
            "product_group": self.model.product_group_id,
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
                    StringTypedSpec(self.model.full_name, filters.full_name),
                    InOrNullSpec(
                        self.model.medical_facility_id, filters.medical_facility_ids
                    ),
                    InOrNullSpec(
                        self.model.responsible_employee_id,
                        filters.responsible_employee_ids,
                    ),
                    InOrNullSpec(self.model.speciality_id, filters.speciality_ids),
                    InOrNullSpec(
                        self.model.client_category_id, filters.client_category_ids
                    ),
                    InOrNullSpec(
                        self.model.product_group_id, filters.product_group_ids
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
    ) -> AsyncIterator[clients.Doctor]:
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
            target_table="Врачи",
            records_count=len(records),
            target_table_name=self.model.__tablename__,
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

        company_map, missing_companies = await self.get_id_map(
            session, Company, "name", {r["компания"] for r in records}
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

            if r["компания"] in missing_companies:
                missing_keys.append(f"компания: {r['компания']}")

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
                "company_id": company_map[r["компания"]],
                "import_log_id": import_log.id,
            }
            data_to_insert.append(map_record(r, doctor_mapping, relation_fields))

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
