import os
import asyncio
from uuid import uuid4
from pathlib import Path
from typing import TYPE_CHECKING

from fastapi import UploadFile
from sqlalchemy import String, and_, asc, desc, func, insert, or_, select

from src.db.models import (
    ClientCategory,
    Doctor,
    Employee,
    GeoIndicator,
    ImportLogs,
    MedicalFacility,
    Pharmacy,
    ProductGroup,
    Speciality,
    Visit,
)
from src.mapping.visits import visit_mapping
from src.schemas import visit
from src.utils.excel_parser import iter_excel_records
from src.utils.mapping import map_record

from .base import BaseService

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


class VisitService(BaseService[Visit, visit.VisitCreate, visit.VisitUpdate]):
    async def import_sales(self, session: "AsyncSession", file: "UploadFile", user_id: int, batch_size: int = 2000):
        from src.tasks.sale_imports import import_sales_task

        upload_dir = Path("temp_uploads")
        upload_dir.mkdir(exist_ok=True)
        file_path = upload_dir / f"{uuid4()}_{file.filename}"

        try:
            with open(file_path, "wb") as f:
                content = await file.read()
                f.write(content)

            task = import_sales_task.delay(
                file_path=str(file_path),
                user_id=user_id,
                service_path="src.services.visit.VisitService",
                model_path="src.db.models.Visit",
                batch_size=batch_size,
            )
            return {"task_id": task.id}
        except Exception:
            if file_path.exists(): os.remove(file_path)
            raise

    async def _import_excel_from_file(
        self,
        session: "AsyncSession",
        file_path: str,
        user_id: int,
        batch_size: int = 2000
    ):
        with open(file_path, "rb") as f:
            from tempfile import SpooledTemporaryFile
            temp = SpooledTemporaryFile(max_size=50 * 1024 * 1024)
            temp.write(f.read())
            temp.seek(0)

        try:
            product_group_names = set()
            employee_names = set()
            doctor_names = set()
            medical_facilities = set()
            pharmacies = set()
            total_records = 0

            for _, r in iter_excel_records(temp):
                total_records += 1
                if r.get("группа"): product_group_names.add(r["группа"])
                if r.get("сотрудник"): employee_names.add(r["сотрудник"])
                if r.get("врач"): doctor_names.add(r["врач"])

                if r.get("тип клиента") == "Врач" and r.get("учреждение"):
                    medical_facilities.add(r["учреждение"])
                elif r.get("тип клиента") == "Аптека" and r.get("учреждение"):
                    pharmacies.add(r["учреждение"])

            import_log = ImportLogs(
                uploaded_by=user_id, target_table="Визиты", records_count=total_records
            )
            session.add(import_log)
            await session.flush()

            results = await asyncio.gather(
                self.get_id_map(session, ProductGroup, "name", product_group_names),
                self.get_id_map(session, Employee, "full_name", employee_names),
                self.get_id_map(session, Doctor, "full_name", doctor_names) if doctor_names else asyncio.sleep(0, ({}, set())),
                self.get_id_map(session, MedicalFacility, "name", medical_facilities) if medical_facilities else asyncio.sleep(0, ({}, set())),
                self.get_id_map(session, Pharmacy, "name", pharmacies) if pharmacies else asyncio.sleep(0, ({}, set())),
            )

            pg_map, pg_miss = results[0]
            emp_map, emp_miss = results[1]
            doc_map, doc_miss = results[2]
            mf_map, mf_miss = results[3]
            ph_map, ph_miss = results[4]

            skipped_records = []
            data_to_insert = []
            imported_count = 0
            temp.seek(0)

            for idx, r in iter_excel_records(temp):
                missing_keys = []

                if r["группа"] in pg_miss: missing_keys.append(f"группа: {r['группа']}")
                if r["сотрудник"] in emp_miss: missing_keys.append(f"сотрудник: {r['сотрудник']}")
                if r.get("врач") in doc_miss: missing_keys.append(f"врач: {r['врач']}")

                m_facility_id = None
                p_id = None

                if r.get("тип клиента") == "Врач":
                    if r.get("учреждение") in mf_miss:
                        missing_keys.append(f"учреждение (ЛПУ): {r['учреждение']}")
                    else:
                        m_facility_id = mf_map.get(r.get("учреждение"))
                elif r.get("тип клиента") == "Аптека":
                    if r.get("учреждение") in ph_miss:
                        missing_keys.append(f"учреждение (Аптека): {r['учреждение']}")
                    else:
                        p_id = ph_map.get(r.get("учреждение"))

                if missing_keys:
                    skipped_records.append({"row": idx, "missing": missing_keys})
                    continue

                relation_fields = {
                    "product_group_id": pg_map[r["группа"]],
                    "doctor_id": doc_map.get(r.get("врач")),
                    "employee_id": emp_map[r["сотрудник"]],
                    "medical_facility_id": m_facility_id,
                    "pharmacy_id": p_id,
                    "import_log_id": import_log.id,
                }
                data_to_insert.append(map_record(r, visit_mapping, relation_fields))

                if len(data_to_insert) >= batch_size:
                    await session.execute(insert(self.model), data_to_insert)
                    imported_count += len(data_to_insert)
                    data_to_insert = []

            if data_to_insert:
                await session.execute(insert(self.model), data_to_insert)
                imported_count += len(data_to_insert)

            await session.commit()
            return {
                "imported": imported_count,
                "skipped": len(skipped_records),
                "total": total_records,
                "skipped_records": skipped_records,
            }
        finally:
            temp.close()

    @staticmethod
    async def get_doctor_count_by_speciality(
        session: "AsyncSession",
        filters: visit.DoctorsCountFilter,
        company_id: int | None,
    ) -> list[visit.DoctorsBySpecialtyResponse]:
        quarter_expr = func.ceil(Visit.month / 3.0)

        doctors_with_visits_subquery = (
            select(
                Doctor.speciality_id,
                func.count(func.distinct(Doctor.id)).label("doctors_with_visits"),
            )
            .select_from(Visit)
            .join(Employee, Visit.employee_id == Employee.id)
            .join(Doctor, Visit.doctor_id == Doctor.id)
            .where(
                Visit.doctor_id.is_not(None),
            )
        )

        if company_id:
            doctors_with_visits_subquery = doctors_with_visits_subquery.where(
                Employee.company_id == company_id
            )
        if filters.speciality_ids:
            doctors_with_visits_subquery = doctors_with_visits_subquery.where(
                Doctor.speciality_id.in_(filters.speciality_ids)
            )
        if filters.medical_facility_ids:
            doctors_with_visits_subquery = doctors_with_visits_subquery.where(
                Visit.medical_facility_id.in_(filters.medical_facility_ids)
            )
        if filters.quarters:
            doctors_with_visits_subquery = doctors_with_visits_subquery.where(
                quarter_expr.in_(filters.quarters)
            )
        if filters.months:
            doctors_with_visits_subquery = doctors_with_visits_subquery.where(
                Visit.month.in_(filters.months),
            )
        if filters.years:
            doctors_with_visits_subquery = doctors_with_visits_subquery.where(
                Visit.year.in_(filters.years),
            )

        doctors_with_visits_subquery = doctors_with_visits_subquery.group_by(
            Doctor.speciality_id
        ).subquery()

        stmt = (
            select(
                Speciality.id.label("speciality_id"),
                Speciality.name.label("speciality_name"),
                func.count(Doctor.id).label("total_count"),
                func.coalesce(
                    doctors_with_visits_subquery.c.doctors_with_visits, 0
                ).label("count_with_visits"),
                (
                    func.coalesce(doctors_with_visits_subquery.c.doctors_with_visits, 0)
                    * 100.0
                    / func.count(Doctor.id)
                ).label("coverage_percentage"),
            )
            .select_from(Doctor)
            .join(Speciality, Doctor.speciality_id == Speciality.id)
            .outerjoin(
                doctors_with_visits_subquery,
                Speciality.id == doctors_with_visits_subquery.c.speciality_id,
            )
            .where(Doctor.company_id == 3)
        )

        if filters.speciality_ids:
            stmt = stmt.where(Speciality.id.in_(filters.speciality_ids))
        if filters.medical_facility_ids:
            stmt = stmt.where(
                Doctor.medical_facility_id.in_(filters.medical_facility_ids)
            )
        if filters.search:
            search_term = f"%{filters.search}%"
            stmt = stmt.where(Speciality.name.ilike(search_term))

        stmt = stmt.group_by(
            Speciality.id,
            Speciality.name,
            doctors_with_visits_subquery.c.doctors_with_visits,
        )

        result = await session.execute(stmt)
        return [visit.DoctorsBySpecialtyResponse(**row) for row in result.mappings()]

    @staticmethod
    async def get_doctor_count_with_visits_by_speciality(
        session: "AsyncSession",
        filters: visit.DoctorsCountWithVisitFilter,
        company_id: int | None,
    ):

        dimension_mapping = {
            "medical_facility": {
                "id": MedicalFacility.id.label("medical_facility_id"),
                "name": MedicalFacility.name.label("medical_facility_name"),
                "group_fields": [MedicalFacility.id, MedicalFacility.name],
            },
            "speciality": {
                "id": Speciality.id.label("speciality_id"),
                "name": Speciality.name.label("speciality_name"),
                "group_fields": [Speciality.id, Speciality.name],
            },
            "doctor": {
                "id": Doctor.id.label("doctor_id"),
                "name": Doctor.full_name.label("doctor_name"),
                "group_fields": [Doctor.id, Doctor.full_name],
            },
        }

        select_fields = []
        group_by_fields = []

        for dim in filters.group_by_dimensions:
            dim_config = dimension_mapping[dim]
            select_fields.extend([dim_config["id"], dim_config["name"]])
            group_by_fields.extend(dim_config["group_fields"])

        all_doctors_subquery = (
            select(
                *select_fields,
                func.count(func.distinct(Doctor.id)).label("total_doctors"),
            )
            .select_from(Doctor)
            .join(Speciality, Doctor.speciality_id == Speciality.id)
            .join(MedicalFacility, Doctor.medical_facility_id == MedicalFacility.id)
        )

        if filters.speciality_ids:
            all_doctors_subquery = all_doctors_subquery.where(
                Speciality.id.in_(filters.speciality_ids)
            )
        if filters.medical_facility_ids:
            all_doctors_subquery = all_doctors_subquery.where(
                MedicalFacility.id.in_(filters.medical_facility_ids)
            )
        if filters.doctor_ids:
            all_doctors_subquery = all_doctors_subquery.where(
                Doctor.id.in_(filters.doctor_ids)
            )

        if filters.search:
            search_term = f"%{filters.search}%"
            search_conditions = []
            if "medical_facility" in filters.group_by_dimensions:
                search_conditions.append(MedicalFacility.name.ilike(search_term))
            if "speciality" in filters.group_by_dimensions:
                search_conditions.append(Speciality.name.ilike(search_term))
            if "doctor" in filters.group_by_dimensions:
                search_conditions.append(Doctor.full_name.ilike(search_term))

            if search_conditions:
                all_doctors_subquery = all_doctors_subquery.where(
                    or_(*search_conditions)
                )

        all_doctors_subquery = all_doctors_subquery.group_by(
            *group_by_fields
        ).subquery()

        visit_select_fields = []
        visit_group_by_fields = []

        for dim in filters.group_by_dimensions:
            if dim == "medical_facility":
                visit_select_fields.append(
                    MedicalFacility.id.label("medical_facility_id")
                )
                visit_group_by_fields.append(MedicalFacility.id)
            elif dim == "speciality":
                visit_select_fields.append(Speciality.id.label("speciality_id"))
                visit_group_by_fields.append(Speciality.id)
            elif dim == "doctor":
                visit_select_fields.append(Doctor.id.label("doctor_id"))
                visit_group_by_fields.append(Doctor.id)

        doctors_with_visits_subquery = (
            select(
                *visit_select_fields,
                func.count(func.distinct(Doctor.id)).label("doctors_with_visits"),
            )
            .select_from(Visit)
            .join(Employee, Visit.employee_id == Employee.id)
            .join(Doctor, Visit.doctor_id == Doctor.id)
            .join(Speciality, Doctor.speciality_id == Speciality.id)
            .join(MedicalFacility, Doctor.medical_facility_id == MedicalFacility.id)
            .where(
                Visit.month.in_(filters.months),
                Visit.year.in_(filters.years),
                Visit.doctor_id.is_not(None),
            )
        )

        if company_id:
            doctors_with_visits_subquery = doctors_with_visits_subquery.where(
                Employee.company_id == company_id
            )
        if filters.speciality_ids:
            doctors_with_visits_subquery = doctors_with_visits_subquery.where(
                Speciality.id.in_(filters.speciality_ids)
            )
        if filters.medical_facility_ids:
            doctors_with_visits_subquery = doctors_with_visits_subquery.where(
                MedicalFacility.id.in_(filters.medical_facility_ids)
            )
        if filters.doctor_ids:
            doctors_with_visits_subquery = doctors_with_visits_subquery.where(
                Doctor.id.in_(filters.doctor_ids)
            )

        if filters.search:
            search_term = f"%{filters.search}%"
            search_conditions = []
            if "medical_facility" in filters.group_by_dimensions:
                search_conditions.append(MedicalFacility.name.ilike(search_term))
            if "speciality" in filters.group_by_dimensions:
                search_conditions.append(Speciality.name.ilike(search_term))
            if "doctor" in filters.group_by_dimensions:
                search_conditions.append(Doctor.full_name.ilike(search_term))

            if search_conditions:
                doctors_with_visits_subquery = doctors_with_visits_subquery.where(
                    or_(*search_conditions)
                )

        doctors_with_visits_subquery = doctors_with_visits_subquery.group_by(
            *visit_group_by_fields
        ).subquery()

        join_conditions = []
        for dim in filters.group_by_dimensions:
            if dim == "medical_facility":
                join_conditions.append(
                    all_doctors_subquery.c.medical_facility_id
                    == doctors_with_visits_subquery.c.medical_facility_id
                )
            elif dim == "speciality":
                join_conditions.append(
                    all_doctors_subquery.c.speciality_id
                    == doctors_with_visits_subquery.c.speciality_id
                )
            elif dim == "doctor":
                join_conditions.append(
                    all_doctors_subquery.c.doctor_id
                    == doctors_with_visits_subquery.c.doctor_id
                )

        final_select_fields = []
        final_group_by_fields = []

        for dim in filters.group_by_dimensions:
            final_select_fields.extend(
                [
                    getattr(all_doctors_subquery.c, f"{dim}_id"),
                    getattr(all_doctors_subquery.c, f"{dim}_name"),
                ]
            )
            final_group_by_fields.extend(
                [
                    getattr(all_doctors_subquery.c, f"{dim}_id"),
                    getattr(all_doctors_subquery.c, f"{dim}_name"),
                ]
            )

        stmt = (
            select(
                *final_select_fields,
                func.coalesce(
                    doctors_with_visits_subquery.c.doctors_with_visits, 0
                ).label("doctors_with_visits"),
                all_doctors_subquery.c.total_doctors,
                (
                    func.coalesce(doctors_with_visits_subquery.c.doctors_with_visits, 0)
                    * 100.0
                    / all_doctors_subquery.c.total_doctors
                ).label("coverage_percentage"),
            )
            .select_from(all_doctors_subquery)
            .outerjoin(
                doctors_with_visits_subquery,
                and_(*join_conditions) if join_conditions else True,
            )
        )

        sort_map = {
            "medical_facility": getattr(
                all_doctors_subquery.c, "medical_facility_name", None
            ),
            "doctor": getattr(all_doctors_subquery.c, "doctor_name", None),
            "speciality": getattr(all_doctors_subquery.c, "speciality_name", None),
            "total_doctors": all_doctors_subquery.c.total_doctors,
            "doctors_with_visits": func.coalesce(
                doctors_with_visits_subquery.c.doctors_with_visits, 0
            ),
        }

        if filters.sort_by and filters.sort_order:
            sort_column = sort_map.get(filters.sort_by)
            if sort_column is not None:
                stmt = stmt.order_by(
                    asc(sort_column)
                    if filters.sort_order == "ASC"
                    else desc(sort_column)
                )
        else:
            order_by_fields = []
            for dim in filters.group_by_dimensions:
                order_by_fields.append(getattr(all_doctors_subquery.c, f"{dim}_name"))

            if order_by_fields:
                stmt = stmt.order_by(*order_by_fields)

        if filters.limit:
            stmt = stmt.limit(filters.limit)
        if filters.offset:
            stmt = stmt.offset(filters.offset)

        result = await session.execute(stmt)
        return result.mappings().all()

    @staticmethod
    async def get_visits_sum_for_period(
        session: "AsyncSession",
        filters: visit.VisitSumForPeriodFilter,
        company_id: int | None,
    ):
        dimension_mapping = {
            "pharmacy": {
                "id_field": Visit.pharmacy_id,
                "name_field": Pharmacy.name,
                "id_label": "pharmacy_id",
                "name_label": "pharmacy",
                "join_table": Pharmacy,
                "join_condition": lambda: Visit.pharmacy_id == Pharmacy.id,
                "join_type": "outerjoin",
            },
            "medical_facility": {
                "id_field": Visit.medical_facility_id,
                "name_field": MedicalFacility.name,
                "id_label": "medical_facility_id",
                "name_label": "medical_facility",
                "join_table": MedicalFacility,
                "join_condition": lambda: Visit.medical_facility_id
                == MedicalFacility.id,
                "join_type": "outerjoin",
            },
            "year": {
                "id_field": Visit.year,
                "name_field": None,
                "id_label": "year",
                "name_label": None,
                "join_table": None,
                "join_type": None,
            },
            "month": {
                "id_field": Visit.month,
                "name_field": None,
                "id_label": "month",
                "name_label": None,
                "join_table": None,
                "join_type": None,
            },
            "employee": {
                "id_field": Visit.employee_id,
                "name_field": Employee.full_name,
                "id_label": "employee_id",
                "name_label": "employee",
                "join_table": Employee,
                "join_condition": lambda: Visit.employee_id == Employee.id,
                "join_type": "join",
            },
            "product_group": {
                "id_field": Visit.product_group_id,
                "name_field": ProductGroup.name,
                "id_label": "product_group_id",
                "name_label": "product_group",
                "join_table": ProductGroup,
                "join_condition": lambda: Visit.product_group_id == ProductGroup.id,
                "join_type": "join",
            },
            "geo_indicator": {
                "id_field": GeoIndicator.id,
                "name_field": GeoIndicator.name,
                "id_label": "indicator_id",
                "name_label": "indicator_name",
                "join_table": GeoIndicator,
                "join_condition": lambda: or_(
                    Pharmacy.geo_indicator_id == GeoIndicator.id,
                    MedicalFacility.geo_indicator_id == GeoIndicator.id,
                ),
                "join_type": "outerjoin",
            },
            "speciality": {
                "id_field": Speciality.id,
                "name_field": Speciality.name,
                "id_label": "speciality_id",
                "name_label": "speciality_name",
                "join_table": Speciality,
                "join_condition": lambda: Doctor.speciality_id == Speciality.id,
                "join_type": "outerjoin",
                "requires": ["doctor"],
            },
            "doctor": {
                "id_field": None,
                "name_field": Doctor.full_name,
                "id_label": None,
                "name_label": "doctor_name",
                "join_table": Doctor,
                "join_condition": lambda: Visit.doctor_id == Doctor.id,
                "join_type": "outerjoin",
            },
        }

        select_fields = []
        group_by_fields = []

        for dim in filters.group_by_dimensions:
            dim_config = dimension_mapping[dim]

            if dim_config["id_field"] is not None:
                select_fields.append(
                    dim_config["id_field"].label(dim_config["id_label"])
                )
                group_by_fields.append(dim_config["id_field"])

            if dim_config["name_field"] is not None:
                select_fields.append(
                    dim_config["name_field"].label(dim_config["name_label"])
                )
                group_by_fields.append(dim_config["name_field"])

        select_fields.append(func.count(Visit.id).label("employee_visits"))

        stmt = select(*select_fields).select_from(Visit)

        tables_to_join = set()
        for dim in filters.group_by_dimensions:
            dim_config = dimension_mapping[dim]
            if dim_config["join_table"] is not None:
                tables_to_join.add(dim)
                if "requires" in dim_config:
                    for req in dim_config["requires"]:
                        tables_to_join.add(req)

        join_order = [
            "employee",
            "product_group",
            "pharmacy",
            "medical_facility",
            "doctor",
            "speciality",
            "geo_indicator",
        ]

        for table_name in join_order:
            if table_name in tables_to_join:
                dim_config = dimension_mapping[table_name]
                join_table = dim_config["join_table"]
                join_condition = dim_config["join_condition"]()
                join_type = dim_config["join_type"]

                if join_type == "join":
                    stmt = stmt.join(join_table, join_condition)
                else:
                    stmt = stmt.outerjoin(join_table, join_condition)

        if filters.years:
            stmt = stmt.where(Visit.year.in_(filters.years))
        if company_id:
            stmt = stmt.where(Employee.company_id == company_id)
        if filters.months:
            stmt = stmt.where(Visit.month.in_(filters.months))
        if filters.pharmacy_ids:
            stmt = stmt.where(Visit.pharmacy_id.in_(filters.pharmacy_ids))
        if filters.employee_ids:
            stmt = stmt.where(Visit.employee_id.in_(filters.employee_ids))
        if filters.medical_facility_ids:
            stmt = stmt.where(
                Visit.medical_facility_id.in_(filters.medical_facility_ids)
            )
        if filters.product_group_ids:
            stmt = stmt.where(Visit.product_group_id.in_(filters.product_group_ids))
        if filters.geo_indicator_ids:
            stmt = stmt.where(GeoIndicator.id.in_(filters.geo_indicator_ids))
        if filters.speciality_ids:
            stmt = stmt.where(Speciality.id.in_(filters.speciality_ids))

        if filters.search:
            search_term = f"%{filters.search}%"
            search_conditions = []

            if "product_group" in filters.group_by_dimensions:
                search_conditions.append(ProductGroup.name.ilike(search_term))
            if "pharmacy" in filters.group_by_dimensions:
                search_conditions.append(Pharmacy.name.ilike(search_term))
            if "medical_facility" in filters.group_by_dimensions:
                search_conditions.append(MedicalFacility.name.ilike(search_term))
            if "employee" in filters.group_by_dimensions:
                search_conditions.append(Employee.full_name.ilike(search_term))
            if "geo_indicator" in filters.group_by_dimensions:
                search_conditions.append(GeoIndicator.name.ilike(search_term))
            if "speciality" in filters.group_by_dimensions:
                search_conditions.append(Speciality.name.ilike(search_term))

            if search_conditions:
                stmt = stmt.where(or_(*search_conditions))

        stmt = stmt.group_by(*group_by_fields)

        sort_map = {
            "medical_facility": (
                MedicalFacility.name
                if "medical_facility" in filters.group_by_dimensions
                else None
            ),
            "pharmacy": (
                Pharmacy.name if "pharmacy" in filters.group_by_dimensions else None
            ),
            "employee": (
                Employee.full_name
                if "employee" in filters.group_by_dimensions
                else None
            ),
            "group": (
                ProductGroup.name
                if "product_group" in filters.group_by_dimensions
                else None
            ),
            "employee_visits": func.count(Visit.id),
            "geo_indicator": (
                GeoIndicator.name
                if "geo_indicator" in filters.group_by_dimensions
                else None
            ),
        }

        if filters.sort_by and filters.sort_order:
            sort_column = sort_map.get(filters.sort_by)
            if sort_column is not None:
                stmt = stmt.order_by(
                    asc(sort_column)
                    if filters.sort_order == "ASC"
                    else desc(sort_column)
                )
        else:
            order_fields = []
            if "year" in filters.group_by_dimensions:
                order_fields.append(Visit.year.desc())
            if "month" in filters.group_by_dimensions:
                order_fields.append(Visit.month.desc())
            if "employee" in filters.group_by_dimensions:
                order_fields.append(Employee.full_name.nulls_last())
            if "pharmacy" in filters.group_by_dimensions:
                order_fields.append(Pharmacy.name.nulls_last())
            if "medical_facility" in filters.group_by_dimensions:
                order_fields.append(MedicalFacility.name.nulls_last())

            if order_fields:
                stmt = stmt.order_by(*order_fields)

        if filters.limit:
            stmt = stmt.limit(filters.limit)
        if filters.offset:
            stmt = stmt.offset(filters.offset)

        result = await session.execute(stmt)
        return result.mappings().all()

    @staticmethod
    async def get_visits_by_period(
        session: "AsyncSession", filters: visit.VisitCountFilter, company_id: int | None
    ):

        quarter_expr = func.ceil(Visit.month / 3.0)

        if filters.group_by_period == "year":
            period_key = func.cast(Visit.year, String).label("period")
            period_group_fields = [Visit.year]
        elif filters.group_by_period == "quarter":
            period_key = func.concat(
                func.cast(Visit.year, String), "-Q", func.cast(quarter_expr, String)
            ).label("period")
            period_group_fields = [Visit.year, quarter_expr]
        else:
            period_key = func.concat(
                func.cast(Visit.year, String),
                "-",
                func.lpad(func.cast(Visit.month, String), 2, "0"),
            ).label("period")
            period_group_fields = [Visit.year, Visit.month]

        stmt = (
            select(period_key, func.count(Visit.id).label("total_visits"))
            .select_from(Visit)
            .join(Employee, Visit.employee_id == Employee.id)
            .where(
                Visit.year.in_(filters.years),
            )
        )

        if company_id:
            stmt = stmt.where(Employee.company_id == company_id)
        if filters.months:
            stmt = stmt.where(Visit.month.in_(filters.months))
        if filters.quarters:
            stmt = stmt.where(quarter_expr.in_(filters.quarters))
        if filters.pharmacy_ids:
            stmt = stmt.where(Visit.pharmacy_id.in_(filters.pharmacy_ids))
        if filters.employee_ids:
            stmt = stmt.where(Visit.employee_id.in_(filters.employee_ids))
        if filters.medical_facility_ids:
            stmt = stmt.where(
                Visit.medical_facility_id.in_(filters.medical_facility_ids)
            )
        if filters.product_group_ids:
            stmt = stmt.where(Visit.product_group_id.in_(filters.product_group_ids))

        stmt = stmt.group_by(*period_group_fields)

        if filters.group_by_period == "year":
            stmt = stmt.order_by(Visit.year.desc())
        elif filters.group_by_period == "quarter":
            stmt = stmt.order_by(Visit.year.desc(), quarter_expr.desc())
        else:
            stmt = stmt.order_by(Visit.year.desc(), Visit.month.desc())

        if filters.limit:
            stmt = stmt.limit(filters.limit)
        if filters.offset:
            stmt = stmt.offset(filters.offset)

        result = await session.execute(stmt)

        return result.mappings().all()


visit_service = VisitService(Visit)
