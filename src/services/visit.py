import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, AsyncIterator
from uuid import uuid4

from fastapi import HTTPException, UploadFile
from sqlalchemy import and_, func, insert, literal, or_, select, tuple_

from src.db.models import (
    Doctor,
    Employee,
    GlobalDoctor,
    ImportLogs,
    MedicalFacility,
    Pharmacy,
    Position,
    ProductGroup,
    Speciality,
    Visit,
)
from src.import_fields import visit
from src.mapping.dimension_mapping.visits import (
    VISITS_DOCTOR_COUNT_DIMENSTIONS_MAPPING,
    VISITS_SUM_FOR_PERIOD_DIMENSTIONS_MAPPING,
)
from src.mapping.visits import visit_mapping
from src.schemas import visit as visit_schema
from src.schemas.visit import VisitsRequest
from src.services.base import ModelType
from src.utils.build_dimensions import build_dimensions
from src.utils.build_period_key import build_period_key
from src.utils.build_period_values import build_period_values
from src.utils.case_insensitive_dict import CaseInsensitiveDict
from src.utils.case_insensitive_set import CaseInsensitiveSet
from src.utils.excel_parser import iter_excel_records
from src.utils.import_result import build_import_result
from src.utils.list_query_helper import (
    InOrNullSpec,
    ListQueryHelper,
    NumberTypedSpec,
    SearchSpec,
)
from src.utils.mapping import map_record
from src.utils.records_resolver import normalize_record
from src.utils.validate_required_columns import validate_required_columns

from .base import BaseService

if TYPE_CHECKING:
    from fastapi import UploadFile
    from sqlalchemy.ext.asyncio import AsyncSession

from src.schemas.base_filter import PaginatedResponse


class VisitService(
    BaseService[Visit, visit_schema.VisitCreate, visit_schema.VisitUpdate]
):
    async def import_sales(
        self,
        session: "AsyncSession",
        file: "UploadFile",
        user_id: int,
        batch_size: int = 10_000,
    ):
        from src.db.models.excel_tasks import ExcelTaskType
        from src.tasks.sale_imports import import_sales_task
        from src.tasks.utils import create_excel_task_record

        upload_dir = Path("temp")
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

            await create_excel_task_record(
                task_id=task.id,
                started_by=user_id,
                file_path="",
                task_type=ExcelTaskType.IMPORT,
            )
            return {"task_id": task.id}
        except Exception:
            if file_path.exists():
                os.remove(file_path)
            raise

    async def get_multi(
        self,
        session: "AsyncSession",
        filters: VisitsRequest | None = None,
        load_options: list[Any] | None = None,
    ) -> PaginatedResponse[visit_schema.VisitResponse]:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        sort_map = {
            "pharmacy": self.model.pharmacy_id,
            "employee": self.model.employee_id,
            "product_group": self.model.product_group_id,
            "medical_facility": self.model.medical_facility_id,
            "doctor": self.model.doctor_id,
            "client_type": self.model.client_type,
            "month": self.model.month,
            "year": self.model.year,
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
                    InOrNullSpec(self.model.pharmacy_id, filters.pharmacy_ids),
                    InOrNullSpec(self.model.employee_id, filters.employee_ids),
                    InOrNullSpec(
                        self.model.product_group_id, filters.product_group_ids
                    ),
                    InOrNullSpec(
                        self.model.medical_facility_id, filters.medical_facility_ids
                    ),
                    InOrNullSpec(self.model.doctor_id, filters.doctor_ids),
                    InOrNullSpec(self.model.client_type, filters.client_type),
                    InOrNullSpec(self.model.month, filters.months),
                    NumberTypedSpec(self.model.year, filters.year),
                ],
            )
            if filters.doctor_or_pharmacy_ids:
                stmt = stmt.where(
                    or_(
                        self.model.doctor_id.in_(filters.doctor_or_pharmacy_ids),
                        self.model.pharmacy_id.in_(filters.doctor_or_pharmacy_ids),
                    )
                )

        # Count before pagination
        count_stmt = select(func.count()).select_from(stmt.subquery())
        total_count = await session.scalar(count_stmt)

        if filters:
            stmt = ListQueryHelper.apply_pagination(stmt, filters.limit, filters.offset)

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
    ) -> AsyncIterator[ModelType]:
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

    @staticmethod
    async def _get_employee_id_company_map(
        session: "AsyncSession",
        names: set[str],
    ) -> tuple[CaseInsensitiveDict, CaseInsensitiveDict, CaseInsensitiveSet]:
        """Возвращает (name→id, name→company_id, missing) для Employee."""
        stmt = select(Employee.id, Employee.full_name, Employee.company_id).where(
            Employee.full_name.in_(names)
        )
        result = await session.execute(stmt)
        rows = result.all()
        id_map = CaseInsensitiveDict(
            {full_name: emp_id for emp_id, full_name, _ in rows}
        )
        company_map = CaseInsensitiveDict(
            {full_name: company_id for _, full_name, company_id in rows}
        )
        missing = CaseInsensitiveSet(v for v in names if v not in id_map)
        return id_map, company_map, missing

    @staticmethod
    async def _get_doctor_id_map(
        session: "AsyncSession",
        names: set[str],
    ) -> tuple[CaseInsensitiveDict, CaseInsensitiveSet]:
        """Ищет Doctor.id по GlobalDoctor.full_name через JOIN, т.к. full_name
        является @property на Doctor, а не колонкой SQLAlchemy."""
        stmt = (
            select(Doctor.id, GlobalDoctor.full_name)
            .join(GlobalDoctor, Doctor.global_doctor_id == GlobalDoctor.id)
            .where(GlobalDoctor.full_name.in_(names))
        )
        result = await session.execute(stmt)
        rows = result.all()
        obj_map = CaseInsensitiveDict(
            {full_name: doctor_id for doctor_id, full_name in rows}
        )
        missing = CaseInsensitiveSet(v for v in names if v not in obj_map)
        return obj_map, missing

    async def _import_excel_from_file(
        self,
        session: "AsyncSession",
        file_path: str,
        user_id: int,
        batch_size: int = 2000,
    ):
        try:
            with open(file_path, "rb") as f:
                first_row = next(iter_excel_records(f), None)

            if first_row is None:
                raise HTTPException(status_code=400, detail="Файл пустой")

            _, first_record = first_row
            try:
                validate_required_columns(
                    [first_record], visit.visit_fields, raise_exception=False
                )
            except Exception as e:
                raise ValueError(str(e))
            total_records = 0

            import_log = ImportLogs(
                uploaded_by=user_id,
                target_table="Визиты",
                records_count=0,
                target_table_name=self.model.__tablename__,
            )
            session.add(import_log)
            await session.flush()

            pg_cache: CaseInsensitiveDict = CaseInsensitiveDict()
            emp_cache: CaseInsensitiveDict = CaseInsensitiveDict()
            emp_company_cache: CaseInsensitiveDict = CaseInsensitiveDict()
            doc_cache: CaseInsensitiveDict = CaseInsensitiveDict()
            mf_cache: CaseInsensitiveDict = CaseInsensitiveDict()
            ph_cache: CaseInsensitiveDict = CaseInsensitiveDict()

            missing_pgs: CaseInsensitiveSet = CaseInsensitiveSet()
            missing_emps: CaseInsensitiveSet = CaseInsensitiveSet()
            missing_docs: CaseInsensitiveSet = CaseInsensitiveSet()
            missing_mfs: CaseInsensitiveSet = CaseInsensitiveSet()
            missing_phs: CaseInsensitiveSet = CaseInsensitiveSet()

            skipped_records = []
            data_to_insert = []
            pending_records: list[tuple[int, dict[str, Any]]] = []
            pending_pgs: set[str] = set()
            pending_emps: set[str] = set()
            pending_docs: set[str] = set()
            pending_mfs: set[str] = set()
            pending_phs: set[str] = set()
            imported_count = 0

            async def resolve_pending_names():
                if pending_pgs:
                    m, miss = await self.get_id_map(
                        session, ProductGroup, "name", pending_pgs
                    )
                    pg_cache.update(m)
                    missing_pgs.update(miss)
                    pending_pgs.clear()

                if pending_emps:
                    m, cm, miss = await self._get_employee_id_company_map(
                        session, pending_emps
                    )
                    emp_cache.update(m)
                    emp_company_cache.update(cm)
                    missing_emps.update(miss)
                    pending_emps.clear()

                if pending_docs:
                    m, miss = await self._get_doctor_id_map(session, pending_docs)
                    doc_cache.update(m)
                    missing_docs.update(miss)
                    pending_docs.clear()

                if pending_mfs:
                    m, miss = await self.get_id_map(
                        session, MedicalFacility, "name", pending_mfs
                    )
                    mf_cache.update(m)
                    missing_mfs.update(miss)
                    pending_mfs.clear()

                if pending_phs:
                    m, miss = await self.get_id_map(
                        session, Pharmacy, "name", pending_phs
                    )
                    ph_cache.update(m)
                    missing_phs.update(miss)
                    pending_phs.clear()

            async def process_pending_records():
                nonlocal imported_count, data_to_insert

                if not pending_records:
                    return

                await resolve_pending_names()

                for row_index, r in pending_records:
                    missing_keys = []

                    if r.get("группа") in missing_pgs:
                        missing_keys.append(f"группа: {r['группа']}")
                    if r.get("сотрудник") in missing_emps:
                        missing_keys.append(f"сотрудник: {r['сотрудник']}")
                    if r.get("врач") and r.get("врач") in missing_docs:
                        missing_keys.append(f"врач: {r['врач']}")

                    m_facility_id = None
                    p_id = None

                    if r.get("тип клиента") == "Врач":
                        if r.get("учреждение") in missing_mfs:
                            missing_keys.append(f"учреждение (ЛПУ): {r['учреждение']}")
                        else:
                            m_facility_id = mf_cache.get(r.get("учреждение"))
                    elif r.get("тип клиента") == "Аптека":
                        if r.get("учреждение") in missing_phs:
                            missing_keys.append(
                                f"учреждение (Аптека): {r['учреждение']}"
                            )
                        else:
                            p_id = ph_cache.get(r.get("учреждение"))

                    if missing_keys:
                        skipped_records.append(
                            {"row": row_index, "missing": missing_keys}
                        )
                        continue

                    relation_fields = {
                        "product_group_id": pg_cache.get(r.get("группа")),
                        "doctor_id": doc_cache.get(r.get("врач")),
                        "employee_id": emp_cache.get(r.get("сотрудник")),
                        "company_id": emp_company_cache.get(r.get("сотрудник")),
                        "medical_facility_id": m_facility_id,
                        "pharmacy_id": p_id,
                        "import_log_id": import_log.id,
                    }
                    data_to_insert.append(map_record(r, visit_mapping, relation_fields))

                    if len(data_to_insert) >= batch_size:
                        await session.execute(insert(self.model), data_to_insert)
                        imported_count += len(data_to_insert)
                        data_to_insert = []

                pending_records.clear()

            with open(file_path, "rb") as f:
                for row_index, r in iter_excel_records(f):
                    total_records += 1
                    normalize_record(r, visit.visit_fields)

                    pending_records.append((row_index, r))

                    if (
                        r.get("группа")
                        and r["группа"] not in pg_cache
                        and r["группа"] not in missing_pgs
                    ):
                        pending_pgs.add(r["группа"])
                    if (
                        r.get("сотрудник")
                        and r["сотрудник"] not in emp_cache
                        and r["сотрудник"] not in missing_emps
                    ):
                        pending_emps.add(r["сотрудник"])
                    if (
                        r.get("врач")
                        and r["врач"] not in doc_cache
                        and r["врач"] not in missing_docs
                    ):
                        pending_docs.add(r["врач"])
                    if r.get("тип клиента") == "Врач" and r.get("учреждение"):
                        if (
                            r["учреждение"] not in mf_cache
                            and r["учреждение"] not in missing_mfs
                        ):
                            pending_mfs.add(r["учреждение"])
                    elif r.get("тип клиента") == "Аптека" and r.get("учреждение"):
                        if (
                            r["учреждение"] not in ph_cache
                            and r["учреждение"] not in missing_phs
                        ):
                            pending_phs.add(r["учреждение"])

                    if len(pending_records) >= batch_size:
                        await process_pending_records()

            await process_pending_records()

            if data_to_insert:
                await session.execute(insert(self.model), data_to_insert)
                imported_count += len(data_to_insert)

            import_log.records_count = total_records
            await session.commit()

            return build_import_result(
                total=total_records,
                imported=imported_count,
                skipped_records=skipped_records,
                inserted=imported_count,
                deduplicated=0,
            )
        finally:
            pass

    @staticmethod
    async def get_doctor_count_by_speciality(
        session: "AsyncSession",
        filters: visit_schema.DoctorsCountFilter,
        company_id: int | None,
    ) -> list[visit_schema.DoctorsBySpecialtyResponse]:
        quarter_expr = func.ceil(Visit.month / 3.0)
        period_values = build_period_values(
            filters.group_by_period, filters.period_values
        )

        base_visits = (
            select(Visit.doctor_id)
            .select_from(Visit)
            .where(Visit.doctor_id.is_not(None))
        )

        base_visits = ListQueryHelper.apply_specs(
            base_visits,
            [
                InOrNullSpec(Visit.company_id, [company_id] if company_id else None),
                InOrNullSpec(Visit.medical_facility_id, filters.medical_facility_ids),
                InOrNullSpec(Visit.product_group_id, filters.product_group_ids),
            ],
        )

        base_visits = ListQueryHelper.apply_period_values(
            base_visits,
            period_values,
            year_col=Visit.year,
            month_col=Visit.month,
            quarter_col=quarter_expr,
        )

        base_visits_subq = base_visits.subquery()

        distinct_doctor_key = tuple_(
            GlobalDoctor.full_name,
            GlobalDoctor.medical_facility_id,
        )

        # Всего уникальных врачей по специальности (из global_doctors)
        total_doctors_subquery = select(
            GlobalDoctor.speciality_id.label("speciality_id"),
            func.count(func.distinct(distinct_doctor_key)).label("total_count"),
        ).select_from(GlobalDoctor)

        total_doctors_subquery = ListQueryHelper.apply_specs(
            total_doctors_subquery,
            [
                InOrNullSpec(GlobalDoctor.speciality_id, filters.speciality_ids),
                InOrNullSpec(
                    GlobalDoctor.medical_facility_id, filters.medical_facility_ids
                ),
            ],
        )

        total_doctors_subquery = total_doctors_subquery.group_by(
            GlobalDoctor.speciality_id
        ).subquery()

        # Врачи с визитами ФК (уник по ФИО+ЛПУ, через doctors → global_doctors)
        doctors_with_visits_subquery = (
            select(
                GlobalDoctor.speciality_id,
                func.count(func.distinct(distinct_doctor_key)).label(
                    "doctors_with_visits"
                ),
            )
            .select_from(Doctor)
            .join(GlobalDoctor, Doctor.global_doctor_id == GlobalDoctor.id)
            .join(base_visits_subq, Doctor.id == base_visits_subq.c.doctor_id)
        )

        doctors_with_visits_subquery = ListQueryHelper.apply_specs(
            doctors_with_visits_subquery,
            [
                InOrNullSpec(GlobalDoctor.speciality_id, filters.speciality_ids),
            ],
        )

        doctors_with_visits_subquery = doctors_with_visits_subquery.group_by(
            GlobalDoctor.speciality_id
        ).subquery()

        stmt = (
            select(
                Speciality.id.label("speciality_id"),
                Speciality.name.label("speciality_name"),
                func.coalesce(total_doctors_subquery.c.total_count, 0).label(
                    "total_count"
                ),
                func.coalesce(
                    doctors_with_visits_subquery.c.doctors_with_visits, 0
                ).label("count_with_visits"),
                func.coalesce(
                    (
                        func.coalesce(
                            doctors_with_visits_subquery.c.doctors_with_visits, 0
                        )
                        * 100.0
                        / func.nullif(total_doctors_subquery.c.total_count, 0)
                    ),
                    0.0,
                ).label("coverage_percentage"),
            )
            .select_from(Speciality)
            .outerjoin(
                total_doctors_subquery,
                Speciality.id == total_doctors_subquery.c.speciality_id,
            )
            .outerjoin(
                doctors_with_visits_subquery,
                Speciality.id == doctors_with_visits_subquery.c.speciality_id,
            )
        )

        stmt = ListQueryHelper.apply_specs(
            stmt,
            [
                InOrNullSpec(Speciality.id, filters.speciality_ids),
            ],
        )

        if filters.search:
            search_term = f"%{filters.search}%"
            stmt = stmt.where(Speciality.name.ilike(search_term))

        stmt = stmt.group_by(
            Speciality.id,
            Speciality.name,
            total_doctors_subquery.c.total_count,
            doctors_with_visits_subquery.c.doctors_with_visits,
        )

        result = await session.execute(stmt)
        return [
            visit_schema.DoctorsBySpecialtyResponse(**row) for row in result.mappings()
        ]

    @staticmethod
    async def get_doctor_count_with_visits_by_speciality(
        session: "AsyncSession",
        filters: visit_schema.DoctorsCountWithVisitFilter,
        company_id: int | None,
    ):

        quarter_expr = func.ceil(Visit.month / 3.0)
        period_values = build_period_values(
            filters.group_by_period, filters.period_values
        )

        select_fields, group_by_fields, search_cols = build_dimensions(
            VISITS_DOCTOR_COUNT_DIMENSTIONS_MAPPING, filters.group_by_dimensions
        )

        all_doctors_subquery = (
            select(
                *select_fields,
                func.count(func.distinct(Doctor.id)).label("total_doctors"),
            )
            .select_from(Doctor)
            .join(GlobalDoctor, Doctor.global_doctor_id == GlobalDoctor.id)
            .join(Speciality, Doctor.speciality_id == Speciality.id)
            .join(
                MedicalFacility, GlobalDoctor.medical_facility_id == MedicalFacility.id
            )
        )

        all_doctors_subquery = ListQueryHelper.apply_specs(
            all_doctors_subquery,
            [
                InOrNullSpec(Speciality.id, filters.speciality_ids),
                InOrNullSpec(MedicalFacility.id, filters.medical_facility_ids),
                InOrNullSpec(Doctor.id, filters.doctor_ids),
                SearchSpec(
                    filters.search if filters.group_by_dimensions else None, search_cols
                ),
            ],
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
            .join(GlobalDoctor, Doctor.global_doctor_id == GlobalDoctor.id)
            .join(Speciality, Doctor.speciality_id == Speciality.id)
            .join(
                MedicalFacility, GlobalDoctor.medical_facility_id == MedicalFacility.id
            )
            .where(
                Visit.doctor_id.is_not(None),
            )
        )

        doctors_with_visits_subquery = ListQueryHelper.apply_specs(
            doctors_with_visits_subquery,
            [
                InOrNullSpec(Employee.company_id, [company_id] if company_id else None),
                InOrNullSpec(Speciality.id, filters.speciality_ids),
                InOrNullSpec(MedicalFacility.id, filters.medical_facility_ids),
                InOrNullSpec(Doctor.id, filters.doctor_ids),
            ],
        )

        doctors_with_visits_subquery = ListQueryHelper.apply_period_values(
            doctors_with_visits_subquery,
            period_values,
            year_col=Visit.year,
            month_col=Visit.month,
            quarter_col=quarter_expr,
        )

        if filters.search:
            search_term = f"%{filters.search}%"
            search_conditions = []
            if "medical_facility" in filters.group_by_dimensions:
                search_conditions.append(MedicalFacility.name.ilike(search_term))
            if "speciality" in filters.group_by_dimensions:
                search_conditions.append(Speciality.name.ilike(search_term))
            if "doctor" in filters.group_by_dimensions:
                search_conditions.append(GlobalDoctor.full_name.ilike(search_term))

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
                and_(*join_conditions) if join_conditions else literal(True),
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
        default_sort = [
            getattr(all_doctors_subquery.c, f"{dim}_name")
            for dim in (filters.group_by_dimensions or [])
            if getattr(all_doctors_subquery.c, f"{dim}_name", None) is not None
        ]

        stmt = ListQueryHelper.apply_sorting_with_default(
            stmt,
            filters.sort_by,
            filters.sort_order,
            sort_map,
            default_sort=default_sort if default_sort else None,
        )

        stmt = ListQueryHelper.apply_pagination(stmt, filters.limit, filters.offset)

        result = await session.execute(stmt)
        return result.mappings().all()

    @staticmethod
    async def get_visits_sum_for_period(
        session: "AsyncSession",
        filters: visit_schema.VisitSumForPeriodFilter,
        company_id: int | None,
    ):
        period_values = build_period_values(
            filters.group_by_period, filters.period_values
        )
        quarter_expr = func.ceil(Visit.month / 3.0)
        group_by_dimensions = list(filters.group_by_dimensions or [])
        if "employee" in group_by_dimensions and "position" not in group_by_dimensions:
            group_by_dimensions.append("position")
        select_fields = []
        group_by_fields = []

        for dim in group_by_dimensions:
            dim_config = VISITS_SUM_FOR_PERIOD_DIMENSTIONS_MAPPING[dim]

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

        if "year" not in group_by_dimensions:
            select_fields.append(Visit.year.label("year"))
            group_by_fields.append(Visit.year)

        if "month" not in group_by_dimensions:
            select_fields.append(Visit.month.label("month"))
            group_by_fields.append(Visit.month)

        select_fields.append(func.count(Visit.id).label("employee_visits"))

        stmt = select(*select_fields).select_from(Visit)

        if company_id:
            stmt = stmt.where(
                Visit.employee_id.in_(
                    select(Employee.id).where(Employee.company_id == company_id)
                )
            )

        tables_to_join = set()
        for dim in group_by_dimensions:
            dim_config = VISITS_SUM_FOR_PERIOD_DIMENSTIONS_MAPPING[dim]
            if dim_config["join_table"] is not None:
                tables_to_join.add(dim)
                if "requires" in dim_config:
                    for req in dim_config["requires"]:
                        tables_to_join.add(req)
        if filters.speciality_ids:
            tables_to_join.add("speciality")

        join_order = [
            "employee",
            "position",
            "product_group",
            "pharmacy",
            "medical_facility",
            "doctor",
            "global_doctor",
            "speciality",
        ]

        for table_name in join_order:
            if table_name in tables_to_join:
                dim_config = VISITS_SUM_FOR_PERIOD_DIMENSTIONS_MAPPING[table_name]
                join_table = dim_config["join_table"]
                join_condition = dim_config["join_condition"]()
                join_type = dim_config["join_type"]

                if join_type == "join":
                    stmt = stmt.join(join_table, join_condition)
                else:
                    stmt = stmt.outerjoin(join_table, join_condition)

        stmt = ListQueryHelper.apply_specs(
            stmt,
            [
                InOrNullSpec(Visit.pharmacy_id, filters.pharmacy_ids),
                InOrNullSpec(Visit.employee_id, filters.employee_ids),
                InOrNullSpec(Position.id, filters.position_ids),
                InOrNullSpec(Visit.medical_facility_id, filters.medical_facility_ids),
                InOrNullSpec(Visit.product_group_id, filters.product_group_ids),
                InOrNullSpec(Visit.doctor_id, filters.doctor_ids),
                InOrNullSpec(Speciality.id, filters.speciality_ids),
            ],
        )

        stmt = ListQueryHelper.apply_period_values(
            stmt,
            period_values,
            year_col=Visit.year,
            month_col=Visit.month,
            quarter_col=quarter_expr,
        )

        if filters.search:
            search_term = f"%{filters.search}%"
            search_conditions = []

            if "product_group" in group_by_dimensions:
                search_conditions.append(ProductGroup.name.ilike(search_term))
            if "pharmacy" in group_by_dimensions:
                search_conditions.append(Pharmacy.name.ilike(search_term))
            if "medical_facility" in group_by_dimensions:
                search_conditions.append(MedicalFacility.name.ilike(search_term))
            if "employee" in group_by_dimensions:
                search_conditions.append(Employee.full_name.ilike(search_term))
            if "position" in group_by_dimensions:
                search_conditions.append(Position.name.ilike(search_term))
            if "speciality" in group_by_dimensions:
                search_conditions.append(Speciality.name.ilike(search_term))

            if search_conditions:
                stmt = stmt.where(or_(*search_conditions))

        stmt = stmt.group_by(*group_by_fields)

        sort_map = {
            "medical_facility": MedicalFacility.name,
            "pharmacy": Pharmacy.name,
            "employee": Employee.full_name,
            "group": ProductGroup.name,
            "employee_visits": func.count(Visit.id),
            "position": Position.name,
        }

        allowed_dim_sorts = {
            "medical_facility",
            "pharmacy",
            "employee",
            "group",
            "position",
        }
        if filters.sort_by in allowed_dim_sorts and filters.sort_by not in (
            group_by_dimensions or []
        ):
            sort_by = None
            sort_order = None
        else:
            sort_by = filters.sort_by
            sort_order = filters.sort_order

        default_sort = []
        g = set(group_by_dimensions or [])

        if "year" in g:
            default_sort.append(Visit.year.desc())
        if "month" in g:
            default_sort.append(Visit.month.desc())
        if "employee" in g:
            default_sort.append(Employee.full_name.nulls_last())
        if "position" in g:
            default_sort.append(Position.name.nulls_last())
        if "pharmacy" in g:
            default_sort.append(Pharmacy.name.nulls_last())
        if "medical_facility" in g:
            default_sort.append(MedicalFacility.name.nulls_last())

        stmt = ListQueryHelper.apply_sorting_with_default(
            stmt,
            sort_by,
            sort_order,
            sort_map,
            default_sort=default_sort if default_sort else None,
        )

        stmt = ListQueryHelper.apply_pagination(stmt, filters.limit, filters.offset)

        result = await session.execute(stmt)
        return result.mappings().all()

    @staticmethod
    async def get_visits_by_period(
        session: "AsyncSession",
        filters: visit_schema.VisitCountFilter | None = None,
        company_id: int | None = None,
    ):
        period_values = build_period_values(
            filters.group_by_period, filters.period_values
        )
        if period_values is None:
            return []

        quarter_expr = func.ceil(Visit.month / 3.0)

        period_expr, period_group_fields = build_period_key(
            filters.group_by_period,
            Visit,
            with_group_fields=True,
            quarter_expr=quarter_expr,
        )

        period_key = period_expr.label("period")

        stmt = (
            select(period_key, func.count(Visit.id).label("total_visits"))
            .select_from(Visit)
            .join(Employee, Visit.employee_id == Employee.id)
        )

        if company_id:
            stmt = stmt.where(Employee.company_id == company_id)
        stmt = ListQueryHelper.apply_specs(
            stmt,
            [
                InOrNullSpec(
                    Visit.month,
                    (
                        [month for _, month in (period_values.months or [])]
                        if period_values.months
                        else None
                    ),
                ),
                InOrNullSpec(
                    Visit.year,
                    (
                        [year for year, _ in (period_values.months or [])]
                        if period_values.months
                        else period_values.years
                    ),
                ),
                InOrNullSpec(
                    quarter_expr,
                    (
                        [quarter for _, quarter in (period_values.quarters or [])]
                        if period_values.quarters
                        else None
                    ),
                ),
                InOrNullSpec(Visit.pharmacy_id, filters.pharmacy_ids),
                InOrNullSpec(Visit.employee_id, filters.employee_ids),
                InOrNullSpec(Visit.medical_facility_id, filters.medical_facility_ids),
                InOrNullSpec(Visit.product_group_id, filters.product_group_ids),
            ],
        )

        stmt = stmt.group_by(*period_group_fields)

        if filters.group_by_period == "year":
            stmt = stmt.order_by(Visit.year.desc())
        elif filters.group_by_period == "quarter":
            stmt = stmt.order_by(Visit.year.desc(), quarter_expr.desc())
        else:
            stmt = stmt.order_by(Visit.year.desc(), Visit.month.desc())

        stmt = ListQueryHelper.apply_pagination(stmt, filters.limit, filters.offset)

        result = await session.execute(stmt)

        return result.mappings().all()


visit_service = VisitService(Visit)
