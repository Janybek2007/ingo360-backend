from typing import TYPE_CHECKING, Any, AsyncIterator

from fastapi import UploadFile
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import selectinload

from src.db.models import (
    Doctor,
    GlobalDoctor,
    ImportLogs,
    clients,
)
from src.import_fields import client
from src.schemas.client import (
    DoctorCreate,
    DoctorListRequest,
    DoctorUpdate,
)
from src.services.base import BaseService
from src.utils.excel_parser import parse_excel_file
from src.utils.import_result import build_import_result
from src.utils.list_query_helper import InOrNullSpec, ListQueryHelper, StringTypedSpec
from src.utils.records_resolver import resolve_records_fields
from src.utils.validate_required_columns import validate_required_columns

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

from src.schemas.base_filter import PaginatedResponse


class DoctorService(BaseService[clients.Doctor, DoctorCreate, DoctorUpdate]):
    async def create(
        self,
        session: "AsyncSession",
        obj_in: DoctorCreate,
        load_options: list[Any] | None = None,
    ):
        global_fields = {"full_name", "medical_facility_id", "speciality_id"}
        obj_data = obj_in.model_dump(exclude={"mode"})

        # =========================
        # GLOBAL MODE
        # =========================
        if obj_in.mode == "global":
            stmt = select(GlobalDoctor).where(
                GlobalDoctor.full_name == obj_data["full_name"],
                GlobalDoctor.medical_facility_id == obj_data["medical_facility_id"],
                GlobalDoctor.speciality_id == obj_data["speciality_id"],
            )

            result = await session.execute(stmt)
            global_doctor = result.scalar_one_or_none()

            if not global_doctor:
                global_doctor = GlobalDoctor(
                    full_name=obj_data["full_name"],
                    medical_facility_id=obj_data["medical_facility_id"],
                    speciality_id=obj_data["speciality_id"],
                )
                session.add(global_doctor)
                await session.flush()

            try:
                await session.commit()
                await session.refresh(global_doctor)

                if load_options:
                    stmt = (
                        select(GlobalDoctor)
                        .options(*load_options)
                        .where(GlobalDoctor.id == global_doctor.id)
                    )
                    result = await session.execute(stmt)
                    global_doctor = result.unique().scalar_one()

                return {
                    "id": global_doctor.id,
                    "mode": "global",
                    "full_name": global_doctor.full_name,
                    "global_doctor": global_doctor,
                    "responsible_employee": None,
                    "medical_facility": global_doctor.medical_facility,
                    "speciality": global_doctor.speciality,
                    "client_category": None,
                    "product_group": None,
                    "company": None,
                }

            except IntegrityError as e:
                await session.rollback()
                await self._rollback_and_raise_integrity(e)
            except Exception:
                await session.rollback()
                raise

        # =========================
        # COMPANY MODE
        # =========================
        # 1. get or create GlobalDoctor
        global_stmt = select(GlobalDoctor).where(
            GlobalDoctor.full_name == obj_data["full_name"],
            GlobalDoctor.medical_facility_id == obj_data["medical_facility_id"],
            GlobalDoctor.speciality_id == obj_data["speciality_id"],
        )

        result = await session.execute(global_stmt)
        global_doctor = result.scalar_one_or_none()

        if not global_doctor:
            global_doctor = GlobalDoctor(
                full_name=obj_data["full_name"],
                medical_facility_id=obj_data["medical_facility_id"],
                speciality_id=obj_data["speciality_id"],
            )
            session.add(global_doctor)
            await session.flush()

        # 2. create Doctor (без глобальных полей)
        doctor_data = {k: v for k, v in obj_data.items() if k not in global_fields}
        db_obj = Doctor(
            **doctor_data,
            global_doctor_id=global_doctor.id,
        )

        session.add(db_obj)

        try:
            await session.commit()
            await session.refresh(db_obj)

            if load_options:
                stmt = (
                    select(Doctor).options(*load_options).where(Doctor.id == db_obj.id)
                )
                result = await session.execute(stmt)
                db_obj = result.unique().scalar_one()

            return db_obj

        except IntegrityError as e:
            await session.rollback()
            await self._rollback_and_raise_integrity(e)
        except Exception:
            await session.rollback()
            raise

    async def update(
        self,
        session: "AsyncSession",
        item_id: int,
        obj_in: DoctorUpdate,
        load_options: list[Any] | None = None,
    ):
        db_obj = await self.get_or_404(session, item_id)
        update_data = obj_in.model_dump(exclude_unset=True, exclude={"mode"})

        global_fields = {"full_name", "medical_facility_id", "speciality_id"}
        doctor_fields = {k: v for k, v in update_data.items() if k not in global_fields}
        global_data = {k: v for k, v in update_data.items() if k in global_fields}

        # Обновляем поля Doctor
        for field, value in doctor_fields.items():
            setattr(db_obj, field, value)

        # Обновляем GlobalDoctor
        if global_data:
            current_global = db_obj.global_doctor
            new_global_values = {
                **{
                    "full_name": current_global.full_name,
                    "medical_facility_id": current_global.medical_facility_id,
                    "speciality_id": current_global.speciality_id,
                },
                **global_data,
            }

            # Проверяем, существует ли уже такой GlobalDoctor
            existing_stmt = select(GlobalDoctor).where(
                GlobalDoctor.full_name == new_global_values["full_name"],
                GlobalDoctor.medical_facility_id
                == new_global_values["medical_facility_id"],
                GlobalDoctor.speciality_id == new_global_values["speciality_id"],
            )
            result = await session.execute(existing_stmt)
            existing_global = result.scalar_one_or_none()

            if existing_global and existing_global.id != current_global.id:
                # Перепривязываем Doctor к существующему GlobalDoctor
                db_obj.global_doctor_id = existing_global.id
            elif not existing_global:
                # Обновляем текущий GlobalDoctor
                for field, value in global_data.items():
                    setattr(current_global, field, value)

        try:
            await session.commit()
            await session.refresh(db_obj)

            if load_options:
                db_obj = await self.get(session, item_id, load_options)

            return db_obj
        except IntegrityError as e:
            await session.rollback()
            await self._rollback_and_raise_integrity(e)
        except Exception:
            await session.rollback()
            raise

    async def get_multi(
        self,
        session: "AsyncSession",
        filters: DoctorListRequest | None = None,
        load_options: list[Any] | None = None,
    ):
        # =========================
        # GLOBAL MODE — только GlobalDoctor
        # =========================
        if filters and filters.mode == "global":
            stmt = select(GlobalDoctor)

            if load_options:
                stmt = stmt.options(*load_options)

            stmt = ListQueryHelper.apply_specs(
                stmt,
                [
                    StringTypedSpec(GlobalDoctor.full_name, filters.full_name),
                    InOrNullSpec(
                        GlobalDoctor.medical_facility_id,
                        filters.medical_facility_ids,
                    ),
                    InOrNullSpec(
                        GlobalDoctor.speciality_id,
                        filters.speciality_ids,
                    ),
                ],
            )

            sort_map = {
                "full_name": GlobalDoctor.full_name,
                "medical_facility": GlobalDoctor.medical_facility_id,
                "speciality": GlobalDoctor.speciality_id,
            }

            stmt = ListQueryHelper.apply_sorting_with_default(
                stmt,
                getattr(filters, "sort_by", None),
                getattr(filters, "sort_order", None),
                sort_map,
                GlobalDoctor.full_name.asc(),
            )

            count_stmt = select(func.count()).select_from(stmt.subquery())
            total_count = await session.scalar(count_stmt)

            stmt = ListQueryHelper.apply_pagination(stmt, filters.limit, filters.offset)

            result = await session.execute(stmt)
            items = result.scalars().all()

            response_items = [
                {
                    "id": item.id,
                    "mode": "global",
                    "full_name": item.full_name,
                    "medical_facility": item.medical_facility,
                    "speciality": item.speciality,
                    "responsible_employee": None,
                    "client_category": None,
                    "product_group": None,
                    "company": None,
                }
                for item in items
            ]

            hasPrev = filters.offset > 0
            hasNext = len(items) == filters.limit if filters.limit else False

            return PaginatedResponse(
                result=response_items,
                hasPrev=hasPrev,
                hasNext=hasNext,
                count=total_count or 0,
            )

        # =========================
        # COMPANY / COMBINED MODE
        # =========================
        is_company_only = filters and filters.mode == "company"

        # Doctor-specific filters — GlobalDoctor не имеет этих полей
        has_doctor_only_filters = filters and (
            filters.responsible_employee_ids
            or filters.client_category_ids
            or filters.product_group_ids
        )

        # --- GlobalDoctor (только если combined и нет Doctor-фильтров) ---
        global_rows = []
        if (
            not is_company_only
            and not (filters and filters.company_ids)
            and not has_doctor_only_filters
        ):
            global_stmt = select(GlobalDoctor).options(
                selectinload(GlobalDoctor.medical_facility),
                selectinload(GlobalDoctor.speciality),
            )

            global_stmt = ListQueryHelper.apply_specs(
                global_stmt,
                [
                    (
                        StringTypedSpec(GlobalDoctor.full_name, filters.full_name)
                        if filters and filters.full_name
                        else None
                    ),
                    InOrNullSpec(
                        GlobalDoctor.medical_facility_id,
                        filters.medical_facility_ids if filters else None,
                    ),
                    InOrNullSpec(
                        GlobalDoctor.speciality_id,
                        filters.speciality_ids if filters else None,
                    ),
                ],
            )

            global_result = await session.execute(global_stmt)
            global_items = global_result.scalars().all()

            global_rows = [
                {
                    "id": item.id,
                    "mode": "global",
                    "full_name": item.full_name,
                    "medical_facility": item.medical_facility,
                    "speciality": item.speciality,
                    "responsible_employee": None,
                    "client_category": None,
                    "product_group": None,
                    "company": None,
                    "_sort_full_name": item.full_name,
                    "_sort_medical_facility_id": item.medical_facility_id,
                    "_sort_speciality_id": item.speciality_id,
                    "_sort_company_id": None,
                    "_sort_responsible_employee_id": None,
                    "_sort_client_category_id": None,
                    "_sort_product_group_id": None,
                    "_sort_created_at": item.created_at,
                }
                for item in global_items
            ]

        # --- Doctor ---
        company_stmt = select(Doctor)
        if load_options:
            company_stmt = company_stmt.options(*load_options)

        company_stmt = company_stmt.join(Doctor.global_doctor)

        if filters:
            company_stmt = ListQueryHelper.apply_specs(
                company_stmt,
                [
                    (
                        StringTypedSpec(GlobalDoctor.full_name, filters.full_name)
                        if filters.full_name
                        else None
                    ),
                    InOrNullSpec(
                        Doctor.company_id,
                        filters.company_ids,
                    ),
                    InOrNullSpec(
                        Doctor.responsible_employee_id,
                        filters.responsible_employee_ids,
                    ),
                    InOrNullSpec(
                        Doctor.client_category_id,
                        filters.client_category_ids,
                    ),
                    InOrNullSpec(
                        Doctor.product_group_id,
                        filters.product_group_ids,
                    ),
                    InOrNullSpec(
                        GlobalDoctor.medical_facility_id,
                        filters.medical_facility_ids,
                    ),
                    InOrNullSpec(
                        GlobalDoctor.speciality_id,
                        filters.speciality_ids,
                    ),
                ],
            )

        company_result = await session.execute(company_stmt)
        company_items = company_result.unique().scalars().all()

        company_rows = [
            {
                "id": item.id,
                "mode": "company",
                "full_name": item.global_doctor.full_name,
                "medical_facility": item.global_doctor.medical_facility,
                "speciality": item.global_doctor.speciality,
                "responsible_employee": item.responsible_employee,
                "client_category": item.client_category,
                "product_group": item.product_group,
                "company": item.company,
                "_sort_full_name": item.global_doctor.full_name,
                "_sort_medical_facility_id": item.global_doctor.medical_facility_id,
                "_sort_speciality_id": item.global_doctor.speciality_id,
                "_sort_company_id": item.company_id,
                "_sort_responsible_employee_id": item.responsible_employee_id,
                "_sort_client_category_id": item.client_category_id,
                "_sort_product_group_id": item.product_group_id,
                "_sort_created_at": item.created_at,
            }
            for item in company_items
        ]

        # --- Объединяем или берём только company ---
        all_rows = global_rows + company_rows if not is_company_only else company_rows

        # --- Сортировка ---
        sort_map = {
            "full_name": "_sort_full_name",
            "medical_facility": "_sort_medical_facility_id",
            "speciality": "_sort_speciality_id",
            "company": "_sort_company_id",
            "responsible_employee": "_sort_responsible_employee_id",
            "client_category": "_sort_client_category_id",
            "product_group": "_sort_product_group_id",
        }

        sort_by = getattr(filters, "sort_by", None) if filters else None
        sort_order = getattr(filters, "sort_order", None) if filters else None

        if sort_by and sort_by in sort_map:
            key = sort_map[sort_by]
            reverse = sort_order.upper() == "DESC" if sort_order else False
            all_rows.sort(key=lambda x: (x[key] is None, x[key] or ""), reverse=reverse)
        else:
            all_rows.sort(key=lambda x: x["_sort_created_at"] or 0, reverse=True)

        total_count = len(all_rows)

        # --- Пагинация ---
        offset = filters.offset if filters else 0
        limit = filters.limit if filters else None
        if limit:
            page = all_rows[offset : offset + limit]
        else:
            page = all_rows[offset:]

        hasPrev = offset > 0
        hasNext = len(page) == limit if limit else False

        return PaginatedResponse(
            result=page,
            hasPrev=hasPrev,
            hasNext=hasNext,
            count=total_count,
        )

    async def iter_multi(
        self,
        session: "AsyncSession",
        load_options: list[Any] | None = None,
        chunk_size: int = 1000,
    ) -> AsyncIterator[dict]:
        # --- GlobalDoctor ---
        global_stmt = select(GlobalDoctor).options(
            selectinload(GlobalDoctor.medical_facility),
            selectinload(GlobalDoctor.speciality),
        )
        global_result = await session.execute(global_stmt)
        global_items = global_result.scalars().all()

        for item in global_items:
            yield {
                "id": item.id,
                "mode": "global",
                "full_name": item.full_name,
                "medical_facility": item.medical_facility,
                "speciality": item.speciality,
                "responsible_employee": None,
                "client_category": None,
                "product_group": None,
                "company": None,
            }

        # --- Doctor ---
        company_stmt = select(Doctor)
        if load_options:
            company_stmt = company_stmt.options(*load_options)
        company_stmt = company_stmt.join(Doctor.global_doctor)
        company_stmt = ListQueryHelper.apply_sorting_with_created(
            company_stmt, Doctor.created_at.desc()
        )

        stream = await session.stream_scalars(
            company_stmt.execution_options(yield_per=chunk_size)
        )
        async for item in stream:
            yield {
                "id": item.id,
                "mode": "company",
                "full_name": item.global_doctor.full_name,
                "medical_facility": item.global_doctor.medical_facility,
                "speciality": item.global_doctor.speciality,
                "responsible_employee": item.responsible_employee,
                "client_category": item.client_category,
                "product_group": item.product_group,
                "company": item.company,
            }

    async def import_excel(
        self, session: "AsyncSession", file: "UploadFile", user_id: int
    ):
        records = await parse_excel_file(file)

        validate_required_columns(records, client.doctor_fields)

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table="Врачи",
            records_count=len(records),
            target_table_name=self.model.__tablename__,
        )
        session.add(import_log)
        await session.flush()

        resolved = await resolve_records_fields(
            session, records, client.doctor_fields, self.get_id_map
        )

        skipped_records = []
        global_data = {}  # (full_name, mf_id, spec_id) -> GlobalDoctor
        doctor_data = []

        for idx, r in enumerate(records):
            missing_keys = resolved.collect_missing_keys(r, client.doctor_fields)

            ids, null_keys = resolved.resolve_id_fields(r, client.doctor_fields)
            if null_keys:
                missing_keys.extend(null_keys)

            access_raw = (
                r.get("уровень доступа")
                or r.get("уровень")
                or r.get("доступ")
                or r.get("access level")
                or r.get("access")
            )
            access_raw_str = str(access_raw).strip().lower() if access_raw else ""

            global_values = {"общий", "обший", "global"}
            company_values = {"компания", "company"}

            if access_raw_str in global_values:
                mode = "global"
            elif access_raw_str in company_values:
                mode = "company"
            else:
                mode = "company"

            full_name = r.get("фио")
            medical_facility_id = ids.get("medical_facility_id")
            speciality_id = ids.get("speciality_id")

            # Если mode=company но нет company_id или product_group_id — пропускаем
            if mode == "company":
                missing = []
                if not ids.get("company_id"):
                    missing.append("Отсутствует компания")
                if not ids.get("product_group_id"):
                    missing.append("Отсутствует группа товара")
                if missing:
                    skipped_records.append(
                        {
                            "row": idx + 1,
                            "missing": missing,
                        }
                    )
                    continue

            # Собираем GlobalDoctor данные
            gd_key = (full_name, medical_facility_id, speciality_id)
            if gd_key not in global_data:
                global_data[gd_key] = None

            # Собираем Doctor данные (только для mode=company)
            if mode == "company":
                doctor_data.append(
                    {
                        "company_id": ids.get("company_id"),
                        "medical_facility_id": medical_facility_id,
                        "speciality_id": speciality_id,
                        "full_name": full_name,
                        "product_group_id": ids.get("product_group_id"),
                        "client_category_id": ids.get("client_category_id"),
                        "responsible_employee_id": ids.get("responsible_employee_id"),
                        "import_log_id": import_log.id,
                    }
                )

        # 1. Найти или создать GlobalDoctor
        gd_keys = list(global_data.keys())
        if gd_keys:
            existing = await session.execute(
                select(GlobalDoctor).where(
                    GlobalDoctor.full_name.in_([k[0] for k in gd_keys]),
                    GlobalDoctor.medical_facility_id.in_([k[1] for k in gd_keys]),
                    GlobalDoctor.speciality_id.in_([k[2] for k in gd_keys]),
                )
            )
            for gd in existing.scalars().all():
                key = (gd.full_name, gd.medical_facility_id, gd.speciality_id)
                global_data[key] = gd

            # Создать недостающие
            for key in gd_keys:
                if global_data[key] is None:
                    gd = GlobalDoctor(
                        full_name=key[0],
                        medical_facility_id=key[1],
                        speciality_id=key[2],
                    )
                    session.add(gd)
                    global_data[key] = gd

            await session.flush()

        # 2. Вставить Doctor
        BATCH_SIZE = 3000
        inserted_ids = []
        if doctor_data:
            for d in doctor_data:
                gd_key = (d["full_name"], d["medical_facility_id"], d["speciality_id"])
                gd = global_data.get(gd_key)
                if gd:
                    d["global_doctor_id"] = gd.id
                del d["full_name"]
                del d["medical_facility_id"]
                del d["speciality_id"]

            for i in range(0, len(doctor_data), BATCH_SIZE):
                batch = doctor_data[i : i + BATCH_SIZE]
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
            deduplicated=len(doctor_data) - len(inserted_ids),
        )
