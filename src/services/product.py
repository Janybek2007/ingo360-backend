import asyncio
from typing import TYPE_CHECKING, Any, Sequence

from fastapi import UploadFile
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from src.db.models import (
    Brand,
    Company,
    Dosage,
    DosageForm,
    ImportLogs,
    ProductGroup,
    PromotionType,
    Segment,
    products,
)
from src.mapping.products import (
    brand_mapping,
    dosage_form_mapping,
    dosage_mapping,
    product_group_mapping,
    promotion_type_mapping,
    segment_mapping,
    sku_mapping,
)
from src.schemas import product
from src.utils.excel_parser import parse_excel_file
from src.utils.import_result import build_import_result
from src.utils.mapping import map_record

from .base import BaseService
from .list_query_helper import ListQueryHelper

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


class BrandService(
    BaseService[products.Brand, product.BrandCreate, product.BrandUpdate]
):
    async def get_multi(
        self,
        session: "AsyncSession",
        filters: product.BrandListRequest | None = None,
        load_options: list[Any] | None = None,
    ) -> Sequence[products.Brand]:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        sort_map = {
            "name": self.model.name,
            "ims_name": self.model.ims_name,
            "promotion_types": self.model.promotion_type_id,
            "product_groups": self.model.product_group_id,
            "companies": self.model.company_id,
        }
        stmt = ListQueryHelper.apply_sorting_with_default(
            stmt,
            getattr(filters, "sort_by", None),
            getattr(filters, "sort_order", None),
            sort_map,
            self.model.created_at.desc(),
        )

        if filters:
            if filters.name:
                stmt = stmt.where(self.model.name.ilike(f"%{filters.name}%"))

            if filters.ims_name:
                stmt = stmt.where(self.model.ims_name.ilike(f"%{filters.ims_name}%"))

            stmt = ListQueryHelper.apply_in_or_null(
                stmt, self.model.promotion_type_id, filters.promotion_type_ids
            )
            stmt = ListQueryHelper.apply_in_or_null(
                stmt, self.model.product_group_id, filters.product_group_ids
            )
            stmt = ListQueryHelper.apply_in_or_null(
                stmt, self.model.company_id, filters.company_ids
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
            target_table="Бренды",
            records_count=len(records),
        )
        session.add(import_log)
        await session.flush()

        promotion_type_map, missing_promotion_types = await self.get_id_map(
            session, PromotionType, "name", {r["тип промоции"] for r in records}
        )
        product_group_map, missing_product_groups = await self.get_id_map(
            session, ProductGroup, "name", {r["группа"] for r in records}
        )
        company_map, missing_companies = await self.get_id_map(
            session, Company, "name", {r["компания"] for r in records}
        )

        existing_rows = await session.execute(select(Brand.name, Brand.ims_name))
        existing_pairs = existing_rows.all()
        existing_names = {row[0] for row in existing_pairs if row[0]}
        existing_ims_names = {row[1] for row in existing_pairs if row[1]}
        seen_names: set[str] = set()
        seen_ims_names: set[str] = set()

        data_to_insert = []
        skipped_records = []
        for r in records:
            if "название ims" not in r and "название в ims" in r:
                r["название ims"] = r.get("название в ims")
            if "название ims" not in r:
                r["название ims"] = None
            missing_keys = []
            if r["тип промоции"] in missing_promotion_types:
                missing_keys.append(f"тип промоции: {r['тип промоции']}")
            if r["группа"] in missing_product_groups:
                missing_keys.append(f"группа: {r['группа']}")
            if r["компания"] in missing_companies:
                missing_keys.append(f"компания: {r['компания']}")

            if missing_keys:
                skipped_records.append({"row": r.get("row"), "missing": missing_keys})
                continue

            brand_name = r.get("название")
            ims_name = r.get("название ims")
            if brand_name and (
                brand_name in existing_names or brand_name in seen_names
            ):
                continue
            if ims_name and (
                ims_name in existing_ims_names or ims_name in seen_ims_names
            ):
                continue

            relation_fields = {
                "promotion_type_id": promotion_type_map[r["тип промоции"]],
                "product_group_id": product_group_map[r["группа"]],
                "company_id": company_map[r["компания"]],
                "import_log_id": import_log.id,
            }
            data_to_insert.append(map_record(r, brand_mapping, relation_fields))
            if brand_name:
                seen_names.add(brand_name)
            if ims_name:
                seen_ims_names.add(ims_name)
        if data_to_insert:
            stmt = insert(self.model).on_conflict_do_nothing()
            await session.execute(stmt, data_to_insert)
        await session.commit()

        return build_import_result(
            total=len(records),
            imported=len(data_to_insert),
            skipped_records=skipped_records,
        )


class PromotionTypeService(
    BaseService[
        products.PromotionType, product.PromotionTypeCreate, product.PromotionTypeUpdate
    ]
):
    async def get_multi(
        self,
        session: "AsyncSession",
        filters: product.PromotionTypeListRequest | None = None,
        load_options: list[Any] | None = None,
    ) -> Sequence[products.PromotionType]:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        stmt = ListQueryHelper.apply_sorting_with_default(
            stmt,
            getattr(filters, "sort_by", None),
            getattr(filters, "sort_order", None),
            {"name": self.model.name},
            self.model.created_at.desc(),
        )
        stmt = ListQueryHelper.apply_pagination(stmt, filters.limit, filters.offset)

        if filters:
            if filters.name:
                stmt = stmt.where(self.model.name.ilike(f"%{filters.name}%"))

        result = await session.execute(stmt)
        return result.unique().scalars().all()

    async def import_excel(
        self, session: "AsyncSession", file: "UploadFile", user_id: int
    ):
        records = await parse_excel_file(file)

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table="Тип промоции",
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
                map_record(r, promotion_type_mapping, relation_fields)
            )

        await session.execute(insert(self.model), data_to_insert)
        await session.commit()


class DosageFormService(
    BaseService[products.DosageForm, product.DosageFormCreate, product.DosageFormUpdate]
):
    async def get_multi(
        self,
        session: "AsyncSession",
        filters: product.DosageFormListRequest | None = None,
        load_options: list[Any] | None = None,
    ) -> Sequence[products.DosageForm]:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        stmt = ListQueryHelper.apply_sorting_with_default(
            stmt,
            getattr(filters, "sort_by", None),
            getattr(filters, "sort_order", None),
            {"name": self.model.name},
            self.model.created_at.desc(),
        )

        if filters:
            if filters.name:
                stmt = stmt.where(self.model.name.ilike(f"%{filters.name}%"))

            stmt = ListQueryHelper.apply_pagination(stmt, filters.limit, filters.offset)

        result = await session.execute(stmt)
        return result.unique().scalars().all()

    async def import_excel(
        self, session: "AsyncSession", file: "UploadFile", user_id: int
    ):
        records = await parse_excel_file(file)

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table="Формы выпуска",
            records_count=len(records),
        )
        session.add(import_log)
        await session.flush()

        data_to_insert = []
        for r in records:
            relation_fields = {
                "import_log_id": import_log.id,
            }
            data_to_insert.append(map_record(r, dosage_form_mapping, relation_fields))

        await session.execute(insert(self.model), data_to_insert)
        await session.commit()


class DosageService(
    BaseService[products.Dosage, product.DosageCreate, product.DosageUpdate]
):
    async def get_multi(
        self,
        session: "AsyncSession",
        filters: product.DosageListRequest | None = None,
        load_options: list[Any] | None = None,
    ) -> Sequence[products.Dosage]:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        stmt = ListQueryHelper.apply_sorting_with_default(
            stmt,
            getattr(filters, "sort_by", None),
            getattr(filters, "sort_order", None),
            {"name": self.model.name},
            self.model.created_at.desc(),
        )
        if filters:
            if filters.name:
                stmt = stmt.where(self.model.name.ilike(f"%{filters.name}%"))

            stmt = ListQueryHelper.apply_pagination(stmt, filters.limit, filters.offset)

        result = await session.execute(stmt)
        return result.unique().scalars().all()

    async def import_excel(
        self, session: "AsyncSession", file: "UploadFile", user_id: int
    ):
        records = await parse_excel_file(file, read_as_str=True)

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table="Дозировка",
            records_count=len(records),
        )
        session.add(import_log)
        await session.flush()

        data_to_insert = []
        for r in records:
            relation_fields = {
                "import_log_id": import_log.id,
            }
            data_to_insert.append(map_record(r, dosage_mapping, relation_fields))

        await session.execute(insert(self.model), data_to_insert)
        await session.commit()


class SegmentService(
    BaseService[products.Segment, product.SegmentCreate, product.SegmentUpdate]
):
    async def get_multi(
        self,
        session: "AsyncSession",
        filters: product.SegmentListRequest | None = None,
        load_options: list[Any] | None = None,
    ) -> Sequence[products.Segment]:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        stmt = ListQueryHelper.apply_sorting_with_default(
            stmt,
            getattr(filters, "sort_by", None),
            getattr(filters, "sort_order", None),
            {"name": self.model.name},
            self.model.created_at.desc(),
        )

        if filters:
            if filters.name:
                stmt = stmt.where(self.model.name.ilike(f"%{filters.name}%"))

            stmt = ListQueryHelper.apply_pagination(stmt, filters.limit, filters.offset)

        result = await session.execute(stmt)
        return result.unique().scalars().all()

    async def import_excel(
        self, session: "AsyncSession", file: "UploadFile", user_id: int
    ):
        records = await parse_excel_file(file)

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table="Сегменты",
            records_count=len(records),
        )
        session.add(import_log)
        await session.flush()

        data_to_insert = []
        for r in records:
            relation_fields = {
                "import_log_id": import_log.id,
            }
            data_to_insert.append(map_record(r, segment_mapping, relation_fields))

        await session.execute(insert(self.model), data_to_insert)
        await session.commit()


class SKUService(BaseService[products.SKU, product.SKUCreate, product.SKUUpdate]):
    async def get_multi(
        self,
        session: "AsyncSession",
        filters: product.SKUListRequest | None = None,
        load_options: list[Any] | None = None,
    ) -> Sequence[products.SKU]:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        sort_map = {
            "name": self.model.name,
            "brands": self.model.brand_id,
            "promotion_types": self.model.promotion_type_id,
            "product_groups": self.model.product_group_id,
            "dosage_forms": self.model.dosage_form_id,
            "dosages": self.model.dosage_id,
            "segments": self.model.segment_id,
            "companies": self.model.company_id,
        }
        stmt = ListQueryHelper.apply_sorting_with_default(
            stmt,
            getattr(filters, "sort_by", None),
            getattr(filters, "sort_order", None),
            sort_map,
            self.model.created_at.desc(),
        )

        if filters:
            if filters.name:
                stmt = stmt.where(self.model.name.ilike(f"%{filters.name}%"))

            stmt = ListQueryHelper.apply_in_or_null(
                stmt, self.model.brand_id, filters.brand_ids
            )
            stmt = ListQueryHelper.apply_in_or_null(
                stmt, self.model.promotion_type_id, filters.promotion_type_ids
            )
            stmt = ListQueryHelper.apply_in_or_null(
                stmt, self.model.product_group_id, filters.product_group_ids
            )
            stmt = ListQueryHelper.apply_in_or_null(
                stmt, self.model.dosage_form_id, filters.dosage_form_ids
            )
            stmt = ListQueryHelper.apply_in_or_null(
                stmt, self.model.dosage_id, filters.dosage_ids
            )
            stmt = ListQueryHelper.apply_in_or_null(
                stmt, self.model.segment_id, filters.segment_ids
            )
            stmt = ListQueryHelper.apply_in_or_null(
                stmt, self.model.company_id, filters.company_ids
            )

            stmt = ListQueryHelper.apply_pagination(stmt, filters.limit, filters.offset)

        result = await session.execute(stmt)
        return result.unique().scalars().all()

    async def import_excel(
        self, session: "AsyncSession", file: "UploadFile", user_id: int
    ):
        records = await parse_excel_file(file, read_as_str=True)

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table="SKU",
            records_count=len(records),
        )
        session.add(import_log)
        await session.flush()

        results = await asyncio.gather(
            self.get_id_map(session, Brand, "name", {r["бренд"] for r in records}),
            self.get_id_map(
                session, DosageForm, "name", {r["форма выпуска"] for r in records}
            ),
            self.get_id_map(
                session, PromotionType, "name", {r["тип промоции"] for r in records}
            ),
            self.get_id_map(session, Company, "name", {r["компания"] for r in records}),
            self.get_id_map(session, Segment, "name", {r["сегмент"] for r in records}),
            self.get_id_map(session, Dosage, "name", {r["дозировка"] for r in records}),
            return_exceptions=True,
        )

        brand_map, missing_brands = results[0]
        dosage_form_map, missing_dosage_forms = results[1]
        promotion_type_map, missing_promotion_types = results[2]
        company_map, missing_companies = results[3]
        segment_map, missing_segments = results[4]
        dosage_map, missing_dosages = results[5]

        product_group_pairs = {
            (r["группа"], company_map.get(r["компания"]))
            for r in records
            if r["компания"] in company_map
        }
        product_group_map, missing_product_groups = (
            await self.get_id_map(
                session,
                ProductGroup,
                "name",
                product_group_pairs,
                "company_id",
                set(company_map.values()),
            )
            if product_group_pairs
            else ({}, set())
        )

        skipped_records = []
        data_to_insert = []

        for idx, r in enumerate(records):
            missing_keys = []

            if r["бренд"] in missing_brands:
                missing_keys.append(f"бренд: {r['бренд']}")

            if r["форма выпуска"] in missing_dosage_forms:
                missing_keys.append(f"форма выпуска: {r['форма выпуска']}")

            if r["тип промоции"] in missing_promotion_types:
                missing_keys.append(f"тип промоции: {r['тип промоции']}")

            if r["компания"] in missing_companies:
                missing_keys.append(f"компания: {r['компания']}")

            segment_value = r.get("сегмент")
            if segment_value and segment_value in missing_segments:
                missing_keys.append(f"сегмент: {segment_value}")

            dosage_value = r.get("дозировка")
            if dosage_value and dosage_value in missing_dosages:
                missing_keys.append(f"дозировка: {dosage_value}")

            company_id = company_map.get(r["компания"])
            if company_id:
                product_group_key = (r["группа"], company_id)
                if product_group_key in missing_product_groups:
                    missing_keys.append(f"группа: {r['группа']}")

            if missing_keys:
                skipped_records.append({"row": idx + 1, "missing": missing_keys})
                continue

            relation_fields = {
                "brand_id": brand_map[r["бренд"]],
                "dosage_form_id": dosage_form_map[r["форма выпуска"]],
                "product_group_id": product_group_map[(r["группа"], company_id)],
                "promotion_type_id": promotion_type_map[r["тип промоции"]],
                "company_id": company_id,
                "import_log_id": import_log.id,
            }
            if dosage_value:
                relation_fields["dosage_id"] = dosage_map[dosage_value]
            if segment_value:
                relation_fields["segment_id"] = segment_map[segment_value]
            data_to_insert.append(map_record(r, sku_mapping, relation_fields))

        if data_to_insert:
            stmt = insert(self.model).on_conflict_do_nothing()
            await session.execute(stmt, data_to_insert)

        await session.commit()

        return build_import_result(
            total=len(records),
            imported=len(data_to_insert),
            skipped_records=skipped_records,
        )


class ProductGroupService(
    BaseService[
        products.ProductGroup, product.ProductGroupCreate, product.ProductGroupUpdate
    ]
):
    async def get_multi(
        self,
        session: "AsyncSession",
        filters: product.ProductGroupListRequest | None = None,
        load_options: list[Any] | None = None,
    ) -> Sequence[products.ProductGroup]:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        sort_map = {
            "name": self.model.name,
            "companies": self.model.company_id,
        }

        stmt = ListQueryHelper.apply_sorting_with_default(
            stmt,
            getattr(filters, "sort_by", None),
            getattr(filters, "sort_order", None),
            sort_map,
            self.model.created_at.desc(),
        )

        if filters:
            if filters.name:
                stmt = stmt.where(self.model.name.ilike(f"%{filters.name}%"))

            stmt = ListQueryHelper.apply_in_or_null(
                stmt, self.model.company_id, filters.company_ids
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
            target_table="Группы",
            records_count=len(records),
        )
        session.add(import_log)
        await session.flush()

        company_map = await self.get_id_map(
            session, Company, "name", {r["компания"] for r in records}
        )

        data_to_insert = []
        for r in records:
            relation_fields = {
                "company_id": company_map[r["компания"]],
                "import_log_id": import_log.id,
            }
            data_to_insert.append(map_record(r, product_group_mapping, relation_fields))

        await session.execute(insert(self.model), data_to_insert)
        await session.commit()


brand_service = BrandService(products.Brand)
promotion_type_service = PromotionTypeService(products.PromotionType)
dosage_form_service = DosageFormService(products.DosageForm)
dosage_service = DosageService(products.Dosage)
segment_service = SegmentService(products.Segment)
sku_service = SKUService(products.SKU)
product_group_service = ProductGroupService(products.ProductGroup)
