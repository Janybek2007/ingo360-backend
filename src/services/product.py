import asyncio
from typing import TYPE_CHECKING

from sqlalchemy.dialects.postgresql import insert
from fastapi import UploadFile

from .base import BaseService
from src.db.models import (
    products, ImportLogs, Company, ProductGroup,
    ClientCategory, Brand, Segment, Dosage, DosageForm,
    MedicalFacility, PromotionType
)
from src.schemas import product
from src.utils.excel_parser import parse_excel_file
from src.utils.mapping import map_record
from src.mapping.products import (
    brand_mapping, sku_mapping, dosage_form_mapping,
    dosage_mapping, segment_mapping, promotion_type_mapping, product_group_mapping,
)


if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


class BrandService(BaseService[products.Brand, product.BrandCreate, product.BrandUpdate]):
    async def import_excel(self, session: 'AsyncSession', file: 'UploadFile', user_id: int):
        records = await parse_excel_file(file)

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table='Бренды',
            records_count=len(records),
        )
        session.add(import_log)
        await session.flush()

        promotion_type_map = await self.get_id_map(
            session, PromotionType, 'name',
            {r['тип промоции'] for r in records}
        )
        product_group_map = await self.get_id_map(session, ProductGroup, 'name', {r['группа'] for r in records})
        company_map = await self.get_id_map(session, Company, 'name', {r['компания'] for r in records})

        data_to_insert = []
        for r in records:
            relation_fields = {
                'promotion_type_id': promotion_type_map[r['тип промоции']],
                'product_group_id': product_group_map[r['группа']],
                'company_id': company_map[r['компания']],
                'import_log_id': import_log.id,
            }
            data_to_insert.append(map_record(r, brand_mapping, relation_fields))
        await session.execute(insert(self.model), data_to_insert)
        await session.commit()


class PromotionTypeService(BaseService[products.PromotionType, product.PromotionTypeCreate, product.PromotionTypeUpdate]):
    async def import_excel(self, session: 'AsyncSession', file: 'UploadFile', user_id: int):
        records = await parse_excel_file(file)

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table='Тип промоции',
            records_count=len(records),
        )
        session.add(import_log)
        await session.flush()

        data_to_insert = []
        for r in records:
            relation_fields = {
                'import_log_id': import_log.id,
            }
            data_to_insert.append(map_record(r, promotion_type_mapping, relation_fields))

        await session.execute(insert(self.model), data_to_insert)
        await session.commit()


class DosageFormService(BaseService[products.DosageForm, product.DosageFormCreate, product.DosageFormUpdate]):
    async def import_excel(self, session: 'AsyncSession', file: 'UploadFile', user_id: int):
        records = await parse_excel_file(file)

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table='Формы выпуска',
            records_count=len(records),
        )
        session.add(import_log)
        await session.flush()

        data_to_insert = []
        for r in records:
            relation_fields = {
                'import_log_id': import_log.id,
            }
            data_to_insert.append(map_record(r, dosage_form_mapping, relation_fields))

        await session.execute(insert(self.model), data_to_insert)
        await session.commit()


class DosageService(BaseService[products.Dosage, product.DosageCreate, product.DosageUpdate]):
    async def import_excel(self, session: 'AsyncSession', file: 'UploadFile', user_id: int):
        records = await parse_excel_file(file, read_as_str=True)

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table='Дозировка',
            records_count=len(records),
        )
        session.add(import_log)
        await session.flush()

        data_to_insert = []
        for r in records:
            relation_fields = {
                'import_log_id': import_log.id,
            }
            data_to_insert.append(map_record(r, dosage_mapping, relation_fields))

        await session.execute(insert(self.model), data_to_insert)
        await session.commit()


class SegmentService(BaseService[products.Segment, product.SegmentCreate, product.SegmentUpdate]):
    async def import_excel(self, session: 'AsyncSession', file: 'UploadFile', user_id: int):
        records = await parse_excel_file(file)

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table='Сегменты',
            records_count=len(records),
        )
        session.add(import_log)
        await session.flush()

        data_to_insert = []
        for r in records:
            relation_fields = {
                'import_log_id': import_log.id,
            }
            data_to_insert.append(map_record(r, segment_mapping, relation_fields))

        await session.execute(insert(self.model), data_to_insert)
        await session.commit()


class SKUService(BaseService[products.SKU, product.SKUCreate, product.SKUUpdate]):
    async def import_excel(self, session: 'AsyncSession', file: 'UploadFile', user_id: int):
        records = await parse_excel_file(file, read_as_str=True)

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table='SKU',
            records_count=len(records),
        )
        session.add(import_log)
        await session.flush()

        results = await asyncio.gather(
            self.get_id_map(session, Brand, 'name', {r['бренд'] for r in records}),
            self.get_id_map(session, DosageForm, 'name', {r['форма выпуска'] for r in records}),
            self.get_id_map(session, PromotionType, 'name', {r['тип промоции'] for r in records}),
            self.get_id_map(session, Company, 'name', {r['компания'] for r in records}),
            self.get_id_map(session, Segment, 'name', {r['сегмент'] for r in records}),
            self.get_id_map(session, Dosage, 'name', {r['дозировка'] for r in records}),
            return_exceptions=True
        )

        brand_map, missing_brands = results[0]
        dosage_form_map, missing_dosage_forms = results[1]
        promotion_type_map, missing_promotion_types = results[2]
        company_map, missing_companies = results[3]
        segment_map, missing_segments = results[4]
        dosage_map, missing_dosages = results[5]

        product_group_pairs = {
            (r['группа'], company_map.get(r['компания']))
            for r in records
            if r['компания'] in company_map
        }
        product_group_map, missing_product_groups = await self.get_id_map(
            session, ProductGroup, 'name',
            product_group_pairs,
            'company_id',
            set(company_map.values())
        ) if product_group_pairs else ({}, set())

        skipped_records = []
        data_to_insert = []

        for idx, r in enumerate(records):
            missing_keys = []

            if r['бренд'] in missing_brands:
                missing_keys.append(f"бренд: {r['бренд']}")

            if r['форма выпуска'] in missing_dosage_forms:
                missing_keys.append(f"форма выпуска: {r['форма выпуска']}")

            if r['тип промоции'] in missing_promotion_types:
                missing_keys.append(f"тип промоции: {r['тип промоции']}")

            if r['компания'] in missing_companies:
                missing_keys.append(f"компания: {r['компания']}")

            if r['сегмент'] in missing_segments:
                missing_keys.append(f"сегмент: {r['сегмент']}")

            if r['дозировка'] in missing_dosages:
                missing_keys.append(f"дозировка: {r['дозировка']}")

            company_id = company_map.get(r['компания'])
            if company_id:
                product_group_key = (r['группа'], company_id)
                if product_group_key in missing_product_groups:
                    missing_keys.append(f"группа: {r['группа']}")

            if missing_keys:
                skipped_records.append({
                    'row': idx + 1,
                    'missing': missing_keys
                })
                continue

            relation_fields = {
                'brand_id': brand_map[r['бренд']],
                'dosage_form_id': dosage_form_map[r['форма выпуска']],
                'dosage_id': dosage_map[r['дозировка']],
                'product_group_id': product_group_map[(r['группа'], company_id)],
                'promotion_type_id': promotion_type_map[r['тип промоции']],
                'company_id': company_id,
                'segment_id': segment_map[r['сегмент']],
                'import_log_id': import_log.id,
            }
            data_to_insert.append(map_record(r, sku_mapping, relation_fields))

        if data_to_insert:
            stmt = insert(self.model).on_conflict_do_nothing()
            await session.execute(stmt, data_to_insert)

        await session.commit()

        return {
            'imported': len(data_to_insert),
            'skipped': len(skipped_records),
            'total': len(records),
            'skipped_records': skipped_records
        }


class ProductGroupService(BaseService[products.ProductGroup, product.ProductGroupCreate, product.ProductGroupUpdate]):
    async def import_excel(self, session: 'AsyncSession', file: 'UploadFile', user_id: int):
        records = await parse_excel_file(file)

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table='Группы',
            records_count=len(records),
        )
        session.add(import_log)
        await session.flush()

        company_map = await self.get_id_map(session, Company, 'name', {r['компания'] for r in records})

        data_to_insert = []
        for r in records:
            relation_fields = {
                'company_id': company_map[r['компания']],
                'import_log_id': import_log.id,
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
