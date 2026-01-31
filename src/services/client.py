import asyncio
from typing import TYPE_CHECKING

from fastapi import UploadFile, HTTPException, status
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from .base import BaseService
from src.db.models import (
    clients, ImportLogs, Employee,
    MedicalFacility, Speciality, ProductGroup, ClientCategory,
    Settlement, District, Distributor, Company, Region, GeoIndicator
)
from src.schemas import client
from src.utils.excel_parser import parse_excel_file
from src.utils.mapping import map_record
from src.mapping.clients import (
    doctor_mapping, medical_facility_mapping,
    pharmacy_mapping, distributor_mapping, client_category_mapping,
    speciality_mapping, geo_indicator_mapping
)


if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


class ClientCategoryService(BaseService[clients.ClientCategory, client.ClientCategoryCreate, client.ClientCategoryUpdate]):
    async def import_excel(self, session: 'AsyncSession', file: 'UploadFile', user_id: int):
        records = await parse_excel_file(file)

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table='Категории клиентов',
            records_count=len(records),
        )
        session.add(import_log)
        await session.flush()

        data_to_insert = []
        for r in records:
            relation_fields = {
                'import_log_id': import_log.id,
            }
            data_to_insert.append(map_record(r, client_category_mapping, relation_fields))
        await session.execute(insert(self.model), data_to_insert)
        await session.commit()


class DoctorService(BaseService[clients.Doctor, client.DoctorCreate, client.DoctorUpdate]):
    async def import_excel(self, session: 'AsyncSession', file: 'UploadFile', user_id: int):
        records = await parse_excel_file(file)

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table='Врачи',
            records_count=len(records),
        )
        session.add(import_log)
        await session.flush()

        results = await asyncio.gather(
            self.get_id_map(session, Speciality, 'name', {r['специальность'] for r in records}),
            self.get_id_map(session, ClientCategory, 'name', {r['категория'] for r in records}),
            return_exceptions=True
        )

        speciality_map, missing_specialities = results[0]
        client_category_map, missing_client_categories = results[1]

        employee_values = {r['фио ответственного сотрудника'] for r in records if
                           r['фио ответственного сотрудника'] is not None}
        employee_map, missing_employees = await self.get_id_map(
            session, Employee, 'full_name', employee_values
        ) if employee_values else ({}, set())

        medical_facility_values = {r['лпу'] for r in records if r['лпу'] is not None}
        medical_facility_map, missing_medical_facilities = await self.get_id_map(
            session, MedicalFacility, 'name', medical_facility_values
        ) if medical_facility_values else ({}, set())

        product_group_values = {r['группа'] for r in records if r['группа'] is not None}
        product_group_map, missing_product_groups = await self.get_id_map(
            session, ProductGroup, 'name', product_group_values
        ) if product_group_values else ({}, set())

        skipped_records = []
        data_to_insert = []

        for idx, r in enumerate(records):
            missing_keys = []

            if r['специальность'] in missing_specialities:
                missing_keys.append(f"специальность: {r['специальность']}")

            if r['категория'] in missing_client_categories:
                missing_keys.append(f"категория: {r['категория']}")

            if r['фио ответственного сотрудника'] and r['фио ответственного сотрудника'] in missing_employees:
                missing_keys.append(f"сотрудник: {r['фио ответственного сотрудника']}")

            if r['лпу'] and r['лпу'] in missing_medical_facilities:
                missing_keys.append(f"ЛПУ: {r['лпу']}")

            if r['группа'] and r['группа'] in missing_product_groups:
                missing_keys.append(f"группа: {r['группа']}")

            if missing_keys:
                skipped_records.append({
                    'row': idx + 1,
                    'missing': missing_keys
                })
                continue

            relation_fields = {
                'responsible_employee_id': employee_map.get(r['фио ответственного сотрудника']),
                'medical_facility_id': medical_facility_map.get(r['лпу']),
                'speciality_id': speciality_map[r['специальность']],
                'client_category_id': client_category_map[r['категория']],
                'product_group_id': product_group_map.get(r['группа']),
                'import_log_id': import_log.id,
            }
            data_to_insert.append(map_record(r, doctor_mapping, relation_fields))

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


class PharmacyService(BaseService[clients.Pharmacy, client.PharmacyCreate, client.PharmacyUpdate]):
    async def import_excel(self, session: 'AsyncSession', file: 'UploadFile', user_id: int):
        records = await parse_excel_file(file)

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table='Аптеки',
            records_count=len(records),
        )
        session.add(import_log)
        await session.flush()

        results = await asyncio.gather(
            self.get_id_map(session, ProductGroup, 'name', {r['группа'] for r in records}),
            self.get_id_map(session, Company, 'name', {r['компания'] for r in records}),
            self.get_id_map(session, Region, 'name', {r['область'] for r in records}),
            return_exceptions=True
        )

        product_group_map, missing_product_groups = results[0]
        company_map, missing_companies = results[1]
        region_map, missing_regions = results[2]

        employee_values = {r['фио ответственного сотрудника'] for r in records if
                           r['фио ответственного сотрудника'] is not None}
        employee_map, missing_employees = await self.get_id_map(
            session, Employee, 'full_name', employee_values
        ) if employee_values else ({}, set())

        client_category_values = {r['категория'] for r in records if r['категория'] is not None}
        client_category_map, missing_client_categories = await self.get_id_map(
            session, ClientCategory, 'name', client_category_values
        ) if client_category_values else ({}, set())

        distributor_values = {r['дистрибьютор / сеть'] for r in records if r['дистрибьютор / сеть'] is not None}
        distributor_map, missing_distributors = await self.get_id_map(
            session, Distributor, 'name', distributor_values
        ) if distributor_values else ({}, set())

        geo_indicator_values = {r['индикатор'] for r in records if r['индикатор'] is not None}
        geo_indicator_map, missing_geo_indicators = await self.get_id_map(
            session, GeoIndicator, 'name', geo_indicator_values
        ) if geo_indicator_values else ({}, set())

        settlement_pairs = {
            (r['населенный пункт'], region_map.get(r['область']))
            for r in records
            if r['населенный пункт'] is not None and r['область'] in region_map
        }
        settlement_map, missing_settlements = await self.get_id_map(
            session, Settlement, 'name',
            settlement_pairs,
            filter_field='region_id',
            filter_values=set(region_map.values())
        ) if settlement_pairs else ({}, set())

        district_triples = {
            (r['район'], region_map.get(r['область']), company_map.get(r['компания']))
            for r in records
            if r['район'] is not None
               and r['область'] in region_map
               and r['компания'] in company_map
        }
        district_map = {}
        missing_districts = set()

        if district_triples:
            district_names = {t[0] for t in district_triples}
            region_ids = {t[1] for t in district_triples}
            company_ids = {t[2] for t in district_triples}

            stmt = select(District).where(
                District.name.in_(district_names),
                District.region_id.in_(region_ids),
                District.company_id.in_(company_ids)
            )
            result = await session.execute(stmt)
            districts = result.scalars().all()

            district_map = {
                (d.name, d.region_id, d.company_id): d.id
                for d in districts
            }
            missing_districts = district_triples - district_map.keys()

        skipped_records = []
        data_to_insert = []

        for idx, r in enumerate(records):
            missing_keys = []

            if r['группа'] in missing_product_groups:
                missing_keys.append(f"группа: {r['группа']}")

            if r['компания'] in missing_companies:
                missing_keys.append(f"компания: {r['компания']}")

            if r['область'] in missing_regions:
                missing_keys.append(f"область: {r['область']}")

            if r['фио ответственного сотрудника'] and r['фио ответственного сотрудника'] in missing_employees:
                missing_keys.append(f"сотрудник: {r['фио ответственного сотрудника']}")

            if r['категория'] and r['категория'] in missing_client_categories:
                missing_keys.append(f"категория: {r['категория']}")

            if r['дистрибьютор / сеть'] and r['дистрибьютор / сеть'] in missing_distributors:
                missing_keys.append(f"дистрибьютор: {r['дистрибьютор / сеть']}")

            if r['индикатор'] and r['индикатор'] in missing_geo_indicators:
                missing_keys.append(f"индикатор: {r['индикатор']}")

            region_id = region_map.get(r['область'])
            company_id = company_map.get(r['компания'])

            settlement_id = None
            if r['населенный пункт'] is not None and region_id:
                settlement_key = (r['населенный пункт'], region_id)
                if settlement_key in missing_settlements:
                    missing_keys.append(f"населенный пункт: {r['населенный пункт']}")
                else:
                    settlement_id = settlement_map.get(settlement_key)

            district_id = None
            if r['район'] is not None and region_id and company_id:
                district_key = (r['район'], region_id, company_id)
                if district_key in missing_districts:
                    missing_keys.append(f"район: {r['район']}")
                else:
                    district_id = district_map.get(district_key)

            if missing_keys:
                skipped_records.append({
                    'row': idx + 1,
                    'missing': missing_keys
                })
                continue

            relation_fields = {
                'responsible_employee_id': employee_map.get(r['фио ответственного сотрудника']),
                'client_category_id': client_category_map.get(r['категория']),
                'product_group_id': product_group_map[r['группа']],
                'distributor_id': distributor_map.get(r['дистрибьютор / сеть']),
                'district_id': district_id,
                'settlement_id': settlement_id,
                'company_id': company_id,
                'import_log_id': import_log.id,
                'geo_indicator_id': geo_indicator_map.get(r['индикатор']),
            }
            data_to_insert.append(map_record(r, pharmacy_mapping, relation_fields))

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


class SpecialityService(BaseService[clients.Speciality, client.SpecialityCreate, client.SpecialityUpdate]):
    async def import_excel(self, session: 'AsyncSession', file: 'UploadFile', user_id: int):
        records = await parse_excel_file(file)

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table='Специальности',
            records_count=len(records),
        )
        session.add(import_log)
        await session.flush()

        data_to_insert = []
        for r in records:
            relation_fields = {
                'import_log_id': import_log.id,
            }
            data_to_insert.append(map_record(r, speciality_mapping, relation_fields))
        await session.execute(insert(self.model), data_to_insert)
        await session.commit()


class MedicalFacilityService(BaseService[
    clients.MedicalFacility,
    client.MedicalFacilityCreate,
    client.MedicalFacilityUpdate
]):
    async def import_excel(self, session: 'AsyncSession', file: 'UploadFile', user_id: int):
        records = await parse_excel_file(file)

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table='ЛПУ',
            records_count=len(records),
        )
        session.add(import_log)
        await session.flush()

        results = await asyncio.gather(
            self.get_id_map(session, Region, 'name', {r['область'] for r in records}),
            self.get_id_map(session, Company, 'name', {r['компания'] for r in records}),
            return_exceptions=True
        )

        region_map, missing_regions = results[0]
        company_map, missing_companies = results[1]

        geo_indicator_values = {r['индикатор'] for r in records if r['индикатор'] is not None}
        geo_indicator_map, missing_geo_indicators = await self.get_id_map(
            session, GeoIndicator, 'name', geo_indicator_values
        ) if geo_indicator_values else ({}, set())

        settlement_pairs = {
            (r['населенный пункт'], region_map.get(r['область']))
            for r in records
            if r['населенный пункт'] is not None and r['область'] in region_map
        }
        settlement_map, missing_settlements = await self.get_id_map(
            session, Settlement, 'name',
            settlement_pairs,
            filter_field='region_id',
            filter_values=set(region_map.values())
        ) if settlement_pairs else ({}, set())

        district_triples = {
            (r['район'], region_map.get(r['область']), company_map.get(r['компания']))
            for r in records
            if r['район'] is not None
               and r['область'] in region_map
               and r['компания'] in company_map
        }

        district_map = {}
        missing_districts = set()

        if district_triples:
            district_names = {t[0] for t in district_triples}
            region_ids = {t[1] for t in district_triples}
            company_ids = {t[2] for t in district_triples}

            stmt = select(District).where(
                District.name.in_(district_names),
                District.region_id.in_(region_ids),
                District.company_id.in_(company_ids)
            )
            result = await session.execute(stmt)
            districts = result.scalars().all()

            district_map = {
                (d.name, d.region_id, d.company_id): d.id
                for d in districts
            }
            missing_districts = district_triples - district_map.keys()

        skipped_records = []
        data_to_insert = []

        for idx, r in enumerate(records):
            missing_keys = []

            if r['область'] in missing_regions:
                missing_keys.append(f"область: {r['область']}")

            if r['компания'] in missing_companies:
                missing_keys.append(f"компания: {r['компания']}")

            if r['индикатор'] and r['индикатор'] in missing_geo_indicators:
                missing_keys.append(f"индикатор: {r['индикатор']}")

            region_id = region_map.get(r['область'])
            company_id = company_map.get(r['компания'])

            settlement_id = None
            if r['населенный пункт'] is not None and region_id:
                settlement_key = (r['населенный пункт'], region_id)
                if settlement_key in missing_settlements:
                    missing_keys.append(f"населенный пункт: {r['населенный пункт']}")
                else:
                    settlement_id = settlement_map.get(settlement_key)

            district_id = None
            if r['район'] is not None and region_id and company_id:
                district_key = (r['район'], region_id, company_id)
                if district_key in missing_districts:
                    missing_keys.append(f"район: {r['район']}")
                else:
                    district_id = district_map.get(district_key)

            if missing_keys:
                skipped_records.append({
                    'row': idx + 1,
                    'missing': missing_keys
                })
                continue

            relation_fields = {
                'district_id': district_id,
                'settlement_id': settlement_id,
                'import_log_id': import_log.id,
                'geo_indicator_id': geo_indicator_map.get(r['индикатор']),
            }
            data_to_insert.append(map_record(r, medical_facility_mapping, relation_fields))

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


class DistributorService(BaseService[
    clients.Distributor,
    client.DistributorCreate,
    client.DistributorUpdate
]):

    async def import_excel(self, session: 'AsyncSession', file: 'UploadFile', user_id: int):
        records = await parse_excel_file(file)

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table='Дистрибьюторы',
            records_count=len(records),
        )
        session.add(import_log)
        await session.flush()

        data_to_insert = []
        for r in records:
            relation_fields = {
                'import_log_id': import_log.id,
            }
            data_to_insert.append(map_record(r, distributor_mapping, relation_fields))
        await session.execute(insert(self.model), data_to_insert)
        await session.commit()


class GeoIndicatorService(BaseService[GeoIndicator, client.GeoIndicatorCreate, client.GeoIndicatorUpdate]):

    async def import_excel(self, session: 'AsyncSession', file: 'UploadFile', user_id: int):
        records = await parse_excel_file(file)

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table='Индикаторы Аптек/ЛПУ',
            records_count=len(records),
        )
        session.add(import_log)
        await session.flush()

        data_to_insert = []
        for r in records:
            relation_fields = {
                'import_log_id': import_log.id,
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
