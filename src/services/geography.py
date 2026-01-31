from typing import TYPE_CHECKING

from sqlalchemy import insert
from fastapi import UploadFile

from .base import BaseService
from src.db.models import geography, ImportLogs, Region, Country, District, Settlement, Company
from src.schemas import geography as geography_schema
from src.utils.excel_parser import parse_excel_file
from src.utils.mapping import map_record
from src.mapping.geography import region_mapping, settlement_mapping, district_mapping


if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


class CountryService(BaseService[geography.Country, geography_schema.CountryCreate, geography_schema.CountryUpdate]):
    async def import_excel(self, session: 'AsyncSession', file: 'UploadFile', user_id: int):
        records = await parse_excel_file(file)

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table='Страны',
            records_count=len(records),
        )
        session.add(import_log)
        await session.flush()

        data_to_insert = []
        for r in records:
            relation_fields = {
                'import_log_id': import_log.id,
            }
            data_to_insert.append(map_record(r, region_mapping, relation_fields))
        await session.execute(insert(self.model), data_to_insert)
        await session.commit()


class RegionService(BaseService[geography.Region, geography_schema.RegionCreate, geography_schema.RegionUpdate]):
    async def import_excel(self, session: 'AsyncSession', file: 'UploadFile', user_id: int):
        records = await parse_excel_file(file)

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table='Области',
            records_count=len(records),
        )
        session.add(import_log)
        await session.flush()

        country_map = await self.get_id_map(session, Country, 'name', {r['страна'] for r in records})

        data_to_insert = []
        for r in records:
            relation_fields = {
                'country_id': country_map[r['страна']],
                'import_log_id': import_log.id,
            }
            data_to_insert.append(map_record(r, region_mapping, relation_fields))
        await session.execute(insert(self.model), data_to_insert)
        await session.commit()


class SettlementService(BaseService[geography.Settlement, geography_schema.SettlementCreate, geography_schema.SettlementUpdate]):
    async def import_excel(self, session: 'AsyncSession', file: 'UploadFile', user_id: int):
        records = await parse_excel_file(file)

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table='Населенные пункты',
            records_count=len(records),
        )
        session.add(import_log)
        await session.flush()

        region_map = await self.get_id_map(session, Region, 'name', {r['область'] for r in records})

        data_to_insert = []
        for r in records:
            relation_fields = {
                'region_id': region_map[r['область']],
                'import_log_id': import_log.id,
            }
            data_to_insert.append(map_record(r, region_mapping, relation_fields))
        await session.execute(insert(self.model), data_to_insert)
        await session.commit()


class DistrictService(BaseService[geography.District, geography_schema.DistrictCreate, geography_schema.DistrictUpdate]):
    async def import_excel(self, session: 'AsyncSession', file: 'UploadFile', user_id: int):
        records = await parse_excel_file(file)

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table='Районы',
            records_count=len(records),
        )
        session.add(import_log)
        await session.flush()

        region_map = await self.get_id_map(
            session, Region, 'name',
            {r['область'] for r in records}
        )
        company_map = await self.get_id_map(
            session, Company, 'name',
            {r['компания'] for r in records}
        )

        settlement_pairs = {
            (r['населенный пункт'], region_map[r['область']])
            for r in records
            if r['населенный пункт'] is not None
        }

        settlement_map = await self.get_id_map(
            session, Settlement, 'name',
            settlement_pairs,
            filter_field='region_id',
            filter_values=set(region_map.values())
        ) if settlement_pairs else {}

        data_to_insert = []
        for r in records:
            region_id = region_map[r['область']]

            settlement_id = None
            if r['населенный пункт'] is not None:
                settlement_key = (r['населенный пункт'], region_id)
                settlement_id = settlement_map.get(settlement_key)

            relation_fields = {
                'company_id': company_map[r['компания']],
                'region_id': region_id,
                'settlement_id': settlement_id,
                'import_log_id': import_log.id,
            }
            data_to_insert.append(map_record(r, district_mapping, relation_fields))
        await session.execute(insert(self.model), data_to_insert)
        await session.commit()


country_service = CountryService(geography.Country)
region_service = RegionService(geography.Region)
settlement_service = SettlementService(geography.Settlement)
district_service = DistrictService(geography.District)
