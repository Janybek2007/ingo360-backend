from src.db.models import geography
from src.services.geography.country import CountryService
from src.services.geography.district import DistrictService
from src.services.geography.region import RegionService
from src.services.geography.settlement import SettlementService

country_service = CountryService(geography.Country)
region_service = RegionService(geography.Region)
settlement_service = SettlementService(geography.Settlement)
district_service = DistrictService(geography.District)
