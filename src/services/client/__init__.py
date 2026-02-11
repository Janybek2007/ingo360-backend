from src.db.models import clients
from src.services.client.client_category import ClientCategoryService
from src.services.client.distributor import DistributorService
from src.services.client.doctor import DoctorService
from src.services.client.geo_indicator import GeoIndicatorService
from src.services.client.medical_facility import MedicalFacilityService
from src.services.client.pharmacy import PharmacyService
from src.services.client.speciality import SpecialityService

client_category_service = ClientCategoryService(clients.ClientCategory)
doctor_service = DoctorService(clients.Doctor)
pharmacy_service = PharmacyService(clients.Pharmacy)
speciality_service = SpecialityService(clients.Speciality)
medical_facility_service = MedicalFacilityService(clients.MedicalFacility)
distributor_service = DistributorService(clients.Distributor)
geo_indicator_service = GeoIndicatorService(clients.GeoIndicator)
