from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict

from src.schemas.base_filter import BaseReferenceFilter
from src.schemas.company import CompanySimpleResponse
from src.schemas.employee import EmployeeSimpleResponse
from src.schemas.geography import DistrictSimpleResponse, SettlementSimpleResponse
from src.schemas.product import ProductGroupSimpleResponse


class GeoIndicatorCreate(BaseModel):
    name: str


class GeoIndicatorUpdate(BaseModel):
    name: str | None = None


class GeoIndicatorResponse(GeoIndicatorCreate):
    model_config = ConfigDict(from_attributes=True)

    id: int


class ClientCategoryCreate(BaseModel):
    name: str


class DoctorCreate(BaseModel):
    full_name: str
    responsible_employee_id: int | None = None
    medical_facility_id: int
    speciality_id: int
    client_category_id: int | None = None
    product_group_id: int
    company_id: int


class PharmacyCreate(BaseModel):
    name: str
    distributor_id: int | None = None
    responsible_employee_id: int | None = None
    settlement_id: int | None = None
    district_id: int | None = None
    client_category_id: int | None = None
    product_group_id: int
    company_id: int
    geo_indicator_id: int | None = None


class SpecialityCreate(BaseModel):
    name: str


class MedicalFacilityCreate(BaseModel):
    name: str
    address: str | None = None
    settlement_id: int
    district_id: int | None = None
    facility_type: str | None = None
    geo_indicator_id: int | None = None


class DistributorCreate(BaseModel):
    name: str


class ClientCategoryUpdate(BaseModel):
    name: str | None = None


class DoctorUpdate(BaseModel):
    full_name: str | None = None
    responsible_employee_id: int | None = None
    medical_facility_id: int | None = None
    speciality_id: int | None = None
    client_category_id: int | None = None
    product_group_id: int | None = None


class PharmacyUpdate(BaseModel):
    name: str | None = None
    distributor_id: int | None = None
    responsible_employee_id: int | None = None
    settlement_id: int | None = None
    district_id: int | None = None
    client_category_id: int | None = None
    product_group_id: int | None = None
    company_id: int | None = None
    geo_indicator_id: int | None = None


class SpecialityUpdate(BaseModel):
    name: str | None = None


class MedicalFacilityUpdate(BaseModel):
    name: str | None = None
    address: str | None = None
    settlement_id: int | None = None
    district_id: int | None = None
    facility_type: str | None = None
    geo_indicator_id: int | None = None


class DistributorUpdate(BaseModel):
    name: str | None = None


class MedicalFacilitySimpleResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str


class DoctorSimpleResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    full_name: str


class PharmacySimpleResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str
    distributor: Optional["DistributorResponse"]
    geo_indicator: GeoIndicatorResponse | None


class ClientCategoryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str


class SpecialityResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str


class DistributorResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str


class MedicalFacilityResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str
    address: str | None
    settlement: SettlementSimpleResponse
    district: DistrictSimpleResponse | None
    facility_type: str | None = None
    geo_indicator: GeoIndicatorResponse | None


class DoctorResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    full_name: str
    responsible_employee: EmployeeSimpleResponse | None
    medical_facility: MedicalFacilitySimpleResponse | None
    speciality: SpecialityResponse
    client_category: ClientCategoryResponse
    product_group: ProductGroupSimpleResponse | None


class PharmacyResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str
    distributor: DistributorResponse | None
    responsible_employee: EmployeeSimpleResponse | None
    settlement: SettlementSimpleResponse | None
    district: DistrictSimpleResponse | None
    client_category: ClientCategoryResponse | None
    product_group: ProductGroupSimpleResponse
    company: CompanySimpleResponse
    geo_indicator: GeoIndicatorResponse | None


class MedicalFacilityFilter(BaseReferenceFilter):
    name: str | None = None
    facility_type: str | None = None
    settlements: str | None = None
    districts: str | None = None
    geo_indicators: str | None = None


class ClientCategoryFilter(BaseReferenceFilter):
    name: str | None = None


class SpecialityFilter(BaseReferenceFilter):
    name: str | None = None


class DistributorFilter(BaseReferenceFilter):
    name: str | None = None


class GeoIndicatorFilter(BaseReferenceFilter):
    name: str | None = None


class DoctorFilter(BaseReferenceFilter):
    full_name: str | None = None
    medical_facilities: str | None = None
    responsible_employees: str | None = None
    specialities: str | None = None
    client_categories: str | None = None
    product_groups: str | None = None


class PharmacyFilter(BaseReferenceFilter):
    name: str | None = None
    companies: str | None = None
    distributors: str | None = None
    responsible_employees: str | None = None
    settlements: str | None = None
    districts: str | None = None
    client_categories: str | None = None
    product_groups: str | None = None
    geo_indicators: str | None = None


class PharmacyListRequest(BaseReferenceFilter):
    name: str | None = None
    company_ids: list[int] | None = None
    distributor_ids: list[int] | None = None
    responsible_employee_ids: list[int] | None = None
    settlement_ids: list[int] | None = None
    district_ids: list[int] | None = None
    client_category_ids: list[int] | None = None
    product_group_ids: list[int] | None = None
    geo_indicator_ids: list[int] | None = None
    sort_by: (
        Literal[
            "name",
            "companies",
            "distributors",
            "responsible_employees",
            "settlements",
            "districts",
            "client_categories",
            "product_groups",
            "geo_indicators",
        ]
        | None
    ) = None


class DoctorListRequest(BaseReferenceFilter):
    full_name: str | None = None
    medical_facility_ids: list[int] | None = None
    responsible_employee_ids: list[int] | None = None
    speciality_ids: list[int] | None = None
    client_category_ids: list[int] | None = None
    product_group_ids: list[int] | None = None
    sort_by: (
        Literal[
            "full_name",
            "medical_facility",
            "responsible_employee",
            "speciality",
            "client_category",
            "product_group",
        ]
        | None
    ) = None


class ClientCategoryListRequest(BaseReferenceFilter):
    name: str | None = None
    sort_by: Literal["name"] | None = None


class SpecialityListRequest(BaseReferenceFilter):
    name: str | None = None
    sort_by: Literal["name"] | None = None


class DistributorListRequest(BaseReferenceFilter):
    offset: int = 0
    name: str | None = None
    sort_by: Literal["name"] | None = None


class GeoIndicatorListRequest(BaseReferenceFilter):
    name: str | None = None
    sort_by: Literal["name"] | None = None


class MedicalFacilityListRequest(BaseReferenceFilter):
    name: str | None = None
    facility_type: str | None = None
    settlement_ids: list[int] | None = None
    district_ids: list[int] | None = None
    geo_indicator_ids: list[int] | None = None
    sort_by: (
        Literal["name", "facility_type", "settlements", "districts", "geo_indicators"]
        | None
    ) = None
