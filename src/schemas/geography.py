from typing import Literal

from pydantic import BaseModel, ConfigDict

from src.schemas.base_filter import BaseReferenceFilter

from .company import CompanySimpleResponse


class CountryCreate(BaseModel):
    name: str


class RegionCreate(BaseModel):
    name: str
    country_id: int


class SettlementCreate(BaseModel):
    name: str
    region_id: int


class DistrictCreate(BaseModel):
    name: str
    settlement_id: int | None = None
    region_id: int
    company_id: int


class CountryUpdate(BaseModel):
    name: str | None = None


class RegionUpdate(BaseModel):
    name: str | None = None
    country_id: int | None = None


class SettlementUpdate(BaseModel):
    name: str | None = None
    region_id: int | None = None


class DistrictUpdate(BaseModel):
    name: str | None = None
    settlement_id: int | None = None
    region_id: int | None = None
    company_id: int | None = None


class RegionSimpleResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str


class SettlementSimpleResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str


class DistrictSimpleResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str


class CountryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str


class RegionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str
    country: CountryResponse


class SettlementResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str
    region: RegionSimpleResponse


class DistrictResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str
    settlement: SettlementSimpleResponse | None
    region: RegionSimpleResponse
    company: CompanySimpleResponse | None


class CountryFilter(BaseReferenceFilter):
    name: str | None = None


class RegionFilter(BaseReferenceFilter):
    name: str | None = None
    countries: str | None = None


class SettlementFilter(BaseReferenceFilter):
    name: str | None = None
    regions: str | None = None


class DistrictFilter(BaseReferenceFilter):
    name: str | None = None
    regions: str | None = None
    settlements: str | None = None
    companies: str | None = None


CountrySortField = Literal["name"]
RegionSortField = Literal["name", "country"]
SettlementSortField = Literal["name", "region"]
DistrictSortField = Literal["name", "region", "settlement", "company"]


class CountryListRequest(BaseReferenceFilter):
    name: str | None = None
    sort_by: CountrySortField | None = None


class RegionListRequest(BaseReferenceFilter):
    name: str | None = None
    country_ids: list[int] | None = None
    sort_by: RegionSortField | None = None


class SettlementListRequest(BaseReferenceFilter):
    name: str | None = None
    region_ids: list[int] | None = None
    sort_by: SettlementSortField | None = None


class DistrictListRequest(BaseReferenceFilter):
    name: str | None = None
    region_ids: list[int] | None = None
    settlement_ids: list[int] | None = None
    company_ids: list[int] | None = None
    sort_by: DistrictSortField | None = None
