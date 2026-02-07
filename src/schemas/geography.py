from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from src.schemas.base_filter import BaseFilter

from .base_filter import BaseFilter, SortDirection
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


class CountryFilter(BaseFilter):
    name: str | None = None


class RegionFilter(BaseFilter):
    name: str | None = None
    countries: str | None = None


class SettlementFilter(BaseFilter):
    name: str | None = None
    regions: str | None = None


class DistrictFilter(BaseFilter):
    name: str | None = None
    regions: str | None = None
    settlements: str | None = None
    companies: str | None = None


CountrySortField = Literal["name"]
RegionSortField = Literal["name", "country"]
SettlementSortField = Literal["name", "region"]
DistrictSortField = Literal["name", "region", "settlement", "company"]


class CountryListRequest(BaseFilter):
    name: str | None = None
    sort_by: CountrySortField | None = None


class RegionListRequest(BaseFilter):
    name: str | None = None
    country_ids: list[int] | None = None
    sort_by: RegionSortField | None = None


class SettlementListRequest(BaseFilter):
    name: str | None = None
    region_ids: list[int] | None = None
    sort_by: SettlementSortField | None = None


class DistrictListRequest(BaseFilter):
    name: str | None = None
    region_ids: list[int] | None = None
    settlement_ids: list[int] | None = None
    company_ids: list[int] | None = None
    sort_by: DistrictSortField | None = None
