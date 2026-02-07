from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from src.schemas.base_filter import BaseReferenceFilter, SortDirection
from src.schemas.company import CompanySimpleResponse
from src.schemas.geography import DistrictSimpleResponse, RegionSimpleResponse
from src.schemas.product import ProductGroupSimpleResponse


class EmployeeCreate(BaseModel):
    full_name: str
    position_id: int
    product_group_id: int
    region_id: int
    district_id: int | None = None
    company_id: int


class PositionCreate(BaseModel):
    name: str


class EmployeeUpdate(BaseModel):
    full_name: str | None = None
    position_id: int | None = None
    product_group_id: int | None = None
    region_id: int | None = None
    district_id: int | None = None
    company_id: int | None = None


class PositionUpdate(BaseModel):
    name: str | None = None


class EmployeeSimpleResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    full_name: str


class PositionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str


class EmployeeResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    full_name: str
    position: PositionResponse
    product_group: ProductGroupSimpleResponse
    region: RegionSimpleResponse
    district: DistrictSimpleResponse | None
    company: CompanySimpleResponse


class EmployeeFilter(BaseReferenceFilter):
    full_name: str | None = None
    positions: str | None = None
    product_groups: str | None = None
    regions: str | None = None
    districts: str | None = None
    companies: str | None = None


class PositionFilter(BaseReferenceFilter):
    name: str | None = None


EmployeeSortField = Literal[
    "full_name",
    "positions",
    "product_groups",
    "regions",
    "districts",
    "companies",
]
PositionSortField = Literal["name"]


class EmployeeListRequest(BaseReferenceFilter):
    full_name: str | None = None
    position_ids: list[int] | None = None
    product_group_ids: list[int] | None = None
    region_ids: list[int] | None = None
    district_ids: list[int] | None = None
    company_ids: list[int] | None = None
    sort_by: EmployeeSortField | None = None


class PositionListRequest(BaseReferenceFilter):
    name: str | None = None
    sort_by: PositionSortField | None = None
