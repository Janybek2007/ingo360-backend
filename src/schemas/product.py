from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from .base_filter import BaseFilter, SortDirection
from .company import CompanySimpleResponse


class ProductGroupCreate(BaseModel):
    name: str
    company_id: int


class BrandCreate(BaseModel):
    name: str
    ims_name: str | None = None
    promotion_type_id: int
    product_group_id: int
    company_id: int


class PromotionTypeCreate(BaseModel):
    name: str


class DosageFormCreate(BaseModel):
    name: str


class DosageCreate(BaseModel):
    name: str


class SegmentCreate(BaseModel):
    name: str


class SKUCreate(BaseModel):
    name: str
    brand_id: int
    promotion_type_id: int
    product_group_id: int
    dosage_form_id: int
    dosage_id: int | None = None
    segment_id: int | None = None
    company_id: int


class ProductGroupUpdate(BaseModel):
    name: str | None = None
    company_id: int | None = None


class BrandUpdate(BaseModel):
    name: str | None = None
    ims_name: str | None = None
    promotion_type_id: int | None = None
    product_group_id: int | None = None
    company_id: int | None = None


class PromotionTypeUpdate(BaseModel):
    name: str | None = None


class DosageFormUpdate(BaseModel):
    name: str | None = None


class DosageUpdate(BaseModel):
    name: str | None = None


class SegmentUpdate(BaseModel):
    name: str | None = None


class SKUUpdate(BaseModel):
    name: str | None = None
    brand_id: int | None = None
    promotion_type_id: int | None = None
    product_group_id: int | None = None
    dosage_form_id: int | None = None
    dosage_id: int | None = None
    segment_id: int | None = None
    company_id: int | None = None


class BrandSimpleResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str


class SKUSimpleResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str
    brand: BrandSimpleResponse


class ProductGroupSimpleResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str


class ProductGroupResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str
    company: CompanySimpleResponse


class PromotionTypeResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str


class DosageFormResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str


class DosageResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str


class SegmentResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str


class BrandResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str
    ims_name: str | None = None
    promotion_type: PromotionTypeResponse
    product_group: ProductGroupSimpleResponse
    company: CompanySimpleResponse


class SKUResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str
    brand: BrandSimpleResponse
    promotion_type: PromotionTypeResponse
    product_group: ProductGroupSimpleResponse
    dosage_form: DosageFormResponse
    dosage: DosageResponse | None = None
    segment: SegmentResponse | None = None
    company: CompanySimpleResponse | None


class ProductGroupFilter(BaseFilter):
    name: str | None = None
    companies: str | None = None


class BrandFilter(BaseFilter):
    name: str | None = None
    promotion_types: str | None = None
    product_groups: str | None = None
    companies: str | None = None


class PromotionTypeFilter(BaseFilter):
    name: str | None = None


class DosageFormFilter(BaseFilter):
    name: str | None = None


class DosageFilter(BaseFilter):
    name: str | None = None


class SegmentFilter(BaseFilter):
    name: str | None = None


class SKUFilter(BaseFilter):
    name: str | None = None
    brands: str | None = None
    promotion_types: str | None = None
    product_groups: str | None = None
    dosage_forms: str | None = None
    dosages: str | None = None
    segments: str | None = None
    companies: str | None = None


ProductGroupSortField = Literal["name", "companies"]
BrandSortField = Literal["name", "promotion_types", "product_groups", "companies"]
PromotionTypeSortField = Literal["name"]
DosageFormSortField = Literal["name"]
DosageSortField = Literal["name"]
SegmentSortField = Literal["name"]
SKUSortField = Literal[
    "name",
    "brands",
    "promotion_types",
    "product_groups",
    "dosage_forms",
    "dosages",
    "segments",
    "companies",
]


class ProductGroupListRequest(BaseModel):
    filters: ProductGroupFilter = Field(default_factory=ProductGroupFilter)
    sort_by: ProductGroupSortField | None = None
    sort_order: SortDirection | None = None


class BrandListRequest(BaseModel):
    filters: BrandFilter = Field(default_factory=BrandFilter)
    sort_by: BrandSortField | None = None
    sort_order: SortDirection | None = None


class PromotionTypeListRequest(BaseModel):
    filters: PromotionTypeFilter = Field(default_factory=PromotionTypeFilter)
    sort_by: PromotionTypeSortField | None = None
    sort_order: SortDirection | None = None


class DosageFormListRequest(BaseModel):
    filters: DosageFormFilter = Field(default_factory=DosageFormFilter)
    sort_by: DosageFormSortField | None = None
    sort_order: SortDirection | None = None


class DosageListRequest(BaseModel):
    filters: DosageFilter = Field(default_factory=DosageFilter)
    sort_by: DosageSortField | None = None
    sort_order: SortDirection | None = None


class SegmentListRequest(BaseModel):
    filters: SegmentFilter = Field(default_factory=SegmentFilter)
    sort_by: SegmentSortField | None = None
    sort_order: SortDirection | None = None


class SKUListRequest(BaseModel):
    filters: SKUFilter = Field(default_factory=SKUFilter)
    sort_by: SKUSortField | None = None
    sort_order: SortDirection | None = None
