from pydantic import BaseModel, ConfigDict

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
    dosage_id: int
    segment_id: int
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
    dosage: DosageResponse
    segment: SegmentResponse
    company: CompanySimpleResponse | None
