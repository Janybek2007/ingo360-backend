from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, computed_field

from src.schemas.client import (
    DistributorResponse,
    GeoIndicatorResponse,
    PharmacySimpleResponse,
)
from src.schemas.product import SKUSimpleResponse


class PrimarySalesAndStockCreate(BaseModel):
    distributor_id: int
    sku_id: int
    month: int
    quarter: int
    year: int
    indicator: str
    packages: float
    amount: float
    published: bool = False


class SecondarySalesCreate(BaseModel):
    pharmacy_id: int
    sku_id: int
    month: int
    year: int
    indicator: str
    quarter: int
    packages: float
    amount: float
    published: bool = False


class TertiarySalesCreate(BaseModel):
    pharmacy_id: int
    sku_id: int
    month: int
    year: int
    quarter: int
    indicator: str
    packages: float
    amount: float
    published: bool = False


class PrimarySalesAndStockUpdate(BaseModel):
    distributor_id: int | None = None
    sku_id: int | None = None
    month: int | None = None
    quarter: int | None = None
    year: int | None = None
    indicator: str | None = None
    packages: float | None = None
    amount: float | None = None
    published: bool | None = None


class SecondarySalesUpdate(BaseModel):
    pharmacy_id: int | None = None
    sku_id: int | None = None
    month: int | None = None
    year: int | None = None
    indicator: str | None = None
    quarter: int | None = None
    packages: float | None = None
    amount: float | None = None
    published: bool | None = None


class TertiarySalesUpdate(BaseModel):
    pharmacy_id: int | None = None
    sku_id: int | None = None
    month: int | None = None
    year: int | None = None
    quarter: int | None = None
    indicator: str | None = None
    packages: float | None = None
    amount: float | None = None
    published: bool | None = None


class PublishUnpublishedRequest(BaseModel):
    ids: list[int] = Field(default_factory=list)


class PublishUnpublishedItem(BaseModel):
    id: int
    published: bool


class PrimarySalesAndStockFilter(BaseModel):
    limit: int = 100
    offset: int = 0
    distributor_ids: list[int] | None = None
    sku_ids: list[int] | None = None
    months: list[int] | None = None
    quarters: list[int] | None = None
    years: list[int] | None = None
    indicator: str | None = None
    published: bool | None = None


class SecondaryTertiarySalesFilter(BaseModel):
    limit: int = 100
    offset: int = 0
    pharmacy_ids: list[int] | None = None
    sku_ids: list[int] | None = None
    months: list[int] | None = None
    quarters: list[int] | None = None
    years: list[int] | None = None
    indicator: str | None = None
    published: bool | None = None


class PrimarySalesAndStockResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    month: int
    quarter: int
    year: int
    indicator: str
    packages: float
    amount: float
    published: bool = False
    distributor: DistributorResponse
    sku: SKUSimpleResponse


class SecondarySalesResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    pharmacy_id: int
    sku_id: int
    month: int
    year: int
    quarter: int
    indicator: str
    packages: float
    amount: float
    published: bool = False
    pharmacy: PharmacySimpleResponse
    sku: SKUSimpleResponse

    @computed_field
    @property
    def distributor(self) -> DistributorResponse | None:
        return self.pharmacy.distributor


class TertiarySalesResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    pharmacy_id: int
    sku_id: int
    month: int
    year: int
    quarter: int
    indicator: str
    packages: float
    amount: float
    published: bool = False
    pharmacy: PharmacySimpleResponse
    sku: SKUSimpleResponse

    @computed_field
    @property
    def distributor(self) -> DistributorResponse | None:
        return self.pharmacy.distributor


class SalesReportFilter(BaseModel):
    limit: int | None = None
    offset: int = 0
    months: list[int] | None = None
    quarters: list[int] | None = None
    years: list[int] = Field(default_factory=lambda: [date.today().year])
    brand_ids: list[int] | None = None
    distributor_ids: list[int] | None = None
    group_ids: list[int] | None = None
    promo_type_id: int | None = None
    search: str | None = None
    group_by_period: Literal["year", "quarter", "month"] = "month"
    sku_ids: list[int] | None = None


class DistributorShareFilter(SalesReportFilter):
    group_by_dimensions: (
        list[Literal["sku", "brand", "promotion_type", "product_group", "distributor"]]
        | None
    ) = None


class StockCoverageFilter(SalesReportFilter):
    group_by_dimensions: (
        list[Literal["sku", "brand", "promotion_type", "product_group", "distributor"]]
        | None
    ) = None


class ShipmentStockFilter(SalesReportFilter):
    group_by_dimensions: (
        list[Literal["sku", "brand", "promotion_type", "product_group", "distributor"]]
        | None
    ) = None


class SecTerSalesReportFilter(SalesReportFilter):
    geo_indicators_ids: list[int] | None = None
    group_by_dimensions: (
        list[
            Literal[
                "sku",
                "brand",
                "promotion_type",
                "product_group",
                "distributor",
                "geo_indicator",
            ]
        ]
        | None
    ) = None


class PeriodFilter(BaseModel):
    years: list[int] = Field(default_factory=lambda: [date.today().year])
    quarters: list[int] | None = None
    months: list[int] | None = None
    distributor_ids: list[int] | None = None
    brand_ids: list[int] | None = None
    product_group_ids: list[int] | None = None
    sku_ids: list[int] | None = None
    group_by_period: Literal["year", "quarter", "month"] = "month"


class SecTerSalesPeriodFilter(PeriodFilter):
    geo_indicator_ids: list[int] | None = None


class PeriodSalesResponse(BaseModel):
    sales: float
    packages: float


class SalesByDistributorFilter(BaseModel):
    limit: int | None = None
    offset: int = 0
    group_by_period: Literal["year", "quarter", "month"] = "month"
    years: list[int] = Field(default_factory=lambda: [date.today().year])
    months: list[int] | None = None
    quarters: list[int] | None = None
    search: str | None = None
    distributor_ids: list[int] | None = None
    brand_ids: list[int] | None = None
    product_group_ids: list[int] | None = None
    geo_indicator_ids: list[int] | None = None
    group_by_dimensions: (
        list[Literal["distributor", "brand", "product_group"]] | None
    ) = None


class ChartSalesByDistributorFilter(BaseModel):
    group_by_period: Literal["year", "quarter", "month"] = "month"
    years: list[int] = Field(default_factory=lambda: [date.today().year])
    months: list[int] | None = None
    quarters: list[int] | None = None
    search: str | None = None
    distributor_ids: list[int] | None = None
    brand_ids: list[int] | None = None
    product_group_ids: list[int] | None = None
    geo_indicator_ids: list[int] | None = None


class NumericDistributionFilter(BaseModel):
    limit: int | None = None
    offset: int = 0
    months: list[int] | None = None
    quarters: list[int] | None = None
    years: list[int] = Field(default_factory=lambda: [date.today().year])
    brand_ids: list[int] | None = None
    segment_ids: list[int] | None = None
    product_group_ids: list[int] | None = None
    distributor_ids: list[int] | None = None
    geo_indicator_ids: list[int] | None = None
    sku_ids: list[int] | None = None
    search: str | None = None
    group_by_period: Literal["year", "quarter", "month"] = "month"
    group_by_dimensions: (
        list[
            Literal[
                "sku",
                "brand",
                "product_group",
                "segment",
                "distributor",
                "geo_indicator",
            ]
        ]
        | None
    ) = None


class SalesReportResponse(BaseModel):
    sku_id: int
    sku_name: str
    brand_id: int
    brand_name: str
    promotion_type_id: int
    promotion_type_name: str
    distributor_id: int
    distributor_name: str
    product_group_id: int
    product_group_name: str
    periods_data: dict[str, dict[str, float]]


class SecTerSalesReportResponse(SalesReportResponse):
    geo_indicator_id: int | None
    geo_indicator_name: str | None
    distributor_id: int | None
    distributor_name: str | None


class SalesByDistributorResponse(BaseModel):
    distributor_id: int
    distributor_name: str
    periods_data: dict[str, dict[str, float]]
    brand_id: int
    brand_name: str
    product_group_id: int
    product_group_name: str


class NumericDistributionResponse(BaseModel):
    sku_id: int
    sku_name: str
    brand_id: int
    brand_name: str
    product_group_id: int
    product_group_name: str
    segment_id: int
    segment_name: str
    periods_data: dict[str, dict[str, int | float]]
    segment_name: str


class LowStockLevelFilter(BaseModel):
    limit: int | None = None
    offset: int = 0
    sku_ids: list[int] | None = None
    brand_ids: list[int] | None = None
    product_group_ids: list[int] | None = None
    responsible_employee_ids: list[int] | None = None
    pharmacy_ids: list[int] | None = None
    search: str | None = None
    years: list[int] = Field(default_factory=lambda: [date.today().year])
    months: list[int] | None = None
    quarters: list[int] | None = None
    group_by_period: Literal["year", "quarter", "month"] = "month"
    group_by_dimensions: (
        list[
            Literal["pharmacy", "product_group", "responsible_employee", "sku", "brand"]
        ]
        | None
    ) = None


class LowStockLevelResponse(BaseModel):
    pharmacy_id: int
    pharmacy_name: str
    product_group_id: int
    product_group_name: str
    responsible_employee_id: int | None
    responsible_employee_name: str | None
    sku_id: int
    sku_name: str
    brand_id: int
    brand_name: str
    periods_data: dict[str, dict[str, float]]
