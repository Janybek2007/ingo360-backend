from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from src.schemas.base_filter import BaseDbFilter, SortDirection


class IMSCreate(BaseModel):
    company: str
    brand: str
    segment: str
    dosage: str
    dosage_form: str
    period: str
    amount: float
    packages: float
    molecule: str


class IMSUpdate(BaseModel):
    company: str | None = None
    brand: str | None = None
    segment: str | None = None
    dosage: str | None = None
    dosage_form: str | None = None
    period: str | None = None
    amount: float | None = None
    packages: float | None = None
    molecule: str | None = None


class IMSResponse(BaseModel):
    id: int
    company: str
    brand: str
    segment: str
    dosage: str
    dosage_form: str
    period: str
    amount: float
    packages: float
    molecule: str

    model_config = ConfigDict(from_attributes=True)


class IMSRequest(BaseDbFilter):
    company: str | None = None
    brand: str | None = None
    segment: str | None = None
    molecule: str | None = None
    dosage: str | None = None
    dosage_form: str | None = None
    period: str | None = None
    amount: str | None = None
    packages: str | None = None
    sort_by: (
        Literal[
            "company",
            "brand",
            "segment",
            "dosage",
            "dosage_form",
            "molecule",
            "period",
            "amount",
            "packages",
        ]
        | None
    ) = None


class IMSTopFilter(BaseModel):
    group_column: Literal["company", "brand", "segment"] = "company"
    period_values: list[str] = Field(
        default_factory=lambda: [datetime.now().strftime("%-m-%y")]
    )
    group_by_period: Literal["month", "quarter", "year", "mat", "ytd"] = "ytd"
    segment_name: str | None = None
    brand_name: str | None = None


class IMSMetricsFilter(BaseModel):
    periods: list[str] = Field(
        default_factory=lambda: [datetime.now().strftime("%-m-%y")]
    )
    group_by_period: Literal["month", "quarter", "year", "mat", "ytd"] = "ytd"


class IMSMetricsResponse(BaseModel):
    sales: float
    market_sales: float
    market_share: float
    growth_vs_previous: float
    market_growth: float
    growth_vs_market: float


class IMSTableFilter(BaseModel):
    company: str | None = None
    brand: str | None = None
    segment: str | None = None
    dosage: str | None = None
    dosage_form: str | None = None
    molecule: str | None = None
    company_names: list[str] | None = None
    brand_names: list[str] | None = None
    segment_names: list[str] | None = None
    dosage_form_names: list[str] | None = None
    search: str | None = None
    period_values: list[str] = Field(
        default_factory=lambda: [datetime.now().strftime("%-m-%y")]
    )
    group_by_period: Literal["month", "quarter", "year", "mat", "ytd"] = "ytd"
    limit: int | None = None
    offset: int = 0
    group_by_dimensions: list[
        Literal["company", "brand", "segment", "dosage_form", "dosage", "molecule"]
    ] = Field(
        default_factory=lambda: [
            "company",
            "brand",
            "segment",
            "dosage_form",
            "dosage",
            "molecule",
        ]
    )
    sort_by: (
        Literal["company", "brand", "segment", "dosage_form", "dosage", "molecule"]
        | None
    ) = None
    sort_order: SortDirection | None = None
