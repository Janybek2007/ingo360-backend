from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from src.schemas.base_filter import BaseDbFilter, SortDirection
from src.schemas.client import (
    DoctorSimpleResponse,
    MedicalFacilitySimpleResponse,
    PharmacySimpleResponse,
)
from src.schemas.employee import EmployeeSimpleResponse
from src.schemas.product import ProductGroupSimpleResponse


class VisitCreate(BaseModel):
    product_group_id: int
    employee_id: int | None = None
    client_type: str
    month: int
    year: int
    doctor_id: int | None = None
    medical_facility_id: int | None = None
    pharmacy_id: int | None = None


class VisitUpdate(BaseModel):
    product_group_id: int | None = None
    employee_id: int | None = None
    client_type: str | None = None
    month: int | None = None
    year: int | None = None
    doctor_id: int | None = None
    medical_facility_id: int | None = None
    pharmacy_id: int | None = None


class VisitsRequest(BaseDbFilter):
    pharmacy_ids: list[int] | None = None
    employee_ids: list[int] | None = None
    product_group_ids: list[int] | None = None
    medical_facility_ids: list[int] | None = None
    doctor_ids: list[int] | None = None
    client_type: str | None = None
    months: list[int] | None = None
    year: str | None = None
    sort_by: (
        Literal[
            "pharmacy",
            "employee",
            "product_group",
            "medical_facility",
            "doctor",
            "client_type",
            "month",
            "year",
        ]
        | None
    ) = None


class VisitResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    client_type: str
    month: int
    year: int
    product_group: ProductGroupSimpleResponse
    employee: EmployeeSimpleResponse | None
    doctor: DoctorSimpleResponse | None
    medical_facility: MedicalFacilitySimpleResponse | None
    pharmacy: PharmacySimpleResponse | None


class DoctorsBySpecialtyResponse(BaseModel):
    speciality_id: int
    speciality_name: str
    total_count: int
    count_with_visits: int
    coverage_percentage: float


class DoctorsBySpecialtyWithVisitResponse(BaseModel):
    speciality_id: int
    speciality_name: str
    medical_facility_id: int
    medical_facility_name: str
    coverage_percentage: float
    doctors_with_visits: int
    total_doctors: int


class DoctorsCountFilter(BaseModel):
    medical_facility_ids: list[int] | None = None
    speciality_ids: list[int] | None = None
    search: str | None = None
    period_values: list[str] | None = None
    group_by_period: Literal["month", "quarter", "year"] = "month"
    limit: int | None = None
    offset: int = 0
    # group_by_period: Literal["month", "quarter", "year"] = "month"


class DoctorsCountWithVisitFilter(BaseModel):
    medical_facility_ids: list[int] | None = None
    speciality_ids: list[int] | None = None
    doctor_ids: list[int] | None = None
    search: str | None = None
    period_values: list[str] | None = None
    months: list[int] = Field(default_factory=lambda: [date.today().month])
    years: list[int] = Field(default_factory=lambda: [date.today().year])
    group_by_period: Literal["month", "quarter", "year"] = "month"
    limit: int | None = None
    offset: int = 0
    group_by_dimensions: list[Literal["medical_facility", "speciality", "doctor"]] = (
        Field(default_factory=lambda: ["medical_facility", "speciality", "doctor"])
    )
    sort_by: (
        Literal[
            "medical_facility",
            "doctor",
            "speciality",
            "total_doctors",
            "doctors_with_visits",
        ]
        | None
    ) = None
    sort_order: SortDirection | None = None


class VisitCountFilter(BaseModel):
    limit: int | None = None
    offset: int = 0
    pharmacy_ids: list[int] | None = None
    product_group_ids: list[int] | None = None
    medical_facility_ids: list[int] | None = None
    employee_ids: list[int] | None = None
    search: str | None = None
    period_values: list[str] | None = None
    geo_indicator_ids: list[int] | None = None
    speciality_ids: list[int] | None = None
    group_by_period: Literal["month", "quarter", "year"] = "month"


class VisitSumForPeriodFilter(BaseModel):
    limit: int | None = None
    offset: int = 0
    pharmacy_ids: list[int] | None = None
    product_group_ids: list[int] | None = None
    medical_facility_ids: list[int] | None = None
    employee_ids: list[int] | None = None
    search: str | None = None
    period_values: list[str] | None = None
    months: list[int] | None = None
    years: list[int] | None = None
    group_by_period: Literal["month", "quarter", "year"] = "month"
    geo_indicator_ids: list[int] | None = Field(default=None, alias="indicator_ids")
    speciality_ids: list[int] | None = None
    group_by_dimensions: list[
        Literal[
            "pharmacy",
            "medical_facility",
            "year",
            "month",
            "employee",
            "product_group",
            "geo_indicator",
            "speciality",
            "doctor",
        ]
    ] = Field(
        default_factory=lambda: [
            "pharmacy",
            "medical_facility",
            "year",
            "month",
            "employee",
            "product_group",
            "geo_indicator",
            "speciality",
            "doctor",
        ]
    )
    sort_by: (
        Literal[
            "medical_facility",
            "pharmacy",
            "employee",
            "group",
            "employee_visits",
            "geo_indicator",
        ]
        | None
    ) = None
    sort_order: SortDirection | None = None


class VisitCountPeriodResponse(BaseModel):
    pharmacy_id: int | None
    pharmacy: str | None
    medical_facility_id: int | None
    medical_facility: str | None
    year: int
    month: int
    employee_id: int
    employee: str
    product_group_id: int
    product_group: str
    employee_visits: int
    indicator_id: int | None
    indicator_name: str | None
    speciality_id: int | None
    speciality_name: str | None
    doctor_name: str | None
