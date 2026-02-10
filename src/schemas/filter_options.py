from typing import Literal, TypeAlias, Union

from pydantic import BaseModel


class FilterOption(BaseModel):
    id: int
    name: str


ReferencesType = Literal[
    "geography_countries",
    "geography_settlements",
    "geography_regions",
    "geography_districts",
    "products_product_groups",
    "products_promotion_types",
    "products_brands",
    "products_dosages",
    "products_dosage_forms",
    "products_segments",
    "products_skus",
    "employees_positions",
    "employees_employees",
    "clients_distributors",
    "clients_geo_indicators",
    "clients_medical_facilities",
    "clients_specialities",
    "clients_client_categories",
    "clients_doctors",
    "clients_pharmacies",
    "companies_companies",
]

ScopeType: TypeAlias = Union[
    Literal[
        "all",
        "clients_clients",
        "sales_primary",
        "sales_secondary",
        "sales_tertiary",
        "visits",
    ],
    ReferencesType,
]


class GroupedFilterOptionsResponse(BaseModel):
    geography_countries: list[FilterOption] | None = None
    geography_settlements: list[FilterOption] | None = None
    geography_regions: list[FilterOption] | None = None
    geography_districts: list[FilterOption] | None = None
    products_product_groups: list[FilterOption] | None = None
    products_promotion_types: list[FilterOption] | None = None
    products_brands: list[FilterOption] | None = None
    products_dosages: list[FilterOption] | None = None
    products_dosage_forms: list[FilterOption] | None = None
    products_segments: list[FilterOption] | None = None
    products_skus: list[FilterOption] | None = None
    employees_positions: list[FilterOption] | None = None
    employees_employees: list[FilterOption] | None = None
    clients_distributors: list[FilterOption] | None = None
    clients_geo_indicators: list[FilterOption] | None = None
    clients_medical_facilities: list[FilterOption] | None = None
    clients_specialities: list[FilterOption] | None = None
    clients_client_categories: list[FilterOption] | None = None
    clients_doctors: list[FilterOption] | None = None
    clients_pharmacies: list[FilterOption] | None = None
    companies_companies: list[FilterOption] | None = None


class GroupedFilterOptionsRequest(BaseModel):
    references: list[ReferencesType]
    scope: ScopeType | None = None
