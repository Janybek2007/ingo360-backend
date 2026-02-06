from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import distinct, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.dependencies.current_user import current_active_user
from src.db.models import (
    SKU,
    Brand,
    ClientCategory,
    Company,
    Country,
    Distributor,
    District,
    Doctor,
    Dosage,
    DosageForm,
    Employee,
    GeoIndicator,
    MedicalFacility,
    Pharmacy,
    Position,
    ProductGroup,
    PromotionType,
    Region,
    Segment,
    Settlement,
    Speciality,
    User,
)
from src.db.session import db_session
from src.services.ims import ims_service

router = APIRouter()


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
    scopes: dict[ReferencesType, Literal["all"] | ReferencesType] | None = None


REFERENCE_ALIASES = {
    "geography_countries": "geography_countries",
    "geography_settlements": "geography_settlements",
    "geography_regions": "geography_regions",
    "geography_districts": "geography_districts",
    "products_product_groups": "products_product_groups",
    "products_promotion_types": "products_promotion_types",
    "products_brands": "products_brands",
    "products_dosages": "products_dosages",
    "products_dosage_forms": "products_dosage_forms",
    "products_segments": "products_segments",
    "products_skus": "products_skus",
    "employees_positions": "employees_positions",
    "employees_employees": "employees_employees",
    "clients_distributors": "clients_distributors",
    "clients_geo_indicators": "clients_geo_indicators",
    "clients_medical_facilities": "clients_medical_facilities",
    "clients_specialities": "clients_specialities",
    "clients_client_categories": "clients_client_categories",
    "clients_doctors": "clients_doctors",
    "clients_pharmacies": "clients_pharmacies",
    "companies_companies": "companies_companies",
}

ALLOWED_REFERENCES = set(REFERENCE_ALIASES.keys())

REFERENCE_CONFIG = {
    "geography_countries": (Country, Country.name),
    "geography_settlements": (Settlement, Settlement.name),
    "geography_regions": (Region, Region.name),
    "geography_districts": (District, District.name),
    "products_product_groups": (ProductGroup, ProductGroup.name),
    "products_promotion_types": (PromotionType, PromotionType.name),
    "products_brands": (Brand, Brand.name),
    "products_dosages": (Dosage, Dosage.name),
    "products_dosage_forms": (DosageForm, DosageForm.name),
    "products_segments": (Segment, Segment.name),
    "products_skus": (SKU, SKU.name),
    "employees_positions": (Position, Position.name),
    "employees_employees": (Employee, Employee.full_name),
    "clients_distributors": (Distributor, Distributor.name),
    "clients_geo_indicators": (GeoIndicator, GeoIndicator.name),
    "clients_medical_facilities": (MedicalFacility, MedicalFacility.name),
    "clients_specialities": (Speciality, Speciality.name),
    "clients_client_categories": (ClientCategory, ClientCategory.name),
    "clients_doctors": (Doctor, Doctor.full_name),
    "clients_pharmacies": (Pharmacy, Pharmacy.name),
    "companies_companies": (Company, Company.name),
}

DEFAULT_COMPANY_FILTER_REFS = {
    "geography_districts",
    "products_product_groups",
    "products_brands",
    "products_skus",
}


def _normalize_options(rows, value_field: str = "name") -> list[FilterOption]:
    result: list[FilterOption] = []
    for row in rows:
        row_mapping = dict(row)
        value = row_mapping.get(value_field)
        if value is None:
            value = row_mapping.get("name")
        if value is None:
            continue
        result.append(FilterOption(id=row_mapping["id"], name=str(value)))
    return result


def _find_fk_column(from_model, to_model):
    for column in from_model.__table__.columns:
        for fk in column.foreign_keys:
            if fk.column.table.name == to_model.__tablename__:
                return column
    return None


def _apply_scope_filter(stmt, target_ref: ReferencesType, scope_ref: ReferencesType):
    if scope_ref == "all" or target_ref == scope_ref:
        return stmt

    target_model, _ = REFERENCE_CONFIG[target_ref]
    scope_model, _ = REFERENCE_CONFIG[scope_ref]

    target_to_scope_fk = _find_fk_column(target_model, scope_model)
    if target_to_scope_fk is not None:
        return stmt.where(target_to_scope_fk.is_not(None))

    scope_to_target_fk = _find_fk_column(scope_model, target_model)
    if scope_to_target_fk is not None:
        scope_subquery = select(distinct(scope_to_target_fk)).where(
            scope_to_target_fk.is_not(None)
        )
        return stmt.where(target_model.id.in_(scope_subquery))

    target_company_id = target_model.__table__.c.get("company_id")
    scope_company_id = scope_model.__table__.c.get("company_id")

    if target_model is Company and scope_company_id is not None:
        scope_subquery = select(distinct(scope_company_id)).where(
            scope_company_id.is_not(None)
        )
        return stmt.where(Company.id.in_(scope_subquery))

    if target_company_id is not None and scope_model is Company:
        return stmt

    if target_company_id is not None and scope_company_id is not None:
        scope_subquery = select(distinct(scope_company_id)).where(
            scope_company_id.is_not(None)
        )
        return stmt.where(target_company_id.in_(scope_subquery))

    return stmt


def _build_reference_stmt(reference: ReferencesType, company_id: int):
    model, label_column = REFERENCE_CONFIG[reference]
    stmt = select(distinct(model.id).label("id"), label_column.label("name"))

    if reference in DEFAULT_COMPANY_FILTER_REFS and company_id:
        stmt = stmt.where(model.company_id == company_id)

    return stmt


@router.post(
    "/filter-options/grouped",
    response_model=GroupedFilterOptionsResponse,
    dependencies=[Depends(current_active_user)],
)
async def get_grouped_filter_options(
    body: GroupedFilterOptionsRequest,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_active_user)],
):
    include_values = body.references

    invalid = [item for item in include_values if item not in ALLOWED_REFERENCES]
    if invalid:
        raise HTTPException(
            status_code=422,
            detail={"invalid_include": invalid, "allowed": sorted(ALLOWED_REFERENCES)},
        )

    scopes = {reference: "all" for reference in include_values}
    scopes.update(body.scopes or {})

    invalid_scope_keys = [key for key in scopes if key not in ALLOWED_REFERENCES]
    invalid_scope_values = [
        value
        for value in scopes.values()
        if value != "all" and value not in ALLOWED_REFERENCES
    ]
    scope_keys_not_in_references = [key for key in scopes if key not in include_values]
    if invalid_scope_keys or invalid_scope_values or scope_keys_not_in_references:
        raise HTTPException(
            status_code=422,
            detail={
                "invalid_scope_keys": invalid_scope_keys,
                "invalid_scope_values": invalid_scope_values,
                "scope_keys_not_in_references": scope_keys_not_in_references,
                "allowed": sorted(ALLOWED_REFERENCES),
            },
        )

    payload: dict[str, list[FilterOption]] = {}

    for key in include_values:
        stmt = _build_reference_stmt(key, current_user.company_id)
        stmt = _apply_scope_filter(stmt, key, scopes.get(key, "all"))

        rows_result = await session.execute(stmt)
        rows = rows_result.mappings().all()
        payload[REFERENCE_ALIASES[key]] = _normalize_options(rows)

    return GroupedFilterOptionsResponse(**payload)


# IMS
@router.get("/filter-options/company-name", dependencies=[Depends(current_active_user)])
async def get_company_names(
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
):
    return await ims_service.get_field(session, "company")


@router.get("/filter-options/brand-name", dependencies=[Depends(current_active_user)])
async def get_brand_names(
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
):
    return await ims_service.get_field(session, "brand")


@router.get("/filter-options/segment-name", dependencies=[Depends(current_active_user)])
async def get_segment_names(
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
):
    return await ims_service.get_field(session, "segment")


@router.get(
    "/filter-options/dosage-form-name", dependencies=[Depends(current_active_user)]
)
async def get_dosage_form_names(
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
):
    return await ims_service.get_field(session, "dosage_form")
