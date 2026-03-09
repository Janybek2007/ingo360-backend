from sqlalchemy import distinct, func, select
from sqlalchemy.ext.asyncio import AsyncSession

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
    PrimarySalesAndStock,
    ProductGroup,
    PromotionType,
    Region,
    SecondarySales,
    Segment,
    Settlement,
    Speciality,
    TertiarySalesAndStock,
    User,
    Visit,
)
from src.schemas.filter_options import FilterOption, ReferencesType, ScopeType

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
ALLOWED_SCOPES = {
    "all",
    "clients_clients",
    "sales_primary",
    "sales_secondary",
    "sales_tertiary",
    "visits",
}
ALLOWED_SCOPES.update(ALLOWED_REFERENCES)

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


def normalize_options(rows, value_field: str = "name") -> list[FilterOption]:
    result: list[FilterOption] = []
    for row in rows:
        row_mapping = dict(row)
        value = row_mapping.get(value_field)
        if value is None:
            value = row_mapping.get("name")
        if value is None:
            continue
        product_group_ids = row_mapping.get("product_group_ids")
        scope_values = (
            {"product_group_ids": product_group_ids} if product_group_ids else None
        )
        result.append(
            FilterOption(
                id=row_mapping["id"], name=str(value), scope_values=scope_values
            )
        )
    return result


def find_fk_column(from_model, to_model):
    for column in from_model.__table__.columns:
        for fk in column.foreign_keys:
            if fk.column.table.name == to_model.__tablename__:
                return column
    return None


def apply_scope_filter(stmt, target_ref: ReferencesType, scope_ref: ScopeType):
    if scope_ref == "all" or target_ref == scope_ref:
        return stmt

    if scope_ref == "clients_clients" and target_ref == "companies_companies":
        company_ids = (
            select(distinct(User.company_id))
            .where(
                User.company_id.is_not(None),
                ~User.is_admin,
                ~User.is_operator,
                ~User.is_superuser,
            )
            .scalar_subquery()
        )
        return stmt.where(Company.id.in_(company_ids))

    if scope_ref in ("sales_primary", "sales_secondary", "sales_tertiary"):
        return apply_sales_scope(stmt, target_ref, scope_ref)

    if scope_ref == "visits":
        return apply_visits_scope(stmt, target_ref)

    target_model, _ = REFERENCE_CONFIG[target_ref]
    scope_model, _ = REFERENCE_CONFIG[scope_ref]

    target_to_scope_fk = find_fk_column(target_model, scope_model)
    if target_to_scope_fk is not None:
        return stmt.where(target_to_scope_fk.is_not(None))

    scope_to_target_fk = find_fk_column(scope_model, target_model)
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


def apply_sales_scope(stmt, target_ref: ReferencesType, scope_ref: ScopeType):
    if target_ref == "clients_distributors" and scope_ref == "sales_primary":
        distributor_ids = select(distinct(PrimarySalesAndStock.distributor_id))
        return stmt.where(Distributor.id.in_(distributor_ids))

    if target_ref == "products_brands":
        if scope_ref == "sales_primary":
            sku_ids = select(distinct(PrimarySalesAndStock.sku_id))
        elif scope_ref == "sales_secondary":
            sku_ids = select(distinct(SecondarySales.sku_id))
        else:
            sku_ids = select(distinct(TertiarySalesAndStock.sku_id))

        brand_ids = select(distinct(SKU.brand_id)).where(SKU.id.in_(sku_ids))
        return stmt.where(Brand.id.in_(brand_ids))

    if target_ref == "products_product_groups":
        if scope_ref == "sales_primary":
            sku_ids = select(distinct(PrimarySalesAndStock.sku_id))
        elif scope_ref == "sales_secondary":
            sku_ids = select(distinct(SecondarySales.sku_id))
        else:
            sku_ids = select(distinct(TertiarySalesAndStock.sku_id))

        product_group_ids = select(distinct(SKU.product_group_id)).where(
            SKU.id.in_(sku_ids)
        )
        return stmt.where(ProductGroup.id.in_(product_group_ids))

    if target_ref == "clients_pharmacies" and scope_ref in (
        "sales_secondary",
        "sales_tertiary",
    ):
        if scope_ref == "sales_secondary":
            pharmacy_ids = select(distinct(SecondarySales.pharmacy_id))
        else:
            pharmacy_ids = select(distinct(TertiarySalesAndStock.pharmacy_id))
        return stmt.where(Pharmacy.id.in_(pharmacy_ids))

    return stmt


def apply_visits_scope(stmt, target_ref: ReferencesType):
    if target_ref == "clients_pharmacies":
        pharmacy_ids = select(distinct(Visit.pharmacy_id)).where(
            Visit.pharmacy_id.is_not(None)
        )
        return stmt.where(Pharmacy.id.in_(pharmacy_ids))

    if target_ref == "employees_employees":
        employee_ids = select(distinct(Visit.employee_id))
        return stmt.where(Employee.id.in_(employee_ids))

    if target_ref == "employees_positions":
        position_ids = (
            select(distinct(Employee.position_id))
            .where(Employee.id.in_(select(distinct(Visit.employee_id))))
            .scalar_subquery()
        )
        return stmt.where(Position.id.in_(position_ids))

    if target_ref == "products_product_groups":
        group_ids = select(distinct(Visit.product_group_id))
        return stmt.where(ProductGroup.id.in_(group_ids))

    if target_ref == "clients_medical_facilities":
        facility_ids = select(distinct(Visit.medical_facility_id)).where(
            Visit.medical_facility_id.is_not(None)
        )
        return stmt.where(MedicalFacility.id.in_(facility_ids))

    if target_ref == "clients_doctors":
        doctor_ids = select(distinct(Visit.doctor_id)).where(
            Visit.doctor_id.is_not(None)
        )
        return stmt.where(Doctor.id.in_(doctor_ids))

    return stmt


def build_reference_stmt(reference: ReferencesType, company_id: int):
    if reference == "products_brands":
        stmt = (
            select(
                Brand.id.label("id"),
                Brand.name.label("name"),
                func.array_remove(
                    func.array_agg(distinct(SKU.product_group_id)), None
                ).label("product_group_ids"),
            )
            .outerjoin(SKU, SKU.brand_id == Brand.id)
            .group_by(Brand.id, Brand.name)
        )
        if company_id:
            stmt = stmt.where(Brand.company_id == company_id)
        return stmt

    model, label_column = REFERENCE_CONFIG[reference]
    stmt = select(distinct(model.id).label("id"), label_column.label("name"))

    if reference in DEFAULT_COMPANY_FILTER_REFS and company_id:
        stmt = stmt.where(model.company_id == company_id)

    return stmt


async def get_grouped_filter_options(
    session: AsyncSession,
    include_values: list[ReferencesType],
    scope: ScopeType | None,
    company_id: int | None,
) -> dict[str, list[FilterOption]]:
    payload: dict[str, list[FilterOption]] = {}

    scope_ref = scope or "all"

    for key in include_values:
        stmt = build_reference_stmt(key, company_id)
        stmt = apply_scope_filter(stmt, key, scope_ref)

        rows_result = await session.execute(stmt)
        rows = rows_result.mappings().all()
        payload[REFERENCE_ALIASES[key]] = normalize_options(rows)

    return payload
