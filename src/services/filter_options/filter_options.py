from sqlalchemy import distinct, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.models import (
    SKU,
    Brand,
    Company,
    Distributor,
    Doctor,
    Employee,
    GeoIndicator,
    MedicalFacility,
    Pharmacy,
    Position,
    PrimarySalesAndStock,
    ProductGroup,
    PromotionType,
    SecondarySales,
    Segment,
    Speciality,
    TertiarySalesAndStock,
    User,
    Visit,
)
from src.schemas.filter_options import FilterOption, ReferencesType, ScopeType

from .config import (
    DEFAULT_COMPANY_FILTER_REFS,
    FILTER_KEY_TO_MODEL,
    IMS_REFERENCE_CONFIG,
    REFERENCE_ALIASES,
    REFERENCE_CONFIG,
)


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
            FilterOption.model_construct(
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


def apply_scope_filter(
    stmt,
    target_ref: ReferencesType,
    scope_ref: ScopeType,
    prefetched_sku_ids: list[int] | None = None,
):
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
        return apply_sales_scope(stmt, target_ref, scope_ref, prefetched_sku_ids)

    if scope_ref in ("clients_pharmacies", "clients_doctors"):
        return apply_visits_scope(stmt, target_ref)

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


def apply_sales_scope(
    stmt,
    target_ref: ReferencesType,
    scope_ref: ScopeType,
    prefetched_sku_ids: list[int] | None = None,
):
    if target_ref == "clients_geo_indicators":
        if scope_ref == "sales_secondary":
            pharmacy_ids = select(distinct(SecondarySales.pharmacy_id))
        elif scope_ref == "sales_tertiary":
            pharmacy_ids = select(distinct(TertiarySalesAndStock.pharmacy_id))
        else:
            return stmt

        geo_indicator_ids = select(distinct(Pharmacy.geo_indicator_id)).where(
            Pharmacy.id.in_(pharmacy_ids),
            Pharmacy.geo_indicator_id.is_not(None),
        )
        return stmt.where(GeoIndicator.id.in_(geo_indicator_ids))

    if target_ref == "clients_distributors":
        if scope_ref == "sales_primary":
            distributor_ids = select(distinct(PrimarySalesAndStock.distributor_id))
            return stmt.where(Distributor.id.in_(distributor_ids))

        if scope_ref == "sales_tertiary":
            distributor_ids = select(
                distinct(TertiarySalesAndStock.distributor_id)
            ).where(TertiarySalesAndStock.distributor_id.is_not(None))
            return stmt.where(Distributor.id.in_(distributor_ids))

        if scope_ref == "sales_secondary":
            distributor_ids = select(distinct(SecondarySales.distributor_id))
            return stmt.where(Distributor.id.in_(distributor_ids))

    if target_ref in (
        "products_skus",
        "products_brands",
        "products_product_groups",
        "products_promotion_types",
        "products_segments",
    ):
        if prefetched_sku_ids is not None:
            sku_filter = SKU.id.in_(prefetched_sku_ids)
        else:
            if scope_ref == "sales_primary":
                sku_ids_sq = select(distinct(PrimarySalesAndStock.sku_id)).subquery()
            elif scope_ref == "sales_secondary":
                sku_ids_sq = select(distinct(SecondarySales.sku_id)).subquery()
            else:
                sku_ids_sq = select(distinct(TertiarySalesAndStock.sku_id)).subquery()
            sku_filter = SKU.id.in_(select(sku_ids_sq))

        if target_ref == "products_skus":
            return stmt.where(sku_filter)

        if target_ref == "products_brands":
            ids = select(distinct(SKU.brand_id)).where(sku_filter)
            return stmt.where(Brand.id.in_(ids))

        if target_ref == "products_product_groups":
            ids = select(distinct(SKU.product_group_id)).where(sku_filter)
            return stmt.where(ProductGroup.id.in_(ids))

        if target_ref == "products_promotion_types":
            ids = select(distinct(SKU.promotion_type_id)).where(
                sku_filter, SKU.promotion_type_id.is_not(None)
            )
            return stmt.where(PromotionType.id.in_(ids))

        if target_ref == "products_segments":
            ids = select(distinct(SKU.segment_id)).where(
                sku_filter, SKU.segment_id.is_not(None)
            )
            return stmt.where(Segment.id.in_(ids))

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

    if target_ref == "clients_geo_indicators":
        pharmacy_ids = select(distinct(Visit.pharmacy_id)).where(
            Visit.pharmacy_id.is_not(None)
        )
        geo_indicator_ids = select(distinct(Pharmacy.geo_indicator_id)).where(
            Pharmacy.id.in_(pharmacy_ids),
            Pharmacy.geo_indicator_id.is_not(None),
        )
        return stmt.where(GeoIndicator.id.in_(geo_indicator_ids))

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

    if target_ref == "clients_specialities":
        doctor_ids = select(distinct(Visit.doctor_id)).where(
            Visit.doctor_id.is_not(None)
        )
        speciality_ids = select(distinct(Doctor.speciality_id)).where(
            Doctor.id.in_(doctor_ids),
            Doctor.speciality_id.is_not(None),
        )
        return stmt.where(Speciality.id.in_(speciality_ids))

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
    if reference in IMS_REFERENCE_CONFIG:
        ims_col = IMS_REFERENCE_CONFIG[reference]
        stmt = (
            select(
                distinct(ims_col).label("id"),
                ims_col.label("name"),
            )
            .where(ims_col.is_not(None))
            .order_by(ims_col)
        )
        return stmt

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


def apply_dynamic_filters(
    stmt,
    reference: ReferencesType,
    filters: dict[str, list[int]],
):
    """
    Динамически применяет фильтры к stmt.

    Логика для каждого filter_key:
      1. Прямое поле на target_model → WHERE col IN values
      2. FK target → filter_model → WHERE fk_col IN values
      3. FK filter_model → target → WHERE target.id IN (
             SELECT DISTINCT fk_col FROM filter_model WHERE id IN values
         )
      Если связь не найдена - фильтр молча пропускается.
    """
    target_model, _ = REFERENCE_CONFIG[reference]

    for filter_key, filter_values in filters.items():
        if not filter_values:
            continue

        # ── 1. Прямая колонка на target_model (напр. "brand_id") ──────────
        direct_col = target_model.__table__.c.get(filter_key)
        if direct_col is not None:
            stmt = stmt.where(direct_col.in_(filter_values))
            continue

        # ── Резолвим filter_key в (filter_model, filter_id_col) ───────────
        filter_info = FILTER_KEY_TO_MODEL.get(filter_key)
        if filter_info is None:
            # Неизвестный ключ - пропускаем
            continue

        filter_model, filter_id_col = filter_info

        # ── 2. Та же модель ───────────────────────────────────────────────
        if filter_model is target_model:
            stmt = stmt.where(target_model.id.in_(filter_values))
            continue

        # ── 3. FK: target_model → filter_model ────────────────────────────
        #    Напр. SKU.brand_id → Brand  =>  WHERE SKU.brand_id IN values
        fk_on_target = find_fk_column(target_model, filter_model)
        if fk_on_target is not None:
            stmt = stmt.where(fk_on_target.in_(filter_values))
            continue

        # ── 4. FK: filter_model → target_model ────────────────────────────
        #    Напр. SKU.brand_id → Brand  (target=Brand, filter=SKU)
        #    => WHERE Brand.id IN (SELECT DISTINCT SKU.brand_id
        #                          FROM SKU WHERE SKU.id IN values)
        fk_on_filter = find_fk_column(filter_model, target_model)
        if fk_on_filter is not None:
            subquery = select(distinct(fk_on_filter)).where(
                filter_id_col.in_(filter_values),
                fk_on_filter.is_not(None),
            )
            stmt = stmt.where(target_model.id.in_(subquery))
            continue

    return stmt


async def get_grouped_filter_options(
    session: AsyncSession,
    include_values: list[ReferencesType],
    scope: ScopeType | None,
    company_id: int | None,
    filters: dict[str, list[int]] | None = None,
) -> dict[str, list[FilterOption]]:
    payload: dict[str, list[FilterOption]] = {}

    scope_ref = scope or "all"

    _product_refs = {
        "products_skus",
        "products_brands",
        "products_product_groups",
        "products_promotion_types",
        "products_segments",
    }
    prefetched_sku_ids: list[int] | None = None
    if scope_ref in ("sales_primary", "sales_secondary", "sales_tertiary") and any(
        k in _product_refs for k in include_values
    ):
        if scope_ref == "sales_primary":
            sku_stmt = select(distinct(PrimarySalesAndStock.sku_id))
        elif scope_ref == "sales_secondary":
            sku_stmt = select(distinct(SecondarySales.sku_id))
        else:
            sku_stmt = select(distinct(TertiarySalesAndStock.sku_id))
        sku_result = await session.execute(sku_stmt)
        prefetched_sku_ids = sku_result.scalars().all()

    for key in include_values:
        stmt = build_reference_stmt(key, company_id)

        if key not in IMS_REFERENCE_CONFIG:
            stmt = apply_scope_filter(stmt, key, scope_ref, prefetched_sku_ids)

        if filters and key not in IMS_REFERENCE_CONFIG:
            stmt = apply_dynamic_filters(stmt, key, filters)

        rows_result = await session.execute(stmt)
        rows = rows_result.mappings().all()
        payload[REFERENCE_ALIASES[key]] = normalize_options(rows)

    return payload
