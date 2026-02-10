from src.db.models.clients import Distributor, GeoIndicator
from src.db.models.employees import Employee
from src.db.models.products import SKU, Brand, ProductGroup, PromotionType, Segment

BASE_SALE_DIMENSTION_MAPPING = {
    "sku": {
        "id": SKU.id.label("sku_id"),
        "name": SKU.name.label("sku_name"),
        "group_fields": [SKU.id, SKU.name],
        "search": SKU.name,
    },
    "brand": {
        "id": Brand.id.label("brand_id"),
        "name": Brand.name.label("brand_name"),
        "group_fields": [Brand.id, Brand.name],
        "search": Brand.name,
    },
    "promotion_type": {
        "id": PromotionType.id.label("promotion_type_id"),
        "name": PromotionType.name.label("promotion_type_name"),
        "group_fields": [PromotionType.id, PromotionType.name],
        "search": PromotionType.name,
    },
    "product_group": {
        "id": ProductGroup.id.label("product_group_id"),
        "name": ProductGroup.name.label("product_group_name"),
        "group_fields": [ProductGroup.id, ProductGroup.name],
        "search": ProductGroup.name,
    },
    "distributor": {
        "id": Distributor.id.label("distributor_id"),
        "name": Distributor.name.label("distributor_name"),
        "group_fields": [Distributor.id, Distributor.name],
        "search": Distributor.name,
    },
}

BASE_SALE_DIMENSTION_MAPPING_WITH_GEO_INDICATOR = {
    **BASE_SALE_DIMENSTION_MAPPING,
    "geo_indicator": {
        "id": GeoIndicator.id.label("geo_indicator_id"),
        "name": GeoIndicator.name.label("geo_indicator_name"),
        "group_fields": [GeoIndicator.id, GeoIndicator.name],
        "search": GeoIndicator.name,
    },
}

BASE_SALE_DIMENSTION_MAPPING_WITH_GEO_INDICATOR_AND_SEGMENT = {
    **BASE_SALE_DIMENSTION_MAPPING,
    "geo_indicator": {
        "id": GeoIndicator.id.label("geo_indicator_id"),
        "name": GeoIndicator.name.label("geo_indicator_name"),
        "group_fields": [GeoIndicator.id, GeoIndicator.name],
        "search": GeoIndicator.name,
    },
    "segment": {
        "id": Segment.id.label("segment_id"),
        "name": Segment.name.label("segment_name"),
        "group_fields": [Segment.id, Segment.name],
        "search": Segment.name,
    },
}


LOW_STOCK_DIMENSTIONS_MAPPING = {
    "product_group": {
        "id": ProductGroup.id.label("product_group_id"),
        "name": ProductGroup.name.label("product_group_name"),
        "group_fields": [ProductGroup.id, ProductGroup.name],
    },
    "responsible_employee": {
        "id": Employee.id.label("responsible_employee_id"),
        "name": Employee.full_name.label("responsible_employee_name"),
        "group_fields": [Employee.id, Employee.full_name],
    },
    "sku": {
        "id": SKU.id.label("sku_id"),
        "name": SKU.name.label("sku_name"),
        "group_fields": [SKU.id, SKU.name],
    },
    "brand": {
        "id": Brand.id.label("brand_id"),
        "name": Brand.name.label("brand_name"),
        "group_fields": [Brand.id, Brand.name],
    },
}
