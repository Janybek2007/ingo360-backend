from src.import_fields.base import (
    brand,
    company,
    dosage,
    dosage_form,
    group,
    ims_name,
    name,
    promotion_type,
    segment,
)

dosage_form_fields = [name]
dosage_fields = [name]
promotion_type_fields = [name]
segment_fields = [name]

product_group_fields = [name.as_required(), company.as_required()]

brand_fields = [
    name.as_required(),
    ims_name,
    company.as_required(),
    promotion_type.as_required(),
    group.as_required(),
]

sku_fields = [
    name.as_required(),
    company.as_required(),
    brand.as_required(),
    dosage_form.as_required(),
    promotion_type.as_required(),
    group.as_required(),
    dosage,
    segment,
]
