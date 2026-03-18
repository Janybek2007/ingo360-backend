from src.import_fields import record_keys as keys
from src.utils.records_resolver import FieldResolverConfig as FC

distributor = FC(keys.DISTRIBUTOR_KEY)
sku = FC(keys.SKU_KEY)
month = FC(keys.MONTH_KEY)
year = FC(keys.YEAR_KEY)
indicator = FC(keys.INDICATOR_KEY)
pharmacy = FC(keys.PHARMACY_KEY)
packages = FC(keys.PACKAGES_KEY)
amount = FC(keys.AMOUNT_KEY)

primary_sales_fields = [
    distributor.as_required(),
    sku.as_required(),
    month.as_required(),
    year.as_required(),
    indicator.as_required(),
    packages,
    amount,
]

secondary_sales_fields = [
    pharmacy.as_required(),
    sku.as_required(),
    month.as_required(),
    year.as_required(),
    indicator.as_required(),
    distributor.as_required(),
    packages,
    amount,
]

tertiary_sales_fields = [
    pharmacy.as_required(),
    sku.as_required(),
    month.as_required(),
    year.as_required(),
    indicator.as_required(),
    packages,
    amount,
]
