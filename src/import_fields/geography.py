from src.import_fields.base import company, country, name, region, settlement

country_fields = [
    name,
]

region_fields = [
    name.as_required(),
    country.as_required(),
]

settlement_fields = [
    name.as_required(),
    region.as_required(),
]

district_fields = [
    name.as_required(),
    company.as_required(),
    region.as_required(),
    settlement,
]
