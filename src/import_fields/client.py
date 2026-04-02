from src.import_fields.base import (
    access_mode,
    category,
    company,
    distributor,
    employee,
    fio,
    geo_indicator,
    group,
    lpu,
    name,
    region,
    settlement,
    speciality,
)

distributor_fields = [name]
client_category_fields = [name]
geo_indicator_fields = [name]
speciality_fields = [name]

doctor_fields = [
    fio.as_required(),
    lpu.as_required(),
    speciality.as_required(),
    company,
    group,
    category,
    employee,
    access_mode,
]

pharmacy_fields = [
    name.as_required(),
    company.as_required(),
    group.as_required(),
    category,
    distributor,
    employee,
    geo_indicator,
]

medical_facility_fields = [
    name.as_required(),
    company.as_required(),
    region.as_required(),
    settlement.as_required(),
    geo_indicator,
]
