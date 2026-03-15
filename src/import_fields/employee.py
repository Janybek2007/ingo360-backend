from src.import_fields.base import company, district, fio, group, name, position, region

position_fields = [
    name,
]

employee_fields = [
    fio.as_required(),
    company.as_required(),
    region.as_required(),
    position.as_required(),
    group.as_required(),
    district,
]
