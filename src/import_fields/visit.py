from src.import_fields import record_keys as keys
from src.utils.records_resolver import FieldResolverConfig as FC

employee = FC(keys.EMPLOYEE_VISIT_KEY)
group = FC(keys.GROUP_KEY)
month = FC(keys.MONTH_KEY)
year = FC(keys.YEAR_KEY)
doctor = FC(keys.DOCTOR_KEY)
client_type = FC(keys.CLIENT_TYPE_KEY)
institution = FC(keys.INSTITUTION_KEY)

visit_fields = [
    employee.as_required(),
    group.as_required(),
    month.as_required(),
    year.as_required(),
    doctor,
    client_type,
    institution,
]
