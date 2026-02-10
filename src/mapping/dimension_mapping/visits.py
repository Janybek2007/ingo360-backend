from sqlalchemy import or_

from src.db.models.clients import (
    Doctor,
    GeoIndicator,
    MedicalFacility,
    Pharmacy,
    Speciality,
)
from src.db.models.employees import Employee
from src.db.models.products import ProductGroup
from src.db.models.visits import Visit

VISITS_DOCTOR_COUNT_DIMENSTIONS_MAPPING = {
    "medical_facility": {
        "id": MedicalFacility.id.label("medical_facility_id"),
        "name": MedicalFacility.name.label("medical_facility_name"),
        "group_fields": [MedicalFacility.id, MedicalFacility.name],
    },
    "speciality": {
        "id": Speciality.id.label("speciality_id"),
        "name": Speciality.name.label("speciality_name"),
        "group_fields": [Speciality.id, Speciality.name],
    },
    "doctor": {
        "id": Doctor.id.label("doctor_id"),
        "name": Doctor.full_name.label("doctor_name"),
        "group_fields": [Doctor.id, Doctor.full_name],
    },
}

VISITS_SUM_FOR_PERIOD_DIMENSTIONS_MAPPING = {
    "pharmacy": {
        "id_field": Visit.pharmacy_id,
        "name_field": Pharmacy.name,
        "id_label": "pharmacy_id",
        "name_label": "pharmacy",
        "join_table": Pharmacy,
        "join_condition": lambda: Visit.pharmacy_id == Pharmacy.id,
        "join_type": "outerjoin",
    },
    "medical_facility": {
        "id_field": Visit.medical_facility_id,
        "name_field": MedicalFacility.name,
        "id_label": "medical_facility_id",
        "name_label": "medical_facility",
        "join_table": MedicalFacility,
        "join_condition": lambda: Visit.medical_facility_id == MedicalFacility.id,
        "join_type": "outerjoin",
    },
    "year": {
        "id_field": Visit.year,
        "name_field": None,
        "id_label": "year",
        "name_label": None,
        "join_table": None,
        "join_type": None,
    },
    "month": {
        "id_field": Visit.month,
        "name_field": None,
        "id_label": "month",
        "name_label": None,
        "join_table": None,
        "join_type": None,
    },
    "employee": {
        "id_field": Visit.employee_id,
        "name_field": Employee.full_name,
        "id_label": "employee_id",
        "name_label": "employee",
        "join_table": Employee,
        "join_condition": lambda: Visit.employee_id == Employee.id,
        "join_type": "join",
    },
    "product_group": {
        "id_field": Visit.product_group_id,
        "name_field": ProductGroup.name,
        "id_label": "product_group_id",
        "name_label": "product_group",
        "join_table": ProductGroup,
        "join_condition": lambda: Visit.product_group_id == ProductGroup.id,
        "join_type": "join",
    },
    "geo_indicator": {
        "id_field": GeoIndicator.id,
        "name_field": GeoIndicator.name,
        "id_label": "indicator_id",
        "name_label": "indicator_name",
        "join_table": GeoIndicator,
        "join_condition": lambda: or_(
            Pharmacy.geo_indicator_id == GeoIndicator.id,
            MedicalFacility.geo_indicator_id == GeoIndicator.id,
        ),
        "join_type": "outerjoin",
    },
    "speciality": {
        "id_field": Speciality.id,
        "name_field": Speciality.name,
        "id_label": "speciality_id",
        "name_label": "speciality_name",
        "join_table": Speciality,
        "join_condition": lambda: Doctor.speciality_id == Speciality.id,
        "join_type": "outerjoin",
        "requires": ["doctor"],
    },
    "doctor": {
        "id_field": None,
        "name_field": Doctor.full_name,
        "id_label": None,
        "name_label": "doctor_name",
        "join_table": Doctor,
        "join_condition": lambda: Visit.doctor_id == Doctor.id,
        "join_type": "outerjoin",
    },
}
