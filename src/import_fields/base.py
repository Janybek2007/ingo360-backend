from src.db.models import (
    Brand,
    ClientCategory,
    Company,
    Distributor,
    District,
    Dosage,
    DosageForm,
    Employee,
    GeoIndicator,
    MedicalFacility,
    Position,
    ProductGroup,
    PromotionType,
    Region,
    Segment,
    Settlement,
    Speciality,
)
from src.db.models.geography import Country
from src.import_fields import record_keys as keys
from src.utils.records_resolver import FieldResolverConfig as FC

fio = FC(keys.FIO_KEY)
name = FC(keys.NAME_KEY)
ims_name = FC(keys.IMS_NAME_KEY)

country = FC(
    keys.COUNTRY_KEY, Country, "name", error_label="страна", db_field="country_id"
)
company = FC(
    keys.COMPANY_KEY, Company, "name", error_label="компания", db_field="company_id"
)
group = FC(
    keys.GROUP_KEY,
    ProductGroup,
    "name",
    error_label="группа",
    db_field="product_group_id",
)
employee = FC(
    keys.RESPONSIBLE_EMPLOYEE_KEY,
    Employee,
    "full_name",
    db_field="responsible_employee_id",
)
category = FC(
    keys.CATEGORY_KEY,
    ClientCategory,
    "name",
    error_label="категория",
    db_field="client_category_id",
)
speciality = FC(
    keys.SPECIALITY_KEY,
    Speciality,
    "name",
    error_label="специальность",
    db_field="speciality_id",
)
lpu = FC(
    keys.MEDICAL_FACILITY_KEY,
    MedicalFacility,
    "name",
    error_label="ЛПУ",
    db_field="medical_facility_id",
)
distributor = FC(
    keys.DISTRIBUTOR_NETWORK_KEY,
    Distributor,
    "name",
    error_label="дистрибьютор",
    db_field="distributor_id",
)
region = FC(
    keys.REGION_KEY, Region, "name", error_label="область", db_field="region_id"
)
settlement = FC(
    keys.SETTLEMENT_KEY,
    Settlement,
    "name",
    error_label="населенный пункт",
    db_field="settlement_id",
)
geo_indicator = FC(
    keys.GEO_INDICATOR_KEY,
    GeoIndicator,
    "name",
    error_label="индикатор",
    db_field="geo_indicator_id",
)
position = FC(
    keys.POSITION_KEY, Position, "name", error_label="должность", db_field="position_id"
)
district = FC(
    keys.DISTRICT_KEY, District, "name", error_label="район", db_field="district_id"
)
brand = FC(keys.BRAND_KEY, Brand, "name", error_label="бренд", db_field="brand_id")
dosage_form = FC(
    keys.DOSAGE_FORM_KEY,
    DosageForm,
    "name",
    error_label="форма выпуска",
    db_field="dosage_form_id",
)
dosage = FC(
    keys.DOSAGE_KEY, Dosage, "name", error_label="дозировка", db_field="dosage_id"
)
promotion_type = FC(
    keys.PROMOTION_TYPE_KEY,
    PromotionType,
    "name",
    error_label="тип промоции",
    db_field="promotion_type_id",
)
segment = FC(
    keys.SEGMENT_KEY, Segment, "name", error_label="сегмент", db_field="segment_id"
)
ims_name = FC(keys.IMS_NAME_KEY)
