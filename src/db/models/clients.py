from typing import TYPE_CHECKING, Optional

from sqlalchemy import ForeignKey, Index, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import CITEXT
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base

if TYPE_CHECKING:
    from . import (
        Base,
        Company,
        District,
        Employee,
        ImportLogs,
        PrimarySalesAndStock,
        ProductGroup,
        SecondarySales,
        Settlement,
        TertiarySalesAndStock,
        Visit,
    )


class ClientCategory(Base):
    __tablename__ = "client_categories"

    name: Mapped[str] = mapped_column(String(256), unique=True)
    doctors: Mapped[list["Doctor"]] = relationship(back_populates="client_category")
    pharmacies: Mapped[list["Pharmacy"]] = relationship(
        back_populates="client_category"
    )
    import_log_id: Mapped[int | None] = mapped_column(
        ForeignKey("import_logs.id", ondelete="CASCADE"), nullable=True
    )
    import_log: Mapped[Optional["ImportLogs"]] = relationship(
        back_populates="client_categories"
    )
    __table_args__ = (Index("idx_client_category_name", "name"),)


class Doctor(Base):
    __tablename__ = "doctors"

    full_name: Mapped[str] = mapped_column(String(256))
    responsible_employee_id: Mapped[int | None] = mapped_column(
        ForeignKey("employees.id"), nullable=True
    )
    responsible_employee: Mapped[Optional["Employee"]] = relationship(
        back_populates="doctors"
    )
    medical_facility_id: Mapped[int] = mapped_column(
        ForeignKey("medical_facilities.id"),
    )
    client_category: Mapped[Optional["ClientCategory"]] = relationship(
        back_populates="doctors"
    )

    medical_facility: Mapped["MedicalFacility"] = relationship(back_populates="doctors")
    speciality_id: Mapped[int] = mapped_column(ForeignKey("specialities.id"))
    speciality: Mapped["Speciality"] = relationship(back_populates="doctors")
    client_category_id: Mapped[int | None] = mapped_column(
        ForeignKey("client_categories.id"), nullable=True
    )
    visits: Mapped[list["Visit"]] = relationship(back_populates="doctor")
    product_group_id: Mapped[int] = mapped_column(
        ForeignKey("product_groups.id"),
    )
    product_group: Mapped["ProductGroup"] = relationship(back_populates="doctors")
    import_log_id: Mapped[int | None] = mapped_column(
        ForeignKey("import_logs.id", ondelete="CASCADE"), nullable=True
    )
    import_log: Mapped[Optional["ImportLogs"]] = relationship(back_populates="doctors")
    company_id: Mapped[int] = mapped_column(
        ForeignKey("companies.id"),
    )
    company: Mapped["Company"] = relationship(back_populates="doctors")

    __table_args__ = (
        Index("idx_doctor_speciality", "speciality_id"),
        Index("idx_doctor_medical_facility", "medical_facility_id"),
        Index("idx_doctor_full_name", "full_name"),
        Index("idx_doctor_company", "company_id"),
        UniqueConstraint(
            "full_name",
            "medical_facility_id",
            "speciality_id",
            "company_id",
            name="uq_doctor_full_name_medical_facility_speciality_company",
        ),
    )


class Pharmacy(Base):
    __tablename__ = "pharmacies"

    name: Mapped[str] = mapped_column(CITEXT)
    distributor_id: Mapped[int | None] = mapped_column(
        ForeignKey("distributors.id"), nullable=True
    )
    distributor: Mapped[Optional["Distributor"]] = relationship(
        back_populates="pharmacies"
    )
    responsible_employee_id: Mapped[int | None] = mapped_column(
        ForeignKey("employees.id"), nullable=True
    )
    responsible_employee: Mapped[Optional["Employee"]] = relationship(
        back_populates="pharmacies"
    )
    settlement_id: Mapped[int | None] = mapped_column(
        ForeignKey("settlements.id"), nullable=True
    )
    settlement: Mapped[Optional["Settlement"]] = relationship(
        back_populates="pharmacies"
    )
    district_id: Mapped[int | None] = mapped_column(
        ForeignKey("districts.id"), nullable=True
    )
    district: Mapped[Optional["District"]] = relationship(back_populates="pharmacies")
    client_category_id: Mapped[int | None] = mapped_column(
        ForeignKey("client_categories.id"), nullable=True
    )
    client_category: Mapped[Optional["ClientCategory"]] = relationship(
        back_populates="pharmacies"
    )
    secondary_sales: Mapped[list["SecondarySales"]] = relationship(
        back_populates="pharmacy"
    )
    tertiary_sales: Mapped[list["TertiarySalesAndStock"]] = relationship(
        back_populates="pharmacy"
    )
    product_group_id: Mapped[int] = mapped_column(ForeignKey("product_groups.id"))
    product_group: Mapped["ProductGroup"] = relationship(back_populates="pharmacies")
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id"))
    company: Mapped["Company"] = relationship(back_populates="pharmacies")
    import_log_id: Mapped[int | None] = mapped_column(
        ForeignKey("import_logs.id", ondelete="CASCADE"), nullable=True
    )
    import_log: Mapped[Optional["ImportLogs"]] = relationship(
        back_populates="pharmacies"
    )
    visits: Mapped[list["Visit"]] = relationship(back_populates="pharmacy")
    geo_indicator_id: Mapped[int | None] = mapped_column(
        ForeignKey("geo_indicators.id"), nullable=True
    )
    geo_indicator: Mapped[Optional["GeoIndicator"]] = relationship(
        back_populates="pharmacies"
    )

    __table_args__ = (
        UniqueConstraint(
            "name",
            "product_group_id",
            "company_id",
            name="uq_pharmacy_name_group_company",
        ),
        Index("idx_pharmacy_name_product_group", "name", "product_group_id"),
        Index("idx_pharmacy_name", "name"),
        Index("idx_pharmacy_company", "company_id"),
        Index("idx_pharmacy_distributor", "distributor_id"),
        Index("idx_pharmacy_geo_indicator", "geo_indicator_id"),
    )


class Speciality(Base):
    __tablename__ = "specialities"

    name: Mapped[str] = mapped_column(String(256), unique=True)
    doctors: Mapped[list["Doctor"]] = relationship(back_populates="speciality")
    import_log_id: Mapped[int | None] = mapped_column(
        ForeignKey("import_logs.id", ondelete="CASCADE"), nullable=True
    )
    import_log: Mapped[Optional["ImportLogs"]] = relationship(
        back_populates="specialities"
    )
    __table_args__ = (Index("idx_speciality_name", "name"),)


class MedicalFacility(Base):
    __tablename__ = "medical_facilities"

    name: Mapped[str] = mapped_column(CITEXT)
    settlement_id: Mapped[int] = mapped_column(ForeignKey("settlements.id"))
    settlement: Mapped["Settlement"] = relationship(back_populates="medical_facilities")
    district_id: Mapped[int | None] = mapped_column(
        ForeignKey("districts.id"), nullable=True
    )
    district: Mapped[Optional["District"]] = relationship(
        back_populates="medical_facilities"
    )
    address: Mapped[str | None] = mapped_column(String(256), nullable=True)
    doctors: Mapped[list["Doctor"]] = relationship(back_populates="medical_facility")
    visits: Mapped[list["Visit"]] = relationship(back_populates="medical_facility")
    import_log_id: Mapped[int | None] = mapped_column(
        ForeignKey("import_logs.id", ondelete="CASCADE"), nullable=True
    )
    import_log: Mapped[Optional["ImportLogs"]] = relationship(
        back_populates="medical_facilities"
    )
    facility_type: Mapped[str | None] = mapped_column(String(256), nullable=True)
    geo_indicator_id: Mapped[int | None] = mapped_column(
        ForeignKey("geo_indicators.id"), nullable=True
    )
    geo_indicator: Mapped[Optional["GeoIndicator"]] = relationship(
        back_populates="medical_facilities"
    )

    __table_args__ = (
        Index("idx_medical_facility_name", "name"),
        UniqueConstraint("name", "geo_indicator_id", name="uq_facility_name_indicator"),
        Index("idx_med_facility_geo_indicator", "geo_indicator_id"),
        UniqueConstraint("name", "settlement_id", name="uq_facility_name_settlement"),
    )


class Distributor(Base):
    __tablename__ = "distributors"

    name: Mapped[str] = mapped_column(String(256), unique=True)
    pharmacies: Mapped[list["Pharmacy"]] = relationship(back_populates="distributor")
    primary_sales: Mapped[list["PrimarySalesAndStock"]] = relationship(
        back_populates="distributor"
    )
    import_log_id: Mapped[int | None] = mapped_column(
        ForeignKey("import_logs.id", ondelete="CASCADE"), nullable=True
    )
    import_log: Mapped[Optional["ImportLogs"]] = relationship(
        back_populates="distributors"
    )
    secondary_sales: Mapped[list["SecondarySales"]] = relationship(
        back_populates="distributor"
    )

    __table_args__ = (Index("idx_distributor_name", "name"),)


class GeoIndicator(Base):
    __tablename__ = "geo_indicators"

    name: Mapped[str] = mapped_column(CITEXT)

    pharmacies: Mapped[list["Pharmacy"]] = relationship(back_populates="geo_indicator")
    medical_facilities: Mapped[list["MedicalFacility"]] = relationship(
        back_populates="geo_indicator"
    )
    import_log_id: Mapped[int | None] = mapped_column(
        ForeignKey("import_logs.id", ondelete="CASCADE"), nullable=True
    )
    import_log: Mapped[Optional["ImportLogs"]] = relationship(
        back_populates="geo_indicators"
    )
    __table_args__ = (Index("idx_geo_indicator_name", "name"),)
