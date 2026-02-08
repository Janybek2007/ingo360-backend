from typing import TYPE_CHECKING, Optional

from sqlalchemy import String, ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base

if TYPE_CHECKING:
    from . import Employee, MedicalFacility, Pharmacy, Company, ImportLogs


class Country(Base):
    __tablename__ = "countries"

    name: Mapped[str] = mapped_column(String(256), unique=True)
    regions: Mapped[list["Region"]] = relationship(back_populates="country")
    import_log_id: Mapped[int | None] = mapped_column(
        ForeignKey("import_logs.id", ondelete="CASCADE"), nullable=True
    )
    import_log: Mapped[Optional["ImportLogs"]] = relationship(
        back_populates="countries"
    )


class Region(Base):
    __tablename__ = "regions"

    name: Mapped[str] = mapped_column(String(256), unique=True)
    country_id: Mapped[int] = mapped_column(ForeignKey("countries.id"))
    country: Mapped["Country"] = relationship(back_populates="regions")
    settlements: Mapped[list["Settlement"]] = relationship(back_populates="region")
    employees: Mapped[list["Employee"]] = relationship(back_populates="region")
    districts: Mapped[list["District"]] = relationship(back_populates="region")
    import_log_id: Mapped[int | None] = mapped_column(
        ForeignKey("import_logs.id", ondelete="CASCADE"), nullable=True
    )
    import_log: Mapped[Optional["ImportLogs"]] = relationship(back_populates="regions")


class Settlement(Base):
    __tablename__ = "settlements"

    name: Mapped[str] = mapped_column(String(256))
    region_id: Mapped[int] = mapped_column(ForeignKey("regions.id"))
    region: Mapped["Region"] = relationship(back_populates="settlements")
    districts: Mapped[list["District"]] = relationship(
        back_populates="settlement", foreign_keys="[District.settlement_id]"
    )
    medical_facilities: Mapped[list["MedicalFacility"]] = relationship(
        back_populates="settlement"
    )
    pharmacies: Mapped[list["Pharmacy"]] = relationship(back_populates="settlement")
    import_log_id: Mapped[int | None] = mapped_column(
        ForeignKey("import_logs.id", ondelete="CASCADE"), nullable=True
    )
    import_log: Mapped[Optional["ImportLogs"]] = relationship(
        back_populates="settlements"
    )

    __table_args__ = (
        UniqueConstraint("name", "region_id", name="uq_settlement_name_region"),
    )


class District(Base):
    __tablename__ = "districts"

    name: Mapped[str] = mapped_column(String(256))
    settlement_id: Mapped[int | None] = mapped_column(
        ForeignKey("settlements.id"), nullable=True
    )
    settlement: Mapped[Optional["Settlement"]] = relationship(
        back_populates="districts", foreign_keys="[District.settlement_id]"
    )
    employees: Mapped[list["Employee"]] = relationship(
        back_populates="district", foreign_keys="[Employee.district_id]"
    )
    medical_facilities: Mapped[list["MedicalFacility"]] = relationship(
        back_populates="district"
    )
    pharmacies: Mapped[list["Pharmacy"]] = relationship(back_populates="district")
    region_id: Mapped[int] = mapped_column(ForeignKey("regions.id"))
    region: Mapped["Region"] = relationship(back_populates="districts")
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id"))
    company: Mapped["Company"] = relationship(back_populates="districts")
    import_log_id: Mapped[int | None] = mapped_column(
        ForeignKey("import_logs.id", ondelete="CASCADE"), nullable=True
    )
    import_log: Mapped[Optional["ImportLogs"]] = relationship(
        back_populates="districts"
    )

    __table_args__ = (
        UniqueConstraint("name", "region_id", name="uq_district_name_region"),
    )
