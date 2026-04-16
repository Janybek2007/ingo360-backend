from typing import TYPE_CHECKING, Optional

from sqlalchemy import ForeignKey, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base

if TYPE_CHECKING:
    from . import (
        Company,
        Doctor,
        Employee,
        ImportLogs,
        MedicalFacility,
        Pharmacy,
        ProductGroup,
    )


class Visit(Base):
    __tablename__ = "visits"

    product_group_id: Mapped[int] = mapped_column(ForeignKey("product_groups.id"))
    product_group: Mapped["ProductGroup"] = relationship(back_populates="visits")
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id"))
    company: Mapped["Company"] = relationship(back_populates="visits")
    employee_id: Mapped[int] = mapped_column(ForeignKey("employees.id"))
    employee: Mapped["Employee"] = relationship(back_populates="visits")
    client_type: Mapped[str]
    month: Mapped[int]
    year: Mapped[int]
    doctor_id: Mapped[int | None] = mapped_column(
        ForeignKey("doctors.id"), nullable=True
    )
    doctor: Mapped[Optional["Doctor"]] = relationship(back_populates="visits")
    medical_facility_id: Mapped[int | None] = mapped_column(
        ForeignKey("medical_facilities.id"), nullable=True
    )
    medical_facility: Mapped[Optional["MedicalFacility"]] = relationship(
        back_populates="visits"
    )
    pharmacy_id: Mapped[int | None] = mapped_column(
        ForeignKey("pharmacies.id"), nullable=True
    )
    pharmacy: Mapped[Optional["Pharmacy"]] = relationship(back_populates="visits")
    import_log_id: Mapped[int | None] = mapped_column(
        ForeignKey("import_logs.id", ondelete="CASCADE"), nullable=True
    )
    import_log: Mapped[Optional["ImportLogs"]] = relationship(back_populates="visits")

    __table_args__ = (
        Index(
            "idx_visits_filters",
            "year",
            "month",
            "employee_id",
            "pharmacy_id",
            "medical_facility_id",
            "product_group_id",
            "doctor_id",
        ),
    )
