from datetime import date
from typing import TYPE_CHECKING, Optional

from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base

if TYPE_CHECKING:
    from . import (
        SKU,
        Brand,
        District,
        Employee,
        ImportLogs,
        Pharmacy,
        ProductGroup,
        User,
    )


class Company(Base):
    __tablename__ = "companies"

    name: Mapped[str] = mapped_column(String, unique=True)
    ims_name: Mapped[str] = mapped_column(String, unique=True, nullable=True)
    brands: Mapped[list["Brand"]] = relationship(back_populates="company")
    active_users_limit: Mapped[int]
    users: Mapped[list["User"]] = relationship(back_populates="company")
    employees: Mapped[list["Employee"]] = relationship(back_populates="company")
    is_active: Mapped[bool] = mapped_column(server_default="true")
    contract_number: Mapped[str] = mapped_column(index=True, unique=True)
    contract_end_date: Mapped[date]
    address: Mapped[str] = mapped_column(String(256), nullable=True)

    can_primary_sales: Mapped[bool] = mapped_column(server_default="true")
    can_secondary_sales: Mapped[bool] = mapped_column(server_default="true")
    can_tertiary_sales: Mapped[bool] = mapped_column(server_default="true")

    can_visits: Mapped[bool] = mapped_column(server_default="true")
    can_market_analysis: Mapped[bool] = mapped_column(server_default="true")

    pharmacies: Mapped[list["Pharmacy"]] = relationship(back_populates="company")
    districts: Mapped[list["District"]] = relationship(back_populates="company")
    skus: Mapped[list["SKU"]] = relationship(back_populates="company")
    product_groups: Mapped[list["ProductGroup"]] = relationship(
        back_populates="company"
    )
    import_log_id: Mapped[int | None] = mapped_column(
        ForeignKey("import_logs.id", ondelete="CASCADE"), nullable=True
    )
    import_log: Mapped[Optional["ImportLogs"]] = relationship(
        back_populates="companies"
    )


class RegistrationApplication(Base):
    __tablename__ = "registration_applications"

    owner_name: Mapped[str] = mapped_column(String(256))
    company_name: Mapped[str] = mapped_column(unique=True)
    email: Mapped[str] = mapped_column(unique=True)
