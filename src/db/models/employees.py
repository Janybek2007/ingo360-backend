from typing import TYPE_CHECKING, Optional

from sqlalchemy import String, ForeignKey, ForeignKeyConstraint, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base


if TYPE_CHECKING:
    from . import (
        Region, District,
        Doctor, Pharmacy,
        ProductGroup, Company, Visit,
        ImportLogs,
    )


class Employee(Base):
    __tablename__ = 'employees'

    full_name: Mapped[str] = mapped_column(String(256))
    position_id: Mapped[int] = mapped_column(ForeignKey('positions.id'))
    position: Mapped['Position'] = relationship(back_populates='employees')
    product_group_id: Mapped[int] = mapped_column(ForeignKey('product_groups.id'))
    product_group: Mapped['ProductGroup'] = relationship(back_populates='employees')
    region_id: Mapped[int] = mapped_column(ForeignKey('regions.id'))
    region: Mapped['Region'] = relationship(back_populates='employees')
    district_id: Mapped[int | None] = mapped_column(ForeignKey('districts.id'), nullable=True)
    district: Mapped[Optional['District']] = relationship(back_populates='employees', foreign_keys='[Employee.district_id]')
    doctors: Mapped[list['Doctor']] = relationship(back_populates='responsible_employee')
    pharmacies: Mapped[list['Pharmacy']] = relationship(back_populates='responsible_employee')
    company_id: Mapped[int] = mapped_column(ForeignKey('companies.id'))
    company: Mapped['Company'] = relationship(back_populates='employees')
    visits: Mapped[list['Visit']] = relationship(back_populates='employee')
    import_log_id: Mapped[int | None] = mapped_column(ForeignKey('import_logs.id', ondelete='CASCADE'), nullable=True)
    import_log: Mapped[Optional['ImportLogs']] = relationship(back_populates='employees')

    __table_args__ = (
        UniqueConstraint('full_name', 'company_id', name='uq_full_name_company'),
    )


class Position(Base):
    __tablename__ = 'positions'

    name: Mapped[str] = mapped_column(String(256), unique=True)
    employees: Mapped[list['Employee']] = relationship(back_populates='position')
    import_log_id: Mapped[int | None] = mapped_column(ForeignKey('import_logs.id', ondelete='CASCADE'), nullable=True)
    import_log: Mapped[Optional['ImportLogs']] = relationship(back_populates='positions')
