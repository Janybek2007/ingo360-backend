from typing import TYPE_CHECKING, Optional

from sqlalchemy import ForeignKey
from sqlalchemy.orm import mapped_column, Mapped, relationship

from .base import Base


if TYPE_CHECKING:
    from . import (
        User, Pharmacy, PrimarySalesAndStock, SecondarySales,
        TertiarySalesAndStock, District, Settlement, Region,
        MedicalFacility, Doctor, Employee, Brand, SKU, Speciality,
        Distributor, Company, Position, GeoIndicator, ClientCategory,
        Country, DosageForm, Dosage, Segment, ProductGroup
    )


class ImportLogs(Base):
    __tablename__ = 'import_logs'

    uploaded_by: Mapped[int | None] = mapped_column(
        ForeignKey('users.id', ondelete='SET NULL', use_alter=True, name='fk_importlog_user'), nullable=True
    )
    target_table: Mapped[str]
    records_count: Mapped[int]

    user: Mapped[Optional['User']] = relationship(back_populates='import_logs')

    primary_sales: Mapped[list['PrimarySalesAndStock']] = relationship(
        back_populates='import_log',
        cascade='all, delete-orphan'
    )
    secondary_sales: Mapped[list['SecondarySales']] = relationship(
        back_populates='import_log',
        cascade='all, delete-orphan'
    )
    tertiary_sales: Mapped[list['TertiarySalesAndStock']] = relationship(
        back_populates='import_log',
        cascade='all, delete-orphan'
    )
    pharmacies: Mapped[list['Pharmacy']] = relationship(
        back_populates='import_log',
        cascade='all, delete-orphan'
    )
    districts: Mapped[list['District']] = relationship(
        back_populates='import_log',
        cascade='all, delete-orphan'
    )
    settlements: Mapped[list['Settlement']] = relationship(
        back_populates='import_log',
        cascade='all, delete-orphan'
    )
    regions: Mapped[list['Region']] = relationship(
        back_populates='import_log',
        cascade='all, delete-orphan'
    )
    medical_facilities: Mapped[list['MedicalFacility']] = relationship(
        back_populates='import_log',
        cascade='all, delete-orphan'
    )
    doctors: Mapped[list['Doctor']] = relationship(
        back_populates='import_log',
        cascade='all, delete-orphan'
    )
    employees: Mapped[list['Employee']] = relationship(
        back_populates='import_log',
        cascade='all, delete-orphan'
    )
    brands: Mapped[list['Brand']] = relationship(
        back_populates='import_log',
        cascade='all, delete-orphan'
    )
    skus: Mapped[list['SKU']] = relationship(
        back_populates='import_log',
        cascade='all, delete-orphan'
    )
    specialities: Mapped[list['Speciality']] = relationship(
        back_populates='import_log',
        cascade='all, delete-orphan'
    )
    distributors: Mapped[list['Distributor']] = relationship(
        back_populates='import_log',
        cascade='all, delete-orphan'
    )
    companies: Mapped[list['Company']] = relationship(
        back_populates='import_log',
        cascade='all, delete-orphan'
    )
    positions: Mapped[list['Position']] = relationship(
        back_populates='import_log',
        cascade='all, delete-orphan'
    )
    geo_indicators: Mapped[list['GeoIndicator']] = relationship(
        back_populates='import_log',
        cascade='all, delete-orphan'
    )

    client_categories: Mapped[list['ClientCategory']] = relationship(
        back_populates='import_log',
        cascade='all, delete-orphan'
    )

    countries: Mapped[list['Country']] = relationship(
        back_populates='import_log',
        cascade='all, delete-orphan'
    )

    dosage_forms: Mapped[list['DosageForm']] = relationship(
        back_populates='import_log',
        cascade='all, delete-orphan'
    )

    dosages: Mapped[list['Dosage']] = relationship(
        back_populates='import_log',
        cascade='all, delete-orphan'
    )

    segments: Mapped[list['Segment']] = relationship(
        back_populates='import_log',
        cascade='all, delete-orphan'
    )

    product_groups: Mapped[list['ProductGroup']] = relationship(
        back_populates='import_log',
        cascade='all, delete-orphan'
    )

