from typing import TYPE_CHECKING, Optional

from sqlalchemy import ForeignKey, Index, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base

if TYPE_CHECKING:
    from . import (
        Company,
        Doctor,
        Employee,
        ImportLogs,
        Pharmacy,
        PrimarySalesAndStock,
        SecondarySales,
        TertiarySalesAndStock,
        Visit,
    )


class Brand(Base):
    __tablename__ = "brands"

    name: Mapped[str] = mapped_column(String(256), unique=True, index=True)
    ims_name: Mapped[str] = mapped_column(String(256), unique=True, nullable=True)
    promotion_type_id: Mapped[int] = mapped_column(ForeignKey("promotion_types.id"))
    promotion_type: Mapped["PromotionType"] = relationship(back_populates="brands")
    product_group_id: Mapped[int] = mapped_column(ForeignKey("product_groups.id"))
    product_group: Mapped["ProductGroup"] = relationship(back_populates="brands")
    sku: Mapped[list["SKU"]] = relationship(back_populates="brand")
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id"))
    company: Mapped["Company"] = relationship(back_populates="brands")
    import_log_id: Mapped[int | None] = mapped_column(
        ForeignKey("import_logs.id", ondelete="CASCADE"), nullable=True
    )
    import_log: Mapped[Optional["ImportLogs"]] = relationship(back_populates="brands")
    __table_args__ = (
        Index("idx_brand_name", "name"),
        Index("idx_brand_ims_name", "ims_name"),
        Index("idx_brand_company", "company_id"),
    )


class PromotionType(Base):
    __tablename__ = "promotion_types"

    name: Mapped[str] = mapped_column(String(256), unique=True)
    brands: Mapped[list["Brand"]] = relationship(back_populates="promotion_type")
    sku: Mapped[list["SKU"]] = relationship(back_populates="promotion_type")
    import_log_id: Mapped[int | None] = mapped_column(
        ForeignKey("import_logs.id", ondelete="CASCADE"), nullable=True
    )
    import_log: Mapped[Optional["ImportLogs"]] = relationship(
        back_populates="promotion_type"
    )
    __table_args__ = (Index("idx_promotion_type_name", "name"),)


class DosageForm(Base):
    __tablename__ = "dosage_forms"

    name: Mapped[str] = mapped_column(String(256))
    sku: Mapped[list["SKU"]] = relationship(back_populates="dosage_form")
    import_log_id: Mapped[int | None] = mapped_column(
        ForeignKey("import_logs.id", ondelete="CASCADE"), nullable=True
    )
    import_log: Mapped[Optional["ImportLogs"]] = relationship(
        back_populates="dosage_forms"
    )
    __table_args__ = (Index("idx_dosage_form_name", "name"),)


class Dosage(Base):
    __tablename__ = "dosages"

    name: Mapped[str] = mapped_column(String(256))
    sku: Mapped[list["SKU"]] = relationship(back_populates="dosage")
    import_log_id: Mapped[int | None] = mapped_column(
        ForeignKey("import_logs.id", ondelete="CASCADE"), nullable=True
    )
    import_log: Mapped[Optional["ImportLogs"]] = relationship(back_populates="dosages")
    __table_args__ = (Index("idx_dosage_name", "name"),)


class Segment(Base):
    __tablename__ = "segments"

    name: Mapped[str] = mapped_column(String(256), unique=True)
    sku: Mapped[list["SKU"]] = relationship(back_populates="segment")
    import_log_id: Mapped[int | None] = mapped_column(
        ForeignKey("import_logs.id", ondelete="CASCADE"), nullable=True
    )
    import_log: Mapped[Optional["ImportLogs"]] = relationship(back_populates="segments")
    __table_args__ = (Index("idx_segment_name", "name"),)


class SKU(Base):
    __tablename__ = "skus"

    name: Mapped[str] = mapped_column(String(256))
    brand_id: Mapped[int] = mapped_column(ForeignKey("brands.id"))
    brand: Mapped["Brand"] = relationship(back_populates="sku")
    promotion_type_id: Mapped[int] = mapped_column(ForeignKey("promotion_types.id"))
    promotion_type: Mapped["PromotionType"] = relationship(back_populates="sku")
    product_group_id: Mapped[int] = mapped_column(ForeignKey("product_groups.id"))
    product_group: Mapped["ProductGroup"] = relationship(back_populates="sku")
    dosage_form_id: Mapped[int] = mapped_column(ForeignKey("dosage_forms.id"))
    dosage_form: Mapped["DosageForm"] = relationship(back_populates="sku")
    dosage_id: Mapped[int | None] = mapped_column(
        ForeignKey("dosages.id"), nullable=True
    )
    dosage: Mapped[Optional["Dosage"]] = relationship(back_populates="sku")
    segment_id: Mapped[int | None] = mapped_column(
        ForeignKey("segments.id"), nullable=True
    )
    segment: Mapped[Optional["Segment"]] = relationship(back_populates="sku")
    primary_sales: Mapped[list["PrimarySalesAndStock"]] = relationship(
        back_populates="sku"
    )
    secondary_sales: Mapped[list["SecondarySales"]] = relationship(back_populates="sku")
    tertiary_sales: Mapped[list["TertiarySalesAndStock"]] = relationship(
        back_populates="sku"
    )
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id"))
    company: Mapped["Company"] = relationship(back_populates="skus")
    import_log_id: Mapped[int | None] = mapped_column(
        ForeignKey("import_logs.id", ondelete="CASCADE"), nullable=True
    )
    import_log: Mapped[Optional["ImportLogs"]] = relationship(back_populates="skus")

    __table_args__ = (
        UniqueConstraint("name", "company_id", name="uq_sku_name_company"),
        Index("idx_sku_name_company", "name", "company_id"),
        Index("idx_sku_name", "name"),
        Index("idx_sku_company", "company_id"),
        Index("idx_sku_brand", "brand_id"),
        Index("idx_sku_promotion", "promotion_type_id"),
        Index("idx_sku_product_group", "product_group_id"),
        Index("idx_sku_segment", "segment_id"),
    )


class ProductGroup(Base):
    __tablename__ = "product_groups"

    name: Mapped[str] = mapped_column(String, unique=True)
    employees: Mapped[list["Employee"]] = relationship(back_populates="product_group")
    brands: Mapped[list["Brand"]] = relationship(back_populates="product_group")
    sku: Mapped[list["SKU"]] = relationship(back_populates="product_group")
    visits: Mapped[list["Visit"]] = relationship(back_populates="product_group")
    pharmacies: Mapped[list["Pharmacy"]] = relationship(back_populates="product_group")
    doctors: Mapped[list["Doctor"]] = relationship(back_populates="product_group")
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id"))
    company: Mapped["Company"] = relationship(back_populates="product_groups")
    import_log_id: Mapped[int | None] = mapped_column(
        ForeignKey("import_logs.id", ondelete="CASCADE"), nullable=True
    )
    import_log: Mapped[Optional["ImportLogs"]] = relationship(
        back_populates="product_groups"
    )

    __table_args__ = (
        Index("idx_product_group_name", "name"),
        Index("idx_product_group_company", "company_id"),
        UniqueConstraint("name", "company_id", name="uq_group_name_company"),
    )
