from decimal import Decimal
from typing import TYPE_CHECKING, Optional

from sqlalchemy import ForeignKey, Index, Numeric, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base

if TYPE_CHECKING:
    from . import SKU, Distributor, ImportLogs, Pharmacy


class PrimarySalesAndStock(Base):
    __tablename__ = "primary_sales_and_stock"

    distributor_id: Mapped[int] = mapped_column(ForeignKey("distributors.id"))
    distributor: Mapped["Distributor"] = relationship(back_populates="primary_sales")
    sku_id: Mapped[int] = mapped_column(ForeignKey("skus.id"))
    sku: Mapped["SKU"] = relationship(back_populates="primary_sales")
    month: Mapped[int]
    quarter: Mapped[int]
    year: Mapped[int]
    indicator: Mapped[str] = mapped_column(String(256))
    packages: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    published: Mapped[bool] = mapped_column(server_default="false")
    import_log_id: Mapped[int | None] = mapped_column(
        ForeignKey("import_logs.id", ondelete="CASCADE"), nullable=True
    )
    import_log: Mapped[Optional["ImportLogs"]] = relationship(
        back_populates="primary_sales"
    )

    __table_args__ = (
        UniqueConstraint(
            "distributor_id",
            "sku_id",
            "month",
            "year",
            "indicator",
            name="uq_primary_sales_business_key",
        ),
        Index("idx_primary_sales_year_month", "year", "month"),
        Index("idx_primary_sales_sku_year", "sku_id", "year"),
        Index(
            "idx_primary_sales_pharmacy_sku_month_year",
            "distributor_id",
            "sku_id",
            "month",
            "year",
        ),
        Index(
            "idx_primary_sales_distributor_year_month",
            "distributor_id",
            "year",
            "month",
        ),
        Index(
            "idx_primary_sales_filters",
            "distributor_id",
            "sku_id",
            "year",
            "quarter",
            "month",
            "indicator",
            "published",
        ),
    )


class SecondarySales(Base):
    __tablename__ = "secondary_sales"

    pharmacy_id: Mapped[int] = mapped_column(ForeignKey("pharmacies.id"))
    pharmacy: Mapped["Pharmacy"] = relationship(back_populates="secondary_sales")
    sku_id: Mapped[int] = mapped_column(ForeignKey("skus.id"))
    sku: Mapped["SKU"] = relationship(back_populates="secondary_sales")
    month: Mapped[int]
    year: Mapped[int]
    indicator: Mapped[str] = mapped_column(String(256))
    quarter: Mapped[int]
    packages: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    published: Mapped[bool] = mapped_column(server_default="false")
    import_log_id: Mapped[int | None] = mapped_column(
        ForeignKey("import_logs.id", ondelete="CASCADE"), nullable=True
    )
    import_log: Mapped[Optional["ImportLogs"]] = relationship(
        back_populates="secondary_sales"
    )
    distributor_id: Mapped[int] = mapped_column(ForeignKey("distributors.id"))
    distributor: Mapped["Distributor"] = relationship(back_populates="secondary_sales")

    __table_args__ = (
        UniqueConstraint(
            "pharmacy_id",
            "sku_id",
            "month",
            "year",
            "indicator",
            name="uq_secondary_sales_business_key",
        ),
        Index("idx_secondary_sales_year_month", "year", "month"),
        Index("idx_secondary_sales_sku_year", "sku_id", "year"),
        Index(
            "idx_secondary_sales_pharmacy_sku_month_year",
            "pharmacy_id",
            "sku_id",
            "month",
            "year",
        ),
        Index(
            "idx_secondary_sales_filters",
            "pharmacy_id",
            "sku_id",
            "year",
            "quarter",
            "month",
            "indicator",
            "published",
        ),
    )


class TertiarySalesAndStock(Base):
    __tablename__ = "tertiary_sales_and_stock"

    pharmacy_id: Mapped[int] = mapped_column(ForeignKey("pharmacies.id"))
    pharmacy: Mapped["Pharmacy"] = relationship(back_populates="tertiary_sales")
    sku_id: Mapped[int] = mapped_column(ForeignKey("skus.id"))
    sku: Mapped["SKU"] = relationship(back_populates="tertiary_sales")
    month: Mapped[int]
    year: Mapped[int]
    quarter: Mapped[int]
    indicator: Mapped[str] = mapped_column(String(256))
    packages: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    published: Mapped[bool] = mapped_column(server_default="false")
    import_log_id: Mapped[int | None] = mapped_column(
        ForeignKey("import_logs.id", ondelete="CASCADE"), nullable=True
    )
    import_log: Mapped[Optional["ImportLogs"]] = relationship(
        back_populates="tertiary_sales"
    )
    distributor_id: Mapped[int] = mapped_column(ForeignKey("distributors.id"))
    distributor: Mapped["Distributor"] = relationship(back_populates="tertiary_sales")

    __table_args__ = (
        UniqueConstraint(
            "pharmacy_id",
            "sku_id",
            "month",
            "year",
            "indicator",
            name="uq_tertiary_sales_business_key",
        ),
        Index(
            "idx_tertiary_sales_pharmacy_sku_month_year",
            "pharmacy_id",
            "sku_id",
            "month",
            "year",
        ),
        Index("idx_tertiary_sales_year_month", "year", "month"),
        Index("idx_tertiary_sales_sku_year", "sku_id", "year"),
        Index(
            "idx_tertiary_sales_filters",
            "pharmacy_id",
            "sku_id",
            "year",
            "quarter",
            "month",
            "indicator",
            "published",
        ),
    )
