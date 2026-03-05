from typing import TYPE_CHECKING, Optional

from sqlalchemy import ForeignKey, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base

if TYPE_CHECKING:
    from . import ImportLogs


class IMS(Base):
    __tablename__ = "ims"

    company: Mapped[str]
    brand: Mapped[str]
    segment: Mapped[str]
    dosage: Mapped[str]
    dosage_form: Mapped[str]
    period: Mapped[str]
    amount: Mapped[float]
    packages: Mapped[float]
    molecule: Mapped[str]

    import_log_id: Mapped[int | None] = mapped_column(
        ForeignKey("import_logs.id", ondelete="CASCADE"), nullable=True
    )
    import_log: Mapped[Optional["ImportLogs"]] = relationship(back_populates="ims")

    __table_args__ = (
        Index("idx_ims_period", "period"),
        Index("idx_ims_company", "company"),
        Index("idx_ims_brand", "brand"),
        Index("idx_ims_segment", "segment"),
        Index("idx_ims_dosage_form", "dosage_form"),
        Index("idx_ims_molecule", "molecule"),
    )
