from sqlalchemy import Index
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


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

    __table_args__ = (
        Index("idx_ims_period", "period"),
        Index("idx_ims_company", "company"),
        Index("idx_ims_brand", "brand"),
        Index("idx_ims_segment", "segment"),
        Index("idx_ims_dosage_form", "dosage_form"),
        Index("idx_ims_molecule", "molecule"),
    )
