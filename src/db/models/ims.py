from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class IMS(Base):
    __tablename__ = 'ims'

    company: Mapped[str]
    brand: Mapped[str]
    segment: Mapped[str]
    dosage: Mapped[str]
    dosage_form: Mapped[str]
    period: Mapped[str]
    amount: Mapped[float]
    packages: Mapped[float]
    molecule: Mapped[str]
