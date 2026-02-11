from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Optional

from sqlalchemy import Boolean, CheckConstraint, ForeignKey, String
from sqlalchemy import Enum as SQLEnum
from sqlalchemy.orm import Mapped, mapped_column, relationship, validates

from .base import Base

if TYPE_CHECKING:
    from . import User


class ExcelTaskType(str, Enum):
    IMPORT = "import"
    EXPORT = "export"


class ExcelTaskStatus(str, Enum):
    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"


class ExcelTask(Base):
    __tablename__ = "excel_tasks"

    task_type: Mapped[ExcelTaskType] = mapped_column(
        SQLEnum(ExcelTaskType, name="excel_task_type", native_enum=False)
    )
    task_id: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    status: Mapped[ExcelTaskStatus] = mapped_column(
        SQLEnum(ExcelTaskStatus, name="excel_task_status", native_enum=False)
    )
    started_by: Mapped[int | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )
    file_path: Mapped[str] = mapped_column(String(512))
    error: Mapped[str | None] = mapped_column(String(500), nullable=True)
    is_deleted: Mapped[bool] = mapped_column(Boolean, server_default="false")
    is_file_download: Mapped[bool] = mapped_column(Boolean, server_default="false")
    download_started_at: Mapped[datetime | None] = mapped_column(nullable=True)
    download_confirmed_at: Mapped[datetime | None] = mapped_column(nullable=True)

    user: Mapped[Optional["User"]] = relationship(back_populates="excel_tasks")

    __table_args__ = (
        CheckConstraint(
            "length(error) <= 500",
            name="ck_excel_tasks_error_len",
        ),
    )

    @validates("error")
    def _validate_error(self, _key: str, value: str | None) -> str | None:
        if value is None:
            return None
        return value[:500]
