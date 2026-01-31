from typing import TYPE_CHECKING, Optional
from datetime import datetime

from sqlalchemy import DateTime, String, ForeignKey, Integer, Boolean
from sqlalchemy.orm import Mapped, mapped_column, relationship
from fastapi_users_db_sqlalchemy import SQLAlchemyBaseUserTable, SQLAlchemyUserDatabase

from .base import Base


if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession
    from . import Company, ImportLogs


class User(Base, SQLAlchemyBaseUserTable[int]):
    __tablename__ = 'users'

    first_name: Mapped[str] = mapped_column(String(256))
    last_name: Mapped[str] = mapped_column(String(256))
    patronymic: Mapped[str | None] = mapped_column(String(256), nullable=True)
    phone_number: Mapped[str] = mapped_column(String(64), nullable=True)
    last_login: Mapped[datetime] = mapped_column(nullable=True)
    is_operator: Mapped[bool] = mapped_column(server_default='false')
    is_admin: Mapped[bool] = mapped_column(server_default='false')
    company_id: Mapped[int | None] = mapped_column(ForeignKey('companies.id'), nullable=True)
    company: Mapped[Optional['Company']] = relationship(back_populates='users')
    import_logs: Mapped[list['ImportLogs']] = relationship(back_populates='user')
    position: Mapped[str | None] = mapped_column(String(256), nullable=True)

    @classmethod
    def get_db(cls, session: 'AsyncSession'):
        return SQLAlchemyUserDatabase(session, cls)


class PasswordSetupToken(Base):
    __tablename__ = "password_setup_tokens"

    user_id: Mapped[int] = mapped_column(Integer, ForeignKey('users.id', ondelete="CASCADE"))
    token: Mapped[str] = mapped_column(String, unique=True, index=True)
    is_used: Mapped[bool] = mapped_column(Boolean, server_default='false')
