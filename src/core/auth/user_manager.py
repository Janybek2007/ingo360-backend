import logging
import secrets
from typing import Optional, TYPE_CHECKING, Sequence, Any

from fastapi_users import BaseUserManager, IntegerIDMixin, exceptions
from fastapi import HTTPException, status
from fastapi_users.db import BaseUserDatabase
from sqlalchemy import select, func, update, or_

from src.db.models import User, Company, PasswordSetupToken
from src.core.settings import settings
from src.schemas.user import (
    UserCreate,
    UserUpdate,
    UserCreateWithoutPassword,
    UserAdminUpdate,
)
from src.tasks.email import send_email
from src.websocket.connection_manager import ConnectionManager
from src.schemas.user import UserFilter

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from fastapi import Request, BackgroundTasks
    from sqlalchemy.ext.asyncio import AsyncSession
    from fastapi_users.password import PasswordHelperProtocol


class UserManager(IntegerIDMixin, BaseUserManager[User, int]):
    reset_password_token_secret = settings.SECRET_KEY
    verification_token_secret = settings.SECRET_KEY

    def __init__(
        self,
        user_db: BaseUserDatabase[User, int],
        connect_manager: ConnectionManager,
        password_helper: Optional["PasswordHelperProtocol"] = None,
        background_tasks: Optional["BackgroundTasks"] = None,
    ):
        super().__init__(user_db, password_helper)
        self.background_tasks = background_tasks
        self.connect_manager = connect_manager

    async def on_after_forgot_password(
        self, user: User, token: str, request: Optional["Request"] = None
    ):
        log.info("User %r has forgot their password. Reset token: %r", user.id, token)

        reset_url = f"https://ingo360.pro/auth/reset-password?token={token}"

        html_body = f"""
        <html>
        <body>
            <h2>Восстановление пароля {user.first_name} {user.email}</h2>
            <p>Нажмите на ссылку ниже, чтобы сбросить пароль:</p>
            <p><a href="{reset_url}">Сбросить пароль</a></p>
        </body>
        </html>
        """

        if self.background_tasks:
            self.background_tasks.add_task(
                send_email,
                to_email=user.email,
                body=html_body,
                subject="Восстановление пароля",
                is_html=True,
            )
        else:
            log.warning("BackgroundTasks not available, email not sent")

    async def on_after_request_verify(
        self, user: User, token: str, request: Optional["Request"] = None
    ):
        log.info(
            "Verification requested for user %r. Verification token: %r", user.id, token
        )

    async def create_with_permission(
        self,
        user_create: UserCreateWithoutPassword,
        creator: User,
        session: "AsyncSession",
        safe: bool = False,
        request: Optional["Request"] = None,
    ) -> User:
        try:
            _existing_user = await self.get_by_email(user_create.email)
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Пользователь с таким email уже существует",
            )
        except exceptions.UserNotExists:
            pass

        if user_create.is_admin and user_create.is_operator:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Пользователь не может одновременно быть администратором и оператором",
            )

        if user_create.is_admin and not creator.is_superuser:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Только суперпользователь может создавать администраторов.",
            )

        if user_create.is_operator and not (creator.is_superuser or creator.is_admin):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Только суперпользователь и администраторы могут создавать операторов.",
            )

        if (user_create.is_operator or user_create.is_admin) and user_create.company_id:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Администраторы и операторы не могут быть сотрудниками какой-либо компании",
            )

        temporary_password = secrets.token_urlsafe(32)

        user_create_dict = user_create.model_dump()
        user_create_dict["password"] = temporary_password

        user_create_with_password = UserCreate(**user_create_dict)

        created_user = await self.create(user_create_with_password, safe, request)
        token = await self.generate_password_setup_token(created_user.id, session)
        await self.send_password_setup_email(
            created_user.email, token, created_user.first_name
        )

        return created_user

    async def update(
        self,
        user_update: UserUpdate | UserAdminUpdate,
        user: User,
        safe: bool = True,
        request: Optional["Request"] = None,
    ):
        update_data = user_update.model_dump(exclude_unset=True)
        final_is_admin = update_data.get("is_admin", user.is_admin)
        final_is_operator = update_data.get("is_operator", user.is_operator)
        final_company_id = update_data.get("company_id", user.company_id)

        if final_is_admin and final_is_operator:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Пользователь не может одновременно быть администратором и оператором",
            )

        if (final_is_admin or final_is_operator) and final_company_id:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Администраторы и операторы не могут быть сотрудниками компании",
            )

        if (
            "is_active" in update_data
            and not update_data["is_active"]
            and user.is_active
        ):
            await self.on_after_deactivate(user)

        if "password" in update_data:
            update_data["hashed_password"] = self.password_helper.hash(
                update_data["password"]
            )
            del update_data["password"]

        updated_user = await self.user_db.update(user, update_data)

        return updated_user

    @staticmethod
    async def get_user_by_id(
        session: "AsyncSession", user_id: int, load_options: list[Any] | None = None
    ) -> User:
        stmt = select(User)

        if load_options:
            stmt = stmt.options(*load_options)

        stmt = stmt.where(User.id == user_id)

        result = await session.execute(stmt)

        user = result.unique().scalar_one_or_none()

        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Пользователь с ID {user_id} не найден",
            )

        return user

    @staticmethod
    async def get_regular_users(
        session: "AsyncSession", load_options: list[Any] | None = None
    ) -> Sequence[User]:
        stmt = select(User)

        if load_options:
            stmt = stmt.options(*load_options)

        stmt = stmt.where(~User.is_admin, ~User.is_operator)

        result = await session.execute(stmt)

        return result.unique().scalars().all()

    @staticmethod
    async def get_admins_and_operators(
        session: "AsyncSession", load_options: list[Any] | None = None
    ) -> Sequence[User]:
        stmt = select(User)

        if load_options:
            stmt = stmt.options(*load_options)

        stmt = stmt.where(or_(User.is_admin, User.is_operator))
        result = await session.execute(stmt)

        return result.unique().scalars().all()

    @staticmethod
    async def check_company_limit(session: "AsyncSession", company_id: int) -> bool:
        company_limit = await session.scalar(
            select(Company.active_users_limit).where(Company.id == company_id)
        )

        if company_limit is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Компания с указанным ID не найдена",
            )

        active_users_count = await session.scalar(
            select(func.count(User.id)).where(
                User.company_id == company_id, User.is_active
            )
        )

        if active_users_count >= company_limit:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Достигнут лимит активных пользователей для компании",
            )

        return True

    async def generate_password_setup_token(
        self, user_id: int, session: "AsyncSession"
    ) -> str:
        token = secrets.token_urlsafe(32)

        password_token = PasswordSetupToken(token=token, user_id=user_id)
        session.add(password_token)
        await session.commit()

        log.info("Password setup token generated for user %r", user_id)
        return token

    async def send_password_setup_email(
        self, email: str, token: str, user_name: str | None = None
    ):
        setup_url = f"https://ingo360.pro/users/set-password?token={token}"

        html_body = f"""
        <html>
            <body>
                <h2>Добро пожаловать {user_name} {email}!</h2>
                <p>Ваш аккаунт успешно создан.</p>
                <p>Для завершения регистрации, пожалуйста, установите пароль:</p>
                <p><a href="{setup_url}">Установить пароль</a></p>
            </body>
        </html>
        """

        if self.background_tasks:
            self.background_tasks.add_task(
                send_email,
                to_email=email,
                body=html_body,
                subject="Установите пароль для вашего аккаунта",
                is_html=True,
            )
            log.info("Password setup email scheduled for %r", email)
        else:
            log.warning("BackgroundTasks not available, email not sent")

    @staticmethod
    async def validate_password_setup_token(
        token: str, session: "AsyncSession"
    ) -> PasswordSetupToken:
        stmt = select(PasswordSetupToken).where(
            PasswordSetupToken.token == token, not PasswordSetupToken.is_used
        )
        result = await session.execute(stmt)
        token_record = result.scalar_one_or_none()

        if not token_record:
            log.warning("Invalid or already used password setup token")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Неверный или использованный токен",
            )

        return token_record

    async def set_password_by_token(
        self, token: str, new_password: str, session: "AsyncSession"
    ) -> User:
        token_record = await self.validate_password_setup_token(token, session)

        user = await self.get(token_record.user_id)
        if not user:
            log.error("User %r not found for password setup", token_record.user_id)
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Пользователь не найден"
            )

        hashed_password = self.password_helper.hash(new_password)

        update_dict = {"hashed_password": hashed_password, "is_verified": True}

        updated_user = await self.user_db.update(user, update_dict)

        token_record.is_used = True
        await session.commit()

        log.info("Password successfully set for user %r", user.id)
        return updated_user

    async def resend_password_setup(self, email: str, session: "AsyncSession"):
        try:
            user = await self.get_by_email(email)
        except exceptions.UserNotExists:
            log.warning("Password setup resend requested for non-existent email")
            return

        if user.is_verified:
            log.warning("Password setup resend requested for active user %r", user.id)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Пароль уже установлен"
            )

        stmt = (
            update(PasswordSetupToken)
            .where(
                PasswordSetupToken.user_id == user.id,
                PasswordSetupToken.is_used.is_(False),
            )
            .values(is_used=True)
        )
        await session.execute(stmt)
        await session.commit()

        token = await self.generate_password_setup_token(user.id, session)

        await self.send_password_setup_email(email, token)

        log.info("Password setup email resent for user %r", user.id)

    async def on_after_deactivate(self, user: User):
        await self.connect_manager.send_user_deactivation(user.id)
        log.info("Пользователь %r был деактивирован", user.id)

    async def get_all(
        self,
        session: "AsyncSession",
        filters: UserFilter,
        load_options: list[Any] | None = None,
    ):
        stmt = select(User)

        if load_options:
            stmt = stmt.options(*load_options)

        if filters.is_active is not None:
            stmt = stmt.where(User.is_active.is_(filters.is_active))

        if filters.search:
            search_term = f"%{filters.search}%"
            search_conditions = [
                User.first_name.ilike(search_term),
                User.patronymic.ilike(search_term),
                User.last_name.ilike(search_term),
            ]
            stmt = stmt.where(or_(*search_conditions))

        result = await session.execute(stmt)
        return result.scalars().all()

    async def get_clients(
        self,
        session: "AsyncSession",
        filters: UserFilter,
        load_options: list[Any] | None = None,
    ):
        stmt = select(User).where(~User.is_admin, ~User.is_operator, ~User.is_superuser)
        if load_options:
            stmt = stmt.options(*load_options)

        if filters.is_active is not None:
            stmt = stmt.where(User.is_active.is_(filters.is_active))

        if filters.search:
            search_term = f"%{filters.search}%"
            search_conditions = [
                User.first_name.ilike(search_term),
                User.patronymic.ilike(search_term),
                User.last_name.ilike(search_term),
            ]
            stmt = stmt.where(or_(*search_conditions))

        result = await session.execute(stmt)
        return result.scalars().all()
