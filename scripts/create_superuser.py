import asyncio
import contextlib
import logging
import os

from dotenv import load_dotenv
from fastapi import BackgroundTasks
from fastapi_users.exceptions import UserAlreadyExists

from src.api.dependencies.user_manager import get_user_manager
from src.api.dependencies.users import get_user_db
from src.core.auth.user_manager import UserManager
from src.db.session import db_session
from src.schemas.user import UserCreate

load_dotenv(".env.scripts")
logger = logging.getLogger(__name__)


get_async_session_context = contextlib.asynccontextmanager(db_session.get_session)
get_user_db_context = contextlib.asynccontextmanager(get_user_db)
get_user_manager_context = contextlib.asynccontextmanager(get_user_manager)


default_email = os.getenv("SUPERUSER_EMAIL")
default_password = os.getenv("SUPERUSER_PASSWORD")
default_first_name = os.getenv("SUPERUSER_FIRST_NAME")
default_last_name = os.getenv("SUPERUSER_LAST_NAME")
default_is_active = True
default_is_verified = True
default_is_superuser = True
default_is_admin = True


async def create_user(user_manager: UserManager, user_create: UserCreate):
    user = await user_manager.create(user_create=user_create, safe=False)

    return user


async def create_superuser(
    email: str = default_email,
    password: str = default_password,
    first_name: str = default_first_name,
    last_name: str = default_last_name,
    is_active: bool = default_is_active,
    is_verified: bool = default_is_verified,
    is_superuser: bool = default_is_superuser,
    is_admin: bool = default_is_admin,
):
    user_create = UserCreate(
        email=email,
        password=password,
        first_name=first_name,
        last_name=last_name,
        is_active=is_active,
        is_verified=is_verified,
        is_superuser=is_superuser,
        is_admin=is_admin,
    )

    try:
        async with get_async_session_context() as session:
            async with get_user_db_context(session) as user_db:
                async with get_user_manager_context(
                    user_db, background_tasks=BackgroundTasks()
                ) as user_manager:
                    user = await create_user(user_manager, user_create)
                    logger.info("User created %r", user)
                    return user
    except UserAlreadyExists:
        logger.info("User %r already exists", email)
        raise


if __name__ == "__main__":
    asyncio.run(create_superuser())
