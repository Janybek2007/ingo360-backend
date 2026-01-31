from typing import TYPE_CHECKING, Annotated

from fastapi import Depends, BackgroundTasks

from src.core.auth.user_manager import UserManager
from src.api.dependencies.users import get_user_db
from src.api.dependencies.password_helper import get_password_helper
from src.websocket.connection_manager import connection_manager


if TYPE_CHECKING:
    from fastapi_users_db_sqlalchemy import SQLAlchemyUserDatabase
    from fastapi_users.password import PasswordHelperProtocol


async def get_user_manager(
        user_db: Annotated['SQLAlchemyUserDatabase', Depends(get_user_db)],
        background_tasks: BackgroundTasks,
        password_helper: Annotated['PasswordHelperProtocol', Depends(get_password_helper)],

):
    yield UserManager(
        user_db,
        background_tasks=background_tasks,
        password_helper=password_helper,
        connect_manager=connection_manager,
    )
