from typing import TYPE_CHECKING, Annotated

from fastapi import Depends
from fastapi_users.authentication.strategy.db import DatabaseStrategy
from sqlalchemy import delete

from src.api.dependencies.access_token import get_access_token_db
from src.core.settings import settings
from src.db.session import db_session
from src.websocket import connection_manager as conn

if TYPE_CHECKING:
    from fastapi_users.authentication.strategy.db import AccessTokenDatabase
    from src.db.models import AccessToken
    from fastapi_users import models


class SingleSessionDatabaseStrategy(DatabaseStrategy):
    """
    Стратегия аутентификации с автоматическим закрытием старых сессий.
    При входе с нового устройства все предыдущие токены пользователя удаляются.
    """

    def __init__(
        self,
        database: "AccessTokenDatabase[AccessToken]",
        lifetime_seconds: int,
        get_session_func,
        connection_manager: conn.ConnectionManager,
    ):
        super().__init__(database=database, lifetime_seconds=lifetime_seconds)
        self.get_session_func = get_session_func
        self.connection_manager = connection_manager

    async def write_token(self, user: "models.UP") -> str:
        await self.connection_manager.send_token_invalidation(user.id)

        await self._delete_user_tokens(user.id)

        return await super().write_token(user)

    async def _delete_user_tokens(self, user_id: int) -> None:
        from src.db.models import AccessToken

        async for session in self.get_session_func():
            stmt = delete(AccessToken).where(AccessToken.user_id == user_id)
            await session.execute(stmt)
            await session.commit()
            break


def get_database_strategy(
    access_token_db: Annotated[
        "AccessTokenDatabase[AccessToken]", Depends(get_access_token_db)
    ],
) -> SingleSessionDatabaseStrategy:
    return SingleSessionDatabaseStrategy(
        database=access_token_db,
        lifetime_seconds=settings.ACCESS_TOKEN_EXPIRE_SECONDS,
        get_session_func=db_session.get_session,
        connection_manager=conn.connection_manager,
    )
