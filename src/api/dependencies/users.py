from typing import TYPE_CHECKING, Annotated

from fastapi import Depends

from src.db.models import User
from src.db.session import db_session

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


async def get_user_db(
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
):
    yield User.get_db(session)
