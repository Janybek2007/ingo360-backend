from typing import TYPE_CHECKING, Annotated

from fastapi import Depends

from src.db.models import AccessToken
from src.db.session import db_session

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


async def get_access_token_db(
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
):
    yield AccessToken.get_db(session)
