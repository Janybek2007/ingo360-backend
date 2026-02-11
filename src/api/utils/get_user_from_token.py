from sqlalchemy import select

from src.db.models import AccessToken, User
from src.db.session import db_session


async def get_user_from_token(token: str) -> User | None:
    async for session in db_session.get_session():
        access_token_db = AccessToken.get_db(session)
        access_token = await access_token_db.get_by_token(token)

        if not access_token:
            return None

        result = await session.execute(
            select(User).where(User.id == access_token.user_id)
        )
        user = result.scalar_one_or_none()

        if not user:
            return None

        return user
