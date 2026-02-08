from typing import TYPE_CHECKING, Annotated

from fastapi import Depends, HTTPException, status

from src.core.auth.fastapi_users import fastapi_users
from src.db.session import db_session

current_active_user = fastapi_users.current_user(active=True)
current_superuser = fastapi_users.current_user(active=True, superuser=True)

if TYPE_CHECKING:
    from src.db.models.users import User
    from sqlalchemy.ext.asyncio import AsyncSession


async def current_active_user_with_company(
    user: Annotated["User", Depends(current_active_user)],
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
) -> "User":
    await session.refresh(user, ["company"])
    return user


async def current_admin_user(user: Annotated["User", Depends(current_active_user)]):
    if not user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Доступ только админам"
        )
    return user


async def current_operator_user(user: Annotated["User", Depends(current_active_user)]):
    if not user.is_operator:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Доступ только операторам"
        )
    return user


async def current_admin_or_operator_user(
    user: Annotated["User", Depends(current_active_user)],
):
    if not user.is_admin and not user.is_operator:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Доступ только админам или операторам",
        )
    return user
