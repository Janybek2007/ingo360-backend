from typing import TYPE_CHECKING, Annotated

from fastapi import Depends, HTTPException, status

from .current_user import current_active_user_with_company

if TYPE_CHECKING:
    from src.db.models.users import User


def _check_company_access(user: 'User', permission_attr: str, error_detail: str) -> 'User':
    if user.is_admin or user.is_operator:
        return user

    if not user.company:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail='У пользователя не установлена компания'
        )

    can_access = getattr(user.company, permission_attr)
    if not can_access or not user.company.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=error_detail
        )
    return user


async def can_view_primary_sales(user: Annotated['User', Depends(current_active_user_with_company)]) -> 'User':
    return _check_company_access(
        user,
        'can_primary_sales',
        'У компании нет доступа к первичным продажам'
    )


async def can_view_secondary_sales(user: Annotated['User', Depends(current_active_user_with_company)]) -> 'User':
    return _check_company_access(
        user,
        'can_secondary_sales',
        'У компании нет доступа к вторичным продажам'
    )


async def can_view_tertiary_sales(user: Annotated['User', Depends(current_active_user_with_company)]) -> 'User':
    return _check_company_access(
        user,
        'can_tertiary_sales',
        'У компании нет доступа к третичным продажам'
    )


async def can_view_visits(user: Annotated['User', Depends(current_active_user_with_company)]) -> 'User':
    return _check_company_access(
        user,
        'can_visits',
        'У компании нет доступа к просмотру визитов'
    )


async def can_view_market_analysis(user: Annotated['User', Depends(current_active_user_with_company)]) -> 'User':
    return _check_company_access(
        user,
        'can_market_analysis',
        'У компании нет доступа к анализу рынка'
    )
