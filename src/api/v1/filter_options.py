from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.dependencies.current_user import current_active_user
from src.db.models import User
from src.db.session import db_session
from src.schemas.filter_options import (
    GroupedFilterOptionsRequest,
    GroupedFilterOptionsResponse,
)
from src.services.filter_options import ALLOWED_REFERENCES, ALLOWED_SCOPES
from src.services.filter_options import (
    get_grouped_filter_options as get_grouped_filter_options_service,
)

router = APIRouter()


@router.post(
    "/filter-options/grouped",
    response_model=GroupedFilterOptionsResponse,
    dependencies=[Depends(current_active_user)],
)
async def get_grouped_filter_options(
    body: GroupedFilterOptionsRequest,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_active_user)],
):
    include_values = body.references

    invalid = [item for item in include_values if item not in ALLOWED_REFERENCES]
    if invalid:
        raise HTTPException(
            status_code=422,
            detail={"invalid_include": invalid, "allowed": sorted(ALLOWED_REFERENCES)},
        )

    scope = body.scope or "all"
    if scope not in ALLOWED_SCOPES:
        raise HTTPException(
            status_code=422,
            detail={
                "invalid_scope": scope,
                "allowed": sorted(ALLOWED_SCOPES),
            },
        )

    payload = await get_grouped_filter_options_service(
        session=session,
        include_values=include_values,
        scope=scope,
        company_id=current_user.company_id,
        filters=body.filters,
    )

    return GroupedFilterOptionsResponse(**payload)
