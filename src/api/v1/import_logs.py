from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.session import db_session
from src.schemas import base_filter
from src.schemas.import_log import ImportLogResponse
from src.api.dependencies.current_user import current_operator_user
from src.services.import_log import import_log_service


router = APIRouter(dependencies=[Depends(current_operator_user)])


@router.get('', response_model=list[ImportLogResponse])
async def get_import_logs(
        session: Annotated['AsyncSession', Depends(db_session.get_session)],
        filters: Annotated[base_filter.BaseFilter, Query()]
):
    return await import_log_service.get_multi(session, filters=filters)


@router.delete('/{import_log_id}')
async def delete_import_log(
        session: Annotated['AsyncSession', Depends(db_session.get_session)],
        import_log_id: int
):
    return await import_log_service.delete(session, import_log_id)
