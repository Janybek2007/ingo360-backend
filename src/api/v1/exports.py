from datetime import datetime, timedelta
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import FileResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.dependencies.current_user import current_active_user
from src.db.models import User
from src.db.models.excel_tasks import ExcelTask, ExcelTaskStatus, ExcelTaskType
from src.db.session import db_session

router = APIRouter()

DOWNLOAD_IN_PROGRESS_TTL_SECONDS = 120


async def _confirm_download(task_id: int, started_at: datetime) -> None:
    async for session in db_session.get_session():
        stmt = select(ExcelTask).where(ExcelTask.id == task_id).with_for_update()
        result = await session.execute(stmt)
        task = result.scalar_one_or_none()
        if task is None:
            return

        if (
            task.download_confirmed_at is None
            and task.download_started_at == started_at
            and task.is_file_download is False
        ):
            task.download_confirmed_at = datetime.utcnow()
            task.is_file_download = True
            await session.commit()
        return


@router.get("/exports/{task_id}/download")
async def download_export_file(
    task_id: str,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_active_user)],
):
    stmt = (
        select(ExcelTask)
        .where(
            ExcelTask.task_id == task_id,
            ExcelTask.started_by == current_user.id,
            ExcelTask.task_type == ExcelTaskType.EXPORT,
        )
        .with_for_update()
    )
    result = await session.execute(stmt)
    task = result.scalar_one_or_none()

    if task is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Задача не найдена"
        )

    if task.status != ExcelTaskStatus.COMPLETED:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Экспорт ещё не завершён",
        )

    if task.is_file_download or task.download_confirmed_at is not None:
        raise HTTPException(
            status_code=status.HTTP_410_GONE,
            detail="Файл уже был скачан",
        )

    now = datetime.utcnow()
    if (
        task.download_started_at is not None
        and task.download_confirmed_at is None
        and now - task.download_started_at
        < timedelta(seconds=DOWNLOAD_IN_PROGRESS_TTL_SECONDS)
    ):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Скачивание уже выполняется",
        )

    file_path = Path(task.file_path)
    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Файл экспорта не найден",
        )

    task.download_started_at = now
    await session.commit()

    response = FileResponse(
        path=str(file_path),
        filename=file_path.name,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    from starlette.background import BackgroundTask

    response.background = BackgroundTask(_confirm_download, task.id, now)
    return response
