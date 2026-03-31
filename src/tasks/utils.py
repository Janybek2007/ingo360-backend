import contextlib
import importlib
import json

import redis
from sqlalchemy import select

from src.core.settings import settings
from src.db.models.excel_tasks import ExcelTask, ExcelTaskStatus, ExcelTaskType
from src.db.session import db_session

get_async_session_context = contextlib.asynccontextmanager(db_session.get_session)

redis_sync = redis.from_url(settings.CELERY_BROKER_URL, socket_keepalive=True)


def import_class(path: str):
    module_path, class_name = path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)


def format_error_message(exc: Exception) -> str:
    raw = str(exc).strip()
    if not raw:
        return exc.__class__.__name__
    return raw.splitlines()[0].strip()[:2000]


def publish_excel_status(payload: dict) -> None:
    redis_sync.publish("celery_excel_status", json.dumps(payload))


async def update_excel_task_result(
    *,
    task_id: str,
    status: ExcelTaskStatus,
    saved_file_path: str,
    error: str | None = None,
    reset_download: bool = False,
):
    async with get_async_session_context() as session:
        stmt = select(ExcelTask).where(ExcelTask.task_id == task_id)
        result = await session.execute(stmt)
        excel_task = result.scalar_one_or_none()
        if excel_task is None:
            return

        excel_task.status = status
        excel_task.file_path = saved_file_path
        excel_task.error = error
        if reset_download and status == ExcelTaskStatus.COMPLETED:
            excel_task.is_file_download = False
            excel_task.download_started_at = None
            excel_task.download_confirmed_at = None
        await session.commit()
        await session.refresh(excel_task)
        return excel_task


async def create_excel_task_record(
    *,
    task_id: str,
    started_by: int,
    file_path: str = "",
    task_type: ExcelTaskType = ExcelTaskType.IMPORT,
) -> None:
    async with get_async_session_context() as session:
        excel_task = ExcelTask(
            task_type=task_type,
            task_id=task_id,
            status=ExcelTaskStatus.PENDING,
            started_by=started_by,
            file_path=file_path,
            error=None,
        )
        session.add(excel_task)
        await session.commit()
