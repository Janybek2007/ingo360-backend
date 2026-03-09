import asyncio
import contextlib
import importlib
import json
import os
from pathlib import Path

import redis
from sqlalchemy import select

from src.celery_app import celery_app
from src.core.settings import settings
from src.db.models.excel_tasks import ExcelTask, ExcelTaskStatus, ExcelTaskType
from src.db.session import db_session

get_async_session_context = contextlib.asynccontextmanager(db_session.get_session)

redis_sync = redis.from_url(settings.CELERY_BROKER_URL)


def import_class(path: str):
    module_path, class_name = path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)


def save_import_result(task_id: str, payload: dict) -> str:
    temp_dir = Path("temp")
    temp_dir.mkdir(exist_ok=True)
    file_path = temp_dir / f"task_{task_id}_result.json"
    with file_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, default=str)
    return str(file_path)


def _format_error_message(exc: Exception) -> str:
    raw = str(exc).strip()
    if not raw:
        return exc.__class__.__name__

    first_line = raw.splitlines()[0].strip()
    return first_line[:500]


async def _update_excel_task_result(
    *,
    task_id: str,
    status: ExcelTaskStatus,
    saved_file_path: str,
    error: str | None = None,
) -> None:
    async with get_async_session_context() as session:
        stmt = select(ExcelTask).where(ExcelTask.task_id == task_id)
        result = await session.execute(stmt)
        excel_task = result.scalar_one_or_none()
        if excel_task is None:
            return

        excel_task.status = status
        excel_task.file_path = saved_file_path
        excel_task.error = error
        await session.commit()
        await session.refresh(excel_task)
        return excel_task


def _publish_excel_status(payload: dict) -> None:
    redis_sync.publish("celery_excel_status", json.dumps(payload))


async def create_excel_task_record(
    *,
    task_id: str,
    started_by: int,
    file_path: str,
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


@celery_app.task(bind=True)
def import_sales_task(
    self,
    file_path: str,
    user_id: int,
    service_path: str,
    model_path: str,
    batch_size: int = 2000,
):
    async def _import():
        service_cls = import_class(service_path)
        model_cls = import_class(model_path)

        async with get_async_session_context() as session:
            service = service_cls(model_cls)
            result = await service._import_excel_from_file(
                session=session,
                file_path=file_path,
                user_id=user_id,
                batch_size=batch_size,
            )
            return result

    task_id = self.request.id
    file_name = Path(file_path).name
    if "_" in file_name:
        file_name = file_name.split("_", 1)[1]
    try:
        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(_import())

        full_payload = {
            "file_name": file_name,
            "import_result": result,
        }
        saved_file_path = save_import_result(task_id, full_payload)

        updated_task = loop.run_until_complete(
            _update_excel_task_result(
                task_id=task_id,
                status=ExcelTaskStatus.COMPLETED,
                saved_file_path=saved_file_path,
            )
        )
        payload = {
            "user_id": user_id,
            "task_id": task_id,
            "status": "completed",
            "type": "excel_imported",
            "result": {
                "saved_file_path": saved_file_path,
                "created_at": (
                    updated_task.created_at.isoformat()
                    if updated_task and updated_task.created_at
                    else None
                ),
            },
        }
        _publish_excel_status(payload)
        return {"saved_file_path": saved_file_path}

    except Exception as e:
        error_message = _format_error_message(e)
        full_payload = {
            "message": error_message,
            "file_name": file_name,
            "import_result": None,
        }
        saved_file_path = save_import_result(task_id, full_payload)

        updated_task = loop.run_until_complete(
            _update_excel_task_result(
                task_id=task_id,
                status=ExcelTaskStatus.FAILED,
                saved_file_path=saved_file_path,
                error=error_message,
            )
        )
        payload = {
            "user_id": user_id,
            "task_id": task_id,
            "status": "failed",
            "type": "excel_imported",
            "result": {
                "saved_file_path": saved_file_path,
                "created_at": (
                    updated_task.created_at.isoformat()
                    if updated_task and updated_task.created_at
                    else None
                ),
            },
        }
        _publish_excel_status(payload)
        return {"saved_file_path": saved_file_path}
    finally:
        try:
            file_path_obj = Path(file_path)
            if file_path_obj.exists():
                os.remove(file_path_obj)
                print(f"Файл удален: {file_path}")
        except Exception as e:
            print(f"Ошибка при удалении {file_path}: {e}")
