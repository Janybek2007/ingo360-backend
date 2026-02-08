import os
import asyncio
import json
import importlib
import contextlib
from pathlib import Path

import redis

from src.celery_app import celery_app
from src.db.session import db_session
from src.core.settings import settings


get_async_session_context = contextlib.asynccontextmanager(db_session.get_session)

redis_sync = redis.from_url(settings.CELERY_BROKER_URL)


def import_class(path: str):
    module_path, class_name = path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)


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
    try:
        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(_import())

        payload = {
            "user_id": user_id,
            "task_id": task_id,
            "status": "completed",
            "result": result
        }
        redis_sync.publish("celery_tasks_notifications", json.dumps(payload))
        return result

    except Exception as e:
        payload = {
            "user_id": user_id,
            "task_id": task_id,
            "status": "failed",
            "message": str(e)
        }
        redis_sync.publish("celery_tasks_notifications", json.dumps(payload))
        raise
    finally:
        try:
            file_path_obj = Path(file_path)
            if file_path_obj.exists():
                os.remove(file_path_obj)
                print(f"Файл удален: {file_path}")
        except Exception as e:
            print(f"Ошибка при удалении {file_path}: {e}")
