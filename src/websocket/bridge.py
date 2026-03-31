import json
from pathlib import Path

from redis import asyncio as aioredis
from sqlalchemy import select

from src.core.settings import settings
from src.db.models.excel_tasks import ExcelTask, ExcelTaskStatus
from src.db.session import db_session
from src.services.websocket import get_file_size_bytes

from .connection_manager import connection_manager


def _read_result_from_saved_file(saved_file_path: str | None) -> dict | None:
    if not saved_file_path:
        return None

    path = Path(saved_file_path)
    if not path.exists():
        return None

    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {"value": data}
    except Exception:
        return None


def _remove_saved_file_path(result: dict | None) -> dict | None:
    if not isinstance(result, dict):
        return result

    cleaned = dict(result)
    cleaned.pop("saved_file_path", None)
    return cleaned


async def _can_notify_user(user_id: int, task_id: str, status: str) -> bool:
    async for session in db_session.get_session():
        allowed_statuses = [ExcelTaskStatus.PENDING]
        if status == ExcelTaskStatus.COMPLETED.value:
            allowed_statuses.append(ExcelTaskStatus.COMPLETED)
        elif status == ExcelTaskStatus.FAILED.value:
            allowed_statuses.append(ExcelTaskStatus.FAILED)

        stmt = select(ExcelTask.id).where(
            ExcelTask.task_id == task_id,
            ExcelTask.started_by == user_id,
            ExcelTask.is_deleted.is_(False),
            ExcelTask.status.in_(allowed_statuses),
        )
        result = await session.execute(stmt)
        return result.scalar_one_or_none() is not None

    return False


async def redis_to_ws_bridge():
    redis = aioredis.from_url(settings.CELERY_BROKER_URL, decode_responses=True)
    pubsub = redis.pubsub()
    await pubsub.subscribe("celery_excel_status")

    try:
        async for message in pubsub.listen():
            if message["type"] != "message":
                continue

            try:
                data = json.loads(message["data"])
            except Exception:
                continue

            user_id = data.get("user_id")
            task_id = data.get("task_id")
            status = data.get("status")
            payload_type = data.get("type")
            result = data.get("result")

            if not user_id or not task_id or not status:
                continue

            if not await _can_notify_user(int(user_id), str(task_id), str(status)):
                continue

            if payload_type == "excel_imported":
                saved_file_path = None
                if isinstance(result, dict):
                    saved_file_path = result.get("saved_file_path")
                file_result = _read_result_from_saved_file(saved_file_path)
                if file_result is not None:
                    result = file_result
                else:
                    result = _remove_saved_file_path(result)

            if payload_type == "excel_exported" and isinstance(result, dict):
                saved_file_path = result.get("saved_file_path")
                file_name = result.get("file_name")
                size_bytes = get_file_size_bytes(saved_file_path)
                result = _remove_saved_file_path(result)
                if file_name:
                    result["file_name"] = file_name
                if size_bytes is not None:
                    result = {
                        **result,
                        "file_size_bytes": size_bytes,
                    }

            await connection_manager.notify_users(
                user_ids=[int(user_id)],
                notification_type=payload_type or "task_complete",
                task_id=task_id,
                status=status,
                result=result,
            )
    except Exception:
        pass
    finally:
        await pubsub.unsubscribe("celery_excel_status")
        await redis.close()
