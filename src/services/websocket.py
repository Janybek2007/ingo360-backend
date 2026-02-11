import json
from pathlib import Path

from sqlalchemy import select

from src.db.models.excel_tasks import ExcelTask, ExcelTaskStatus, ExcelTaskType
from src.db.session import db_session


def get_file_size_bytes(file_path: str | None) -> int | None:
    if not file_path:
        return None

    path = Path(file_path)
    if not path.exists() or not path.is_file():
        return None

    try:
        return path.stat().st_size
    except Exception:
        return None


def _read_import_result(file_path: str | None) -> dict | None:
    if not file_path:
        return None

    path = Path(file_path)
    if not path.exists() or not path.is_file():
        return None

    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {"value": data}
    except Exception:
        return None


async def build_tasks_payload(user_id: int) -> dict:
    async for session in db_session.get_session():
        stmt = (
            select(ExcelTask)
            .where(
                ExcelTask.started_by == user_id,
                ExcelTask.status == ExcelTaskStatus.COMPLETED,
                ExcelTask.is_deleted.is_(False),
                ExcelTask.is_file_download.is_(False),
            )
            .order_by(ExcelTask.created_at.desc())
        )
        result = await session.execute(stmt)
        tasks = result.scalars().all()

        def _get_export_file_name(task: ExcelTask) -> str | None:
            if task.task_type != ExcelTaskType.EXPORT or not task.file_path:
                return None

            name = Path(task.file_path).name
            prefix = f"export_{task.task_id}_"
            if name.startswith(prefix):
                return name[len(prefix) :]
            return name

        return {
            "type": "get_tasks",
            "count": len(tasks),
            "tasks": [
                {
                    "task_id": task.task_id,
                    "id": task.id,
                    "created_at": task.created_at.isoformat(),
                    "status": task.status.value,
                    "task_type": task.task_type.value,
                    "file_name": _get_export_file_name(task),
                    "file_size_bytes": (
                        get_file_size_bytes(task.file_path)
                        if task.task_type == ExcelTaskType.EXPORT
                        else None
                    ),
                    "result": (
                        _read_import_result(task.file_path)
                        if task.task_type == ExcelTaskType.IMPORT
                        else None
                    ),
                }
                for task in tasks
            ],
        }

    return {"type": "tasks", "count": 0, "tasks": []}


async def soft_delete_task(user_id: int, task_id: str) -> bool:
    async for session in db_session.get_session():
        stmt = select(ExcelTask).where(
            ExcelTask.task_id == task_id,
            ExcelTask.started_by == user_id,
            ExcelTask.is_deleted.is_(False),
        )
        result = await session.execute(stmt)
        task = result.scalar_one_or_none()
        if task is None:
            return False

        task.is_deleted = True
        await session.commit()
        return True

    return False
