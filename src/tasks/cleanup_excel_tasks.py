import asyncio
import contextlib
from datetime import datetime, timedelta
from pathlib import Path

from sqlalchemy import delete, select

from src.celery_app import celery_app
from src.db.models.excel_tasks import ExcelTask, ExcelTaskStatus
from src.db.session import db_session

get_async_session_context = contextlib.asynccontextmanager(db_session.get_session)


async def _cleanup_excel_tasks(days: int) -> dict[str, int]:
    cutoff = datetime.utcnow() - timedelta(days=days)
    deleted_files = 0

    async with get_async_session_context() as session:
        stmt = select(ExcelTask).where(
            ExcelTask.status.in_([ExcelTaskStatus.COMPLETED, ExcelTaskStatus.FAILED]),
            ExcelTask.created_at < cutoff,
        )
        result = await session.execute(stmt)
        tasks = list(result.scalars().all())

        for task in tasks:
            file_path = (task.file_path or "").strip()
            if not file_path:
                continue
            try:
                path = Path(file_path)
                if path.exists():
                    path.unlink()
                    deleted_files += 1
            except Exception:
                continue

        if tasks:
            ids = [task.id for task in tasks]
            await session.execute(delete(ExcelTask).where(ExcelTask.id.in_(ids)))
            await session.commit()

    return {"deleted_records": len(tasks), "deleted_files": deleted_files}


@celery_app.task(bind=True)
def cleanup_excel_tasks(self, days: int = 1) -> dict[str, int]:
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(_cleanup_excel_tasks(days=days))
