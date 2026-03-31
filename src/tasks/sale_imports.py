import asyncio
import json
import os
from pathlib import Path

from src.celery_app import celery_app
from src.db.models.excel_tasks import ExcelTaskStatus
from src.tasks.utils import (
    format_error_message,
    get_async_session_context,
    import_class,
    publish_excel_status,
    update_excel_task_result,
)


def save_import_result(task_id: str, payload: dict) -> str:
    temp_dir = Path("temp")
    temp_dir.mkdir(exist_ok=True)
    file_path = temp_dir / f"task_{task_id}_result.json"
    with file_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, default=str)
    return str(file_path)


def _extract_import_error(result: object) -> str | None:
    if result is None:
        return "Пустой результат импорта"

    if isinstance(result, dict):
        message = result.get("message")
        error = result.get("error")
        import_result = result.get("import_result")

        if error:
            return str(error)
        if message and import_result is None:
            return str(message)

    return None


@celery_app.task(bind=True)
def import_sales_task(
    self,
    file_path: str,
    user_id: int,
    service_path: str,
    model_path: str,
    batch_size: int = 2000,
):
    task_id = self.request.id
    file_name = Path(file_path).name
    if "_" in file_name:
        file_name = file_name.split("_", 1)[1]

    async def _run():
        from src.db.session import db_session

        await db_session.engine.dispose()

        service_cls = import_class(service_path)
        model_cls = import_class(model_path)

        try:
            async with get_async_session_context() as session:
                service = service_cls(model_cls)
                result = await service._import_excel_from_file(
                    session=session,
                    file_path=file_path,
                    user_id=user_id,
                    batch_size=batch_size,
                )

            error_from_result = _extract_import_error(result)

            if error_from_result:
                error_message = format_error_message(ValueError(error_from_result))
                full_payload = {
                    "message": error_message,
                    "file_name": file_name,
                    "import_result": None,
                }
                saved_file_path = save_import_result(task_id, full_payload)
                updated_task = await update_excel_task_result(
                    task_id=task_id,
                    status=ExcelTaskStatus.FAILED,
                    saved_file_path=saved_file_path,
                    error=error_message,
                )
                return "failed", saved_file_path, updated_task

            full_payload = {
                "file_name": file_name,
                "import_result": result,
            }
            saved_file_path = save_import_result(task_id, full_payload)
            updated_task = await update_excel_task_result(
                task_id=task_id,
                status=ExcelTaskStatus.COMPLETED,
                saved_file_path=saved_file_path,
            )
            return "completed", saved_file_path, updated_task

        except Exception as e:
            error_message = format_error_message(e)
            full_payload = {
                "message": error_message,
                "file_name": file_name,
                "import_result": None,
            }
            saved_file_path = save_import_result(task_id, full_payload)
            updated_task = await update_excel_task_result(
                task_id=task_id,
                status=ExcelTaskStatus.FAILED,
                saved_file_path=saved_file_path,
                error=error_message,
            )
            return "failed", saved_file_path, updated_task

    try:
        status, saved_file_path, updated_task = asyncio.run(_run())
        payload = {
            "user_id": user_id,
            "task_id": task_id,
            "status": status,
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
        publish_excel_status(payload)
        return {"saved_file_path": saved_file_path}
    finally:
        try:
            file_path_obj = Path(file_path)
            if file_path_obj.exists():
                os.remove(file_path_obj)
        except Exception:
            pass
