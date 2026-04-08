import asyncio
import contextlib

from sqlalchemy import text

from src.celery_app import celery_app
from src.db.session import db_session

get_async_session_context = contextlib.asynccontextmanager(db_session.get_session)


@celery_app.task(bind=True, max_retries=3)
def delete_import_log_task(self, import_log_id: int, table_name: str):
    async def _delete():
        async with get_async_session_context() as session:
            if table_name in ("global_doctors", "doctors"):
                # 1. Удаляем visits связанные с doctors этого импорта
                while True:
                    result = await session.execute(
                        text("""
                            DELETE FROM visits
                            WHERE id IN (
                                SELECT v.id FROM visits v
                                JOIN doctors d ON d.id = v.doctor_id
                                JOIN global_doctors gd ON gd.id = d.global_doctor_id
                                WHERE gd.import_log_id = :import_log_id
                                LIMIT 15000
                            )
                        """),
                        {"import_log_id": import_log_id},
                    )
                    await session.commit()
                    if result.rowcount == 0:
                        break
                    await asyncio.sleep(0.1)

                # 2. Удаляем doctors связанные с global_doctors этого импорта
                while True:
                    result = await session.execute(
                        text("""
                            DELETE FROM doctors
                            WHERE id IN (
                                SELECT d.id FROM doctors d
                                JOIN global_doctors gd ON gd.id = d.global_doctor_id
                                WHERE gd.import_log_id = :import_log_id
                                LIMIT 15000
                            )
                        """),
                        {"import_log_id": import_log_id},
                    )
                    await session.commit()
                    if result.rowcount == 0:
                        break
                    await asyncio.sleep(0.1)

                # 3. Удаляем global_doctors
                while True:
                    result = await session.execute(
                        text("""
                            DELETE FROM global_doctors
                            WHERE id IN (
                                SELECT id FROM global_doctors
                                WHERE import_log_id = :import_log_id
                                LIMIT 15000
                            )
                        """),
                        {"import_log_id": import_log_id},
                    )
                    await session.commit()
                    if result.rowcount == 0:
                        break
                    await asyncio.sleep(0.1)

            else:
                while True:
                    result = await session.execute(
                        text(f"""
                            DELETE FROM {table_name}
                            WHERE id IN (
                                SELECT id FROM {table_name}
                                WHERE import_log_id = :import_log_id
                                LIMIT 15000
                            )
                        """),
                        {"import_log_id": import_log_id},
                    )
                    await session.commit()
                    if result.rowcount == 0:
                        break
                    await asyncio.sleep(0.1)

            await session.execute(
                text("DELETE FROM import_logs WHERE id = :id"), {"id": import_log_id}
            )
            await session.commit()

    try:
        asyncio.run(_delete())
    except Exception as exc:
        raise self.retry(exc=exc, countdown=60)
