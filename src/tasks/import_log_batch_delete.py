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
        loop = asyncio.get_event_loop()
        loop.run_until_complete(_delete())
    except Exception as exc:
        raise self.retry(exc=exc, countdown=60)
