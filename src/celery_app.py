from celery import Celery
from celery.schedules import crontab

from .core.settings import settings

celery_app = Celery(
    "imports", broker=settings.CELERY_BROKER_URL, backend=settings.CELERY_RESULT_BACKEND
)


celery_app.autodiscover_tasks(
    [
        "src.tasks.sale_imports",
        "src.tasks.export_excel",
        "src.tasks.email",
        "src.tasks.cleanup_excel_tasks",
        "src.tasks.import_log_batch_delete",
    ]
)

celery_app.conf.beat_schedule = {
    "cleanup-excel-tasks-daily": {
        "task": "src.tasks.cleanup_excel_tasks.cleanup_excel_tasks",
        "schedule": crontab(minute=0, hour=3),
        "args": (3,),
    }
}
