from celery import Celery

from .core.settings import settings

celery_app = Celery(
    "imports", broker=settings.CELERY_BROKER_URL, backend=settings.CELERY_RESULT_BACKEND
)


celery_app.autodiscover_tasks(
    ["src.tasks.sale_imports", "src.tasks.export_excel", "src.tasks.email"]
)
