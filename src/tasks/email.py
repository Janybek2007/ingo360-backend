import logging
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from src.celery_app import celery_app
from src.core.settings import settings

log = logging.getLogger(__name__)


@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def send_email(self, to_email: str, subject: str, body: str, is_html: bool = False):
    try:
        msg = MIMEMultipart("alternative")
        msg["From"] = settings.SMTP_USER
        msg["To"] = to_email
        msg["Subject"] = subject

        content_type = "html" if is_html else "plain"
        msg.attach(MIMEText(body, content_type, "utf-8"))

        # Максимально ослабленный SSL контекст для старых серверов
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

        # Разрешаем все cipher suites и старые протоколы
        context.set_ciphers("DEFAULT:@SECLEVEL=0")
        context.options &= ~ssl.OP_NO_SSLv3
        context.options &= ~ssl.OP_NO_TLSv1
        context.options &= ~ssl.OP_NO_TLSv1_1
        context.minimum_version = ssl.TLSVersion.MINIMUM_SUPPORTED

        with smtplib.SMTP_SSL(
            settings.SMTP_HOST, settings.SMTP_PORT, timeout=60, context=context
        ) as server:
            server.login(settings.SMTP_USER, settings.SMTP_PASSWORD)
            server.send_message(msg)

        log.info(f"Email успешно отправлен на {to_email}")
    except Exception as e:
        log.error(f"Ошибка при отправке email на {to_email}: {e}")
        raise self.retry(exc=e)
