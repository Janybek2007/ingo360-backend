from datetime import date, datetime, timedelta, timezone


def format_date(value: date | datetime | None) -> str:
    if not value:
        return ""
    if isinstance(value, datetime):
        # для datetime оставляем старую логику
        bishkek_tz = timezone(timedelta(hours=6))
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        bishkek_time = value.astimezone(bishkek_tz)
        return bishkek_time.strftime("%d.%m.%Y %H:%M")
    # для date
    return value.strftime("%d.%m.%Y")
