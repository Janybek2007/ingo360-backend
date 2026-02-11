import re
from dataclasses import dataclass


@dataclass(frozen=True)
class PeriodValues:
    group_by_period: str
    years: list[int] | None = None
    quarters: list[tuple[int, int]] | None = None
    months: list[tuple[int, int]] | None = None
    base_periods: list[str] | None = None


def _parse_year_month(raw: str) -> tuple[int, int] | None:
    parts = raw.split("-")
    if len(parts) != 2:
        return None

    first, second = parts[0].strip(), parts[1].strip()
    if not (first.isdigit() and second.isdigit()):
        return None

    if len(first) == 4:
        year = int(first)
        month = int(second)
    elif len(second) == 4:
        year = int(second)
        month = int(first)
    else:
        year = int(second)
        month = int(first)
        if year < 100:
            year += 2000

    if month < 1 or month > 12:
        return None

    return year, month


def build_period_values(group_by_period: str | None, period_values: list[str] | None):
    if not period_values:
        return None

    group = (group_by_period or "month").strip().lower()

    if group == "year":
        years: list[int] = []
        for value in period_values:
            raw = (value or "").strip()
            if raw.isdigit():
                years.append(int(raw))
        return PeriodValues(
            group_by_period="year", years=years or None, base_periods=period_values
        )

    if group == "quarter":
        quarters: list[tuple[int, int]] = []
        for value in period_values:
            raw = (value or "").strip()
            match = re.match(r"^(quarter|q)-(\d{2,4})-(\d)$", raw)
            if not match:
                continue
            year = int(match.group(2))
            if year < 100:
                year += 2000
            quarter = int(match.group(3))
            quarters.append((year, quarter))
        return PeriodValues(
            group_by_period="quarter",
            quarters=quarters or None,
            base_periods=period_values,
        )

    if group in {"mat", "ytd"}:
        months: list[tuple[int, int]] = []
        for value in period_values:
            raw = (value or "").strip()
            if raw.startswith(f"{group}-"):
                raw = raw[len(group) + 1 :]
            parsed = _parse_year_month(raw)
            if not parsed:
                continue
            months.append(parsed)
        return PeriodValues(
            group_by_period=group,
            months=months or None,
            base_periods=period_values,
        )

    months: list[tuple[int, int]] = []
    for value in period_values:
        raw = (value or "").strip()
        if raw.startswith("month-"):
            raw = raw[len("month-") :]
        parsed = _parse_year_month(raw)
        if not parsed:
            continue
        months.append(parsed)
    return PeriodValues(
        group_by_period="month", months=months or None, base_periods=period_values
    )
