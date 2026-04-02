SALES_INDICATORS = {
    "продажа",
    "продажи",
    "третичная продажа",
    "третичные продажи",
    "третичнаяпродажа",
    "третичныепродажи",
}

STOCK_INDICATORS = {
    "остаток",
    "остатки",
    "остаток в аптеке",
    "остатки в аптеке",
    "остатоквaптеке",
    "остаткиваптеке",
}

PRIMARY_SALES_INDICATORS = {
    "первичная продажа",
    "первичные продажи",
    "первичныепродажа",
    "первичныепродажи",
}

PRIMARY_STOCK_INDICATORS = {
    "остаток на складе",
    "остатки на складе",
    "остатокнаскладе",
    "остаткинаскладе",
}

SECONDARY_SALES_INDICATORS = {
    "вторичная продажа",
    "вторичные продажи",
    "вторичныепродажа",
    "вторичныепродажи",
}

TERTIARY_SALES_VALUE = "Третичная Продажа"
TERTIARY_STOCK_VALUE = "Остаток в аптеке"
PRIMARY_SALES_VALUE = "Первичные продажи"
PRIMARY_STOCK_VALUE = "Остаток на складе"
SECONDARY_SALES_VALUE = "Вторичные продажи"

TERTIARY_SALES_VALUES = (TERTIARY_SALES_VALUE,)
TERTIARY_STOCK_VALUES = (TERTIARY_STOCK_VALUE,)
PRIMARY_SALES_VALUES = (PRIMARY_SALES_VALUE,)
PRIMARY_STOCK_VALUES = (PRIMARY_STOCK_VALUE,)
SECONDARY_SALES_VALUES = (SECONDARY_SALES_VALUE,)


def normalize_tertiary_indicator(value: str) -> str:
    v = value.lower().strip()
    if v in SALES_INDICATORS:
        return TERTIARY_SALES_VALUE
    if v in STOCK_INDICATORS:
        return TERTIARY_STOCK_VALUE
    return value


def normalize_primary_indicator(value: str) -> str:
    v = value.lower().strip()
    if v in PRIMARY_SALES_INDICATORS:
        return PRIMARY_SALES_VALUE
    if v in PRIMARY_STOCK_INDICATORS:
        return PRIMARY_STOCK_VALUE
    return value


def normalize_secondary_indicator(value: str) -> str:
    v = value.lower().strip()
    if v in SECONDARY_SALES_INDICATORS:
        return SECONDARY_SALES_VALUE
    return v if v else value
