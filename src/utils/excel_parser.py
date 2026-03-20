from io import BytesIO
from tempfile import SpooledTemporaryFile
from typing import Iterator

import polars as pl
from fastapi import UploadFile


async def save_upload_to_temp(
    file: UploadFile, chunk_size: int = 1024 * 1024
) -> SpooledTemporaryFile:
    temp = SpooledTemporaryFile(max_size=50 * 1024 * 1024)
    while True:
        chunk = await file.read(chunk_size)
        if not chunk:
            break
        temp.write(chunk)
    temp.seek(0)
    return temp


def _clean_excel_dataframe(df: pl.DataFrame, read_as_str: bool = False) -> pl.DataFrame:
    """Векторизованная очистка DataFrame без Python loops"""
    if df.height == 0:
        return df

    # Нормализация имён колонок
    df.columns = [str(col).strip().lower() for col in df.columns]

    # Получаем строковые колонки (Utf8, String, или могут содержать строки)
    string_cols = [
        col
        for col in df.columns
        if df[col].dtype in (pl.Utf8, pl.String) or df[col].dtype == pl.Null
    ]

    # 1. Заменяем "-" на null ТОЛЬКО В СТРОКОВЫХ колонках (vectorized)
    if string_cols:
        df = df.with_columns(
            [
                pl.when(pl.col(col).cast(pl.Utf8) == "-")
                .then(None)
                .otherwise(pl.col(col))
                .alias(col)
                for col in string_cols
            ]
        )

    # 2. Strip для строковых колонок (vectorized)
    if string_cols:
        df = df.with_columns(
            [
                pl.col(col).cast(pl.Utf8).str.strip_chars().alias(col)
                for col in string_cols
            ]
        )

    # 3. Заменяем ошибочные значения ("#N/A", "nan") на null (vectorized, case-insensitive)
    if string_cols:
        df = df.with_columns(
            [
                pl.when(
                    pl.col(col).cast(pl.Utf8).str.to_lowercase().is_in(["#n/a", "nan"])
                )
                .then(None)
                .otherwise(pl.col(col))
                .alias(col)
                for col in string_cols
            ]
        )

    # 4. Конвертируем в строки если нужно
    if read_as_str:
        df = df.with_columns(pl.all().cast(pl.Utf8))
        df = df.with_columns(
            [pl.col(col).str.strip_chars().alias(col) for col in df.columns]
        )

    # 5. Финальная фильтрация: убираем строки, которые полностью null
    df = df.filter(~pl.all_horizontal(pl.col("*").is_null()))

    return df


def iter_excel_records(
    file_obj: SpooledTemporaryFile,
    read_as_str: bool = False,
) -> Iterator[tuple[int, dict]]:
    """Итератор по записям Excel файла с номерами строк"""
    file_obj.seek(0)
    content = file_obj.read()

    df = pl.read_excel(BytesIO(content), engine="calamine")

    if df.height == 0:
        return iter(())

    df = _clean_excel_dataframe(df, read_as_str)

    for idx, row in enumerate(df.iter_rows(named=True), start=2):
        yield idx, row


async def parse_excel_to_df(
    file: UploadFile, read_as_str: bool = False
) -> pl.DataFrame:
    content = await file.read()
    df = pl.read_excel(BytesIO(content), engine="calamine")
    return _clean_excel_dataframe(df, read_as_str)


async def parse_excel_file(file: UploadFile, read_as_str: bool = False) -> list[dict]:
    df = await parse_excel_to_df(file, read_as_str)
    return df.to_dicts()


def get_batch_records(df: pl.DataFrame, batch_size: int = 5000) -> Iterator[list[dict]]:
    total_rows = df.height
    for start in range(0, total_rows, batch_size):
        end = min(start + batch_size, total_rows)
        batch_df = df.slice(start, end - start)
        yield batch_df.to_dicts()
