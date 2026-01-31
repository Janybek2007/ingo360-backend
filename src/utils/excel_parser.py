import pandas as pd
from fastapi import UploadFile
from io import BytesIO


async def parse_excel_file(file: UploadFile, read_as_str: bool = False) -> list[dict]:

    content = await file.read()

    if read_as_str:
        df = pd.read_excel(BytesIO(content), sheet_name=0, dtype=str)
    else:
        df = pd.read_excel(BytesIO(content), sheet_name=0)

    df = df.dropna(how='all')
    df = df.where(pd.notna(df), None)
    df = df.replace('-', None)

    df.columns = df.columns.str.strip().str.lower()

    return df.to_dict('records')
