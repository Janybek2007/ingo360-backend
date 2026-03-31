from typing import Annotated

from fastapi import Depends, HTTPException, UploadFile, status


async def validate_excel_file(file: UploadFile) -> UploadFile:
    if not file.filename or not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only Excel files are allowed",
        )
    return file


ExcelFile = Annotated[UploadFile, Depends(validate_excel_file)]
