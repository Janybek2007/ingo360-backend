from __future__ import annotations

from io import BytesIO
from typing import Awaitable, Callable, TypeVar
from urllib.parse import quote

from fastapi.responses import StreamingResponse

from src.schemas.export import ExportExcelRequest
from src.utils.export_excel import export_excel

T = TypeVar("T")

RowsGetter = Callable[[], Awaitable[list[T]]]
Serializer = Callable[[T], dict]


def _excel_stream(bytes_: bytes, file_name: str) -> StreamingResponse:
    filename = f"{file_name}.xlsx"
    encoded_filename = quote(filename)
    return StreamingResponse(
        BytesIO(bytes_),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename*=UTF-8''{encoded_filename}"
        },
    )


async def export_excel_response(
    *,
    payload: ExportExcelRequest,
    get_rows: RowsGetter[T],
    serialize: Serializer[T],
) -> StreamingResponse:
    items = await get_rows()
    rows = [serialize(item) for item in items]

    excel_bytes = export_excel(
        rows=rows,
        header_map=payload.header_map,
        fields_map=payload.fields_map,
        boolean_map=payload.boolean_map,
        custom_map=payload.custom_map,
    )
    return _excel_stream(excel_bytes, payload.file_name)
