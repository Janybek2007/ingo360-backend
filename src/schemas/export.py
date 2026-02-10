from pydantic import BaseModel


class ExportExcelRequest(BaseModel):
    header_map: dict[str, str]
    fields_map: dict[str, str] | None = None
    boolean_map: dict[str, list[str]] | None = None
    custom_map: dict[str, dict[str, str]] | None = None
    file_name: str
