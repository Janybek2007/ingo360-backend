from datetime import datetime

from pydantic import BaseModel, ConfigDict


class ImportLogCreate(BaseModel):
    uploaded_by_id: str
    target_table: str
    records_count: int


class ImportLogUpdate(BaseModel):
    uploaded_by_id: str | None = None
    target_table: str | None = None
    records_count: int | None = None


class ImportLogBase(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    user_first_name: str
    user_last_name: str
    target_table: str
    records_count: int


class ImportLogResponse(ImportLogBase):
    created_at: datetime


class ImportLogFormattedResponse(ImportLogBase):
    created_at: str
