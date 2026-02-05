from pydantic import BaseModel


class BaseFilter(BaseModel):
    limit: int | None = None
    offset: int = 0
