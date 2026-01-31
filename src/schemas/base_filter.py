from pydantic import BaseModel


class BaseFilter(BaseModel):
    limit: int = 100
    offset: int = 0
