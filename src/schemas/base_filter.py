from typing import Literal

from pydantic import BaseModel

SortDirection = Literal["ASC", "DESC"]


class BaseFilter(BaseModel):
    limit: int | None = None
    offset: int = 0
