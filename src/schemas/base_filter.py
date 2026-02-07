from typing import Literal

from pydantic import BaseModel

SortDirection = Literal["ASC", "DESC"]


class BaseFilter(BaseModel):
    limit: int | None = None
    offset: int = 0
    sort_order: SortDirection | None = None


class BaseReferenceFilter(BaseModel):
    limit: int | None
    offset: int = 0
    sort_order: SortDirection | None = None


class BaseDbFilter(BaseModel):
    limit: int | None
    offset: int = 0
    sort_order: SortDirection | None = None
