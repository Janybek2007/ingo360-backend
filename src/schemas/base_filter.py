from typing import Generic, Literal, TypeVar

from pydantic import BaseModel

SortDirection = Literal["ASC", "DESC"]

T = TypeVar("T")


class PaginatedResponse(BaseModel, Generic[T]):
    result: list[T]
    hasPrev: bool
    hasNext: bool
    count: int


class BaseFilter(BaseModel):
    limit: int | None = None
    offset: int = 0
    sort_order: SortDirection | None = None


class BaseReferenceFilter(BaseModel):
    limit: int | None = 500
    offset: int = 0
    sort_order: SortDirection | None = None


class BaseDbFilter(BaseModel):
    limit: int | None = 500
    offset: int = 0
    sort_order: SortDirection | None = None
