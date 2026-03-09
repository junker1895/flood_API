from typing import Generic, TypeVar

from pydantic import BaseModel

T = TypeVar("T")


class Meta(BaseModel):
    count: int
    next_cursor: str | None = None


class ListEnvelope(BaseModel, Generic[T]):
    data: list[T]
    meta: Meta


class SingleEnvelope(BaseModel, Generic[T]):
    data: T
