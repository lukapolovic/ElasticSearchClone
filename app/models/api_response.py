from pydantic import BaseModel
from typing import Generic, TypeVar, Optional
from app.models.error import APIError

T = TypeVar('T')

class Meta(BaseModel):
    page: Optional[int] = None
    page_size: Optional[int] = None
    total_hits: Optional[int] = None
    took_ms: Optional[float] = None

class APIResponse(BaseModel, Generic[T]):
    status: str
    data: Optional[T] = None
    meta: Optional[Meta] = None
    error: Optional[APIError] = None