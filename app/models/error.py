from pydantic import BaseModel
from typing import Optional, Any

class APIError(BaseModel):
    code: str
    message: str
    details: Optional[Any] = None