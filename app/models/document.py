from pydantic import BaseModel
from typing import List

class MovieDocument(BaseModel):
    id: int
    title: str
    year: int
    genres: List[str]
    description: str
    cast: List[str]
    director: str
    rating: float