from pydantic import BaseModel
from typing import List, Optional, Any

class SearchResult(BaseModel):
    doc_id: int
    title: str
    director: str
    cast: List[str]
    year: str
    rating: str
    score: Optional[float] = 0.0
    explanations: Optional[List[Any]] = None

class SearchResponse(BaseModel):
    query: str
    total_hits: int
    page: int
    page_size: int
    results: List[SearchResult]