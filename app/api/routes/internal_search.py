from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Any, Dict

router = APIRouter()

class InternalSearchRequest(BaseModel):
    q: str
    page: int = 1
    page_size: int = 10
    debug: bool = False

@router.post("/search")
def internal_search(request: Request, body: InternalSearchRequest) -> Dict[str, Any]:
    search_service = request.app.state.search_service

    # IMPORTANT: Coordinator needs scores for merging results from shards
    # debug=True is forced here to include scores in the response
    response = search_service.search(
        query=body.q,
        page=body.page,
        page_size=body.page_size,
        debug=True
    )

    return {
        "query": response.query,
        "total_hits": response.total_hits,
        "page": response.page,
        "page_size": response.page_size,
        "results": [r.model_dump() for r in response.results]
    }

@router.get("/health")
def internal_health_check(request: Request) -> Dict[str, Any]:
    search_service = request.app.state.search_service
    return search_service.health_check()

@router.get("/ready")
def internal_readiness_check(request: Request):
    #Readiness controlled by shard_main.py
    is_ready = bool(getattr(request.app.state, "is_ready", False))

    if not is_ready:
        # Return 503 Service Unavailable if not ready
        return JSONResponse(
            status_code=503,
            content={"detail": "not_ready"}
        )
    
    return {"detail": "ready"}