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
        "logical_shard": request.app.state.logical_shard_id,
        "replica_id": request.app.state.replica_id,
        "results": [r.model_dump() for r in response.results]
    }

@router.get("/health")
def internal_health_check(request: Request) -> Dict[str, Any]:
    search_service = request.app.state.search_service
    payload = search_service.health_check()
    payload.update(
        {
            "logical_shard": request.app.state.logical_shard_id,
            "replica_id": request.app.state.replica_id
        }
    )
    return payload

@router.get("/ready")
def internal_readiness_check(request: Request):
    """
    Readiness: shard is ready if it has finished loading and indexing data
    For now app.state.ready is set to True only after data is loaded in shard_main.py
    """
    ready = bool(getattr(request.app.state, "is_ready", False))
    if not ready:
        return JSONResponse(
            content={
                "logical_shard": request.app.state.logical_shard_id,
                "replica_id": request.app.state.replica_id,
                "status": "not ready",
            },
            status_code=503,
        )
    
    return {
        "logical_shard": request.app.state.logical_shard_id,
        "replica_id": request.app.state.replica_id,
        "status": "ready"
    }