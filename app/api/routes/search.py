from fastapi import APIRouter, Request, Depends
from app.models.search_query import SearchQuery
from app.models.api_response import APIResponse, Meta
from app.models.search_response import SearchResponse
from time import perf_counter

router = APIRouter()

@router.get("/", response_model=APIResponse[SearchResponse])
def search(
    request: Request,
    query: SearchQuery = Depends()
    ):
    service = request.app.state.search_service

    start_time = perf_counter()

    results =  service.search(
        query.q,
        page=query.page,
        page_size=query.page_size,
        debug=query.debug
    )

    took_ms = (perf_counter() - start_time) * 1000

    return APIResponse(
        status="ok",
        data=results,
        meta=Meta(
            page=query.page,
            page_size=query.page_size,
            total_hits=results.total_hits,
            took_ms=round(took_ms, 2)
        )
    )

@router.get("/health", response_model=APIResponse)
def health(request: Request):
    service = request.app.state.search_service
    
    return APIResponse(
        status="ok",
        data=service.health_check()
    )