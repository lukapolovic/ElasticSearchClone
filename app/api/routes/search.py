from fastapi import APIRouter, Request, Depends
from app.models.search_query import SearchQuery

router = APIRouter()

@router.get("/")
def search(
    request: Request,
    query: SearchQuery = Depends()
    ):
    service = request.app.state.search_service
    return service.search(
        query.q,
        page=query.page,
        page_size=query.page_size,
        debug=query.debug
    )

@router.get("/health")
def health(request: Request):
    service = request.app.state.search_service
    return service.health_check()