from fastapi import APIRouter, Request

router = APIRouter()

@router.get("/")
def search(request: Request, q: str):
    service = request.app.state.search_service
    return service.search(q)

@router.get("/debug")
def search_debug(request: Request, q: str):
    service = request.app.state.search_service
    return service.search_debug(q)

@router.get("/health")
def health(request: Request):
    service = request.app.state.search_service
    return service.health_check()