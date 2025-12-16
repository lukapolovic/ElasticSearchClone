from fastapi import APIRouter, Request

router = APIRouter()

@router.get("/")
def search(request: Request, q: str):
    service = request.app.state.search_service
    return service.search(q)