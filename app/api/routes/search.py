from fastapi import APIRouter
from app.models.search import SearchRequest
from app.core.indexer import engine

router = APIRouter()

@router.post("/")
def search(req: SearchRequest):
    return engine.search(req.query)