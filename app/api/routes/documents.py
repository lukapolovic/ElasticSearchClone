from fastapi import APIRouter
from app.models.document import MovieDocument
from app.core.indexer import indexer

router = APIRouter()

@router.post("/")
def index_document(doc: MovieDocument):
    indexer.build([doc.model_dump()], fields=[
        "title",
        "year",
        "genres",
        "description",
        "cast",
        "director",
        "rating"
    ])
    return {"status": "indexed", "id": doc.id}