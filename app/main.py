from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.core.search_service import SearchService
from app.api.routes import search

search_service = SearchService()

@asynccontextmanager
async def lifespan(app: FastAPI):
    search_service.load_data()
    yield

app = FastAPI(
    title="ElasticSearch Clone",
    lifespan=lifespan
)

app.include_router(search.router, prefix="/search", tags=["search"])

app.state.search_service = search_service