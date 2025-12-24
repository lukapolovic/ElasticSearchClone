import os
from fastapi import FastAPI
from contextlib import asynccontextmanager

from app.core.search_service import SearchService
from app.api.routes import search
from app.api.errors import search_error_handler
from app.core.exceptions import SearchError
from search.nltk_setup import ensure_nltk_data

search_service = SearchService()

@asynccontextmanager
async def lifespan(app: FastAPI):
    ensure_nltk_data() 
    search_service.load_data()
    yield

app = FastAPI(
    title="ElasticSearch Clone",
    lifespan=lifespan
)

app.include_router(search.router, prefix="/search", tags=["search"])

app.add_exception_handler(SearchError, search_error_handler)

app.state.search_service = search_service