import os
from fastapi import FastAPI
from contextlib import asynccontextmanager

from app.core.search_service import SearchService
from app.api.routes import internal_search
from app.api.errors import search_error_handler
from app.core.exceptions import SearchError
from search.nltk_setup import ensure_nltk_data

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default
    
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Readiness impleented for Kubernetes liveness/readiness probes
    app.state.is_ready = False

    ensure_nltk_data()
    shard_id = _env_int("SHARD_ID", 0)
    num_shards = _env_int("NUM_SHARDS", 1)
    replica_id = _env_int("REPLICA_ID", 0)

    search_service = SearchService(shard_id=shard_id, num_shards=num_shards)
    search_service.load_data()

    app.state.search_service = search_service
    app.state.logical_shard_id = shard_id
    app.state.replica_id = replica_id

    # Mark ready only after data is loaded and indexed
    app.state.is_ready = True
    yield

app = FastAPI(
    title="ElasticSearchClone - Shard",
    lifespan=lifespan,
)

app.include_router(internal_search.router, prefix="/internal", tags=["internal"])

app.add_exception_handler(SearchError, search_error_handler)