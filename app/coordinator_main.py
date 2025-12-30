import os
import asyncio
from typing import Any, Dict, List, Tuple, cast
from fastapi import FastAPI, Query
from contextlib import asynccontextmanager
from fastapi.responses import JSONResponse
import httpx
from time import perf_counter

from app.models.api_response import APIResponse, Meta
from app.models.search_response import SearchResult, SearchResponse
from search.nltk_setup import ensure_nltk_data

def _parse_shard_urls() -> List[str]:
    raw = os.getenv("SHARD_URLS", "http://127.0.0.1:8001,http://127.0.0.1:8002")
    urls = [u.strip().rstrip("/") for u in raw.split(",") if u.strip()]
    return urls

@asynccontextmanager
async def lifespan(app: FastAPI):
    ensure_nltk_data()
    app.state.shard_urls = _parse_shard_urls()
    yield

app = FastAPI(
    title="ElasticSearchClone - Coordinator",
    lifespan=lifespan,
)

@app.get("/search", response_model=APIResponse[SearchResponse])
async def search(
    q: str = Query(..., min_length=2, max_length=100),
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=50),
    debug: bool = Query(False),
):
    shard_urls: List[str] = app.state.shard_urls

    k = page * page_size

    start_time = perf_counter()

    async with httpx.AsyncClient(timeout=2.0) as client:
        tasks = []
        for base in shard_urls:
            url = f"{base}/internal/search"
            tasks.append(
                client.post(
                    url, 
                    json={"q": q, "page":1, "page_size": k, "debug": True}
                )
            )

        responses = await asyncio.gather(*tasks, return_exceptions=True)

    shard_results: List[Dict[str, Any]] = []
    errors: List[str] = []

    for i, r in enumerate(responses):
        if isinstance(r, Exception):
            errors.append(f"Shard[{i}] error: {type(r).__name__}")
            continue

        resp = cast(httpx.Response, r)

        if resp.status_code != 200:
            errors.append(f"Shard[{i}] status={resp.status_code}")
            continue
        shard_results.append(resp.json())

    total_hits = sum(sr.get("total_hits", 0) for sr in shard_results)

    merged_results: List[Tuple[float, int, Dict[str, Any]]] = []
    for sr in shard_results:
        for item in sr.get("results", []):
            score = float(item.get("score", 0.0))
            doc_id = int(item["doc_id"])
            merged_results.append((score, doc_id, item))

    merged_results.sort(key=lambda x: (-x[0], x[1]))

    took_ms = round((perf_counter() - start_time) * 1000, 2)

    start = (page - 1) * page_size
    end = start + page_size
    page_items = merged_results[start:end]

    results: List[SearchResult] = []
    for score, doc_id, item in page_items:
        results.append(
            SearchResult(
                doc_id=doc_id,
                title=item.get("title", ""),
                director=item.get("director", ""),
                cast=item.get("cast", []),
                year=item.get("year", ""),
                rating=item.get("rating", ""),
                score=score if debug else None,
                explanations=item.get("explanations") if debug else None,
            )
        )

    data = SearchResponse(
        query=q,
        total_hits=total_hits,
        page=page,
        page_size=page_size,
        results=results,
    )

    status = "ok" if not errors else "partial_ok"
    return APIResponse(
        status=status,
        data=data,
        meta=Meta(
            page=page,
            page_size=page_size,
            total_hits=total_hits,
            took_ms=took_ms,
        ),
        error=None,
    )

@app.get("/health")
def health() -> Dict[str, str]:
    # Liveness check always returns healthy
    return {"status": "ok"}

@app.get("/ready")
async def ready() -> JSONResponse:
    """
    Readiness: coordinator is ready only if ALL shards are ready

    We call each shard's /internal/ready endpoint to check readiness.
    If any shard is not ready, we return 503 Service Unavailable.
    """
    shard_urls: List[str] = app.state.shard_urls

    timeout = httpx.Timeout(1.0, connect=0.5)
    async with httpx.AsyncClient(timeout=timeout) as client:
        tasks = []
        for base in shard_urls:
            url = f"{base}/internal/ready"
            tasks.append(client.get(url))

        responses = await asyncio.gather(*tasks, return_exceptions=True)

    not_ready: List[str] = []
    for base, r in zip(shard_urls, responses):
        if isinstance(r, Exception):
            not_ready.append(f"{base} error: {type(r).__name__}")
            continue

        resp = cast(httpx.Response, r)

        if resp.status_code != 200:
            not_ready.append(f"{base} status={resp.status_code}")

    if not_ready:
        return JSONResponse(
            status_code=503,
            content={
                "status": "not_ready", 
                "details": not_ready
            }
        )
    
    return JSONResponse(
        status_code=200,
        content={"status": "ready"}
    )