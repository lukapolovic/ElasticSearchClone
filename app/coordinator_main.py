import os
import asyncio
from typing import Any, Dict, List, Tuple, cast, Optional
from fastapi import FastAPI, Query
from contextlib import asynccontextmanager
from fastapi.responses import JSONResponse
import httpx
from time import perf_counter
import uuid

from app.models.api_response import APIResponse, Meta
from app.models.search_response import SearchResult, SearchResponse
from search.nltk_setup import ensure_nltk_data

# COORDINATOR NETWORKING CONSTANTS
CONNECT_TIMEOUT_SEC = 0.5
READ_TIMEOUT_SEC = 1.5
RETRY_ONCE = True

_RETRYABLE_EXCEPTIONS = (
    httpx.ConnectTimeout,
    httpx.ReadTimeout,
    httpx.ConnectError,
    httpx.RemoteProtocolError,
)

def _parse_shard_urls() -> List[str]:
    raw = os.getenv("SHARD_URLS", "http://127.0.0.1:8001,http://127.0.0.1:8002")
    urls = [u.strip().rstrip("/") for u in raw.split(",") if u.strip()]
    return urls

def _parse_shard_groups() -> Dict[int, List[str]]:
    """
    Parse SHARD_GROUPS like:
      "0=http://127.0.0.1:8001,http://127.0.0.1:8003;1=http://127.0.0.1:8002,http://127.0.0.1:8004"
    Returns: {0: [...], 1: [...]}

    Backward compat:
    - if SHARD_GROUPS missing, fallback to SHARD_URLS (treated as one-replica-per-shard in list order)
    """

    raw = os.getenv("SHARD_GROUPS")
    if raw and raw.strip():
        groups: Dict[int, List[str]] = {}
        parts = [p.strip() for p in raw.split(";") if p.strip()]
        for part in parts:
            left, right = part.split("=", 1)
            shard_id = int(left.strip())
            urls = [u.strip().rstrip("/") for u in right.split(",") if u.strip()]
            groups[shard_id] = urls
        return groups
    
    # Fallback to SHARD_URLS
    urls = _parse_shard_urls()
    return {i: [url] for i, url in enumerate(urls)}

async def _post_with_retry(
        client: httpx.AsyncClient,
        url: str,
        payload: Dict[str, Any],
) -> httpx.Response:
    """
    Send a POST request and retry once on network-related exceptions.
    We do NOT retry on HTTP status codes, those are real responses.
    """
    try:
        return await client.post(url, json=payload)
    except _RETRYABLE_EXCEPTIONS as e:
        if not RETRY_ONCE:
            raise e
        return await client.post(url, json=payload)

async def query_shard_group(
        shard_id: int,
        replicas: List[str],
        client: httpx.AsyncClient,
        payload: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Try replicas sequentially until one responds successfully.
    Returns:
    {
        shard_id,
        ok,
        chosen_replica,
        attempts: [...],
        response_json: {...}  (if ok)
    }
    """
    attempts: List[Dict[str, Any]] = []

    for rep in replicas:
        url = f"{rep}/internal/search"
        t0 = perf_counter()
        try:
            resp = await _post_with_retry(client, url, payload)
            elapsed_ms = round((perf_counter() - t0) * 1000, 2)

            attempts.append(
                {
                    "replica": rep,
                    "ok": resp.status_code == 200,
                    "status_code": resp.status_code,
                    "took_ms": elapsed_ms,
                }
            )

            if resp.status_code == 200:
                return {
                    "shard_id": shard_id,
                    "ok": True,
                    "chosen_replica": rep,
                    "attempts": attempts,
                    "response_json": resp.json(),
                }
            
        except Exception as e:
            elapsed_ms = round((perf_counter() - t0) * 1000, 2)
            attempts.append(
                {
                    "replica": rep,
                    "ok": False,
                    "error": type(e).__name__,
                    "took_ms": elapsed_ms,
                }
            )

    return {
        "shard_id": shard_id,
        "ok": False,
        "chosen_replica": None,
        "attempts": attempts,
    }

@asynccontextmanager
async def lifespan(app: FastAPI):
    ensure_nltk_data()
    app.state.shard_groups = _parse_shard_groups()
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
    shard_groups: Dict[int, List[str]] = app.state.shard_groups

    k = page * page_size
    payload = {"q": q, "page": page, "page_size": k, "debug": debug}

    start_time = perf_counter()

    request_id = uuid.uuid4().hex[:12]

    timeout = httpx.Timeout(READ_TIMEOUT_SEC, connect=CONNECT_TIMEOUT_SEC)
    async with httpx.AsyncClient(timeout=timeout) as client:
        tasks = [
            query_shard_group(shard_id, replicas, client, payload)
            for shard_id, replicas in shard_groups.items()
        ]
        shard_call_results = await asyncio.gather(*tasks)

    shard_results: List[Dict[str, Any]] = []
    errors: List[str] = []
    shard_meta : List[Dict[str, Any]] = []

    for r in shard_call_results:
        shard_meta.append(
            {
                "shard_id": r["shard_id"],
                "ok": r["ok"],
                "chosen_replica": r["chosen_replica"],
                "attempts": r["attempts"],
            }
        )

        if not r["ok"]:
            errors.append(f"Shard Group: {r['shard_id']} all replicas failed")
            continue
        
        shard_results.append(r["response_json"])

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

    # TODO: If all shard fail, return 503 instead of empty OK response
    # To be revisited once replication & quorum logic is implemented
    status = "ok" if not errors else "partial"
    return APIResponse(
        status=status,
        data=data,
        meta=Meta(
            page=page,
            page_size=page_size,
            total_hits=total_hits,
            took_ms=took_ms,
            shards=shard_meta,
            request_id=request_id,
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
    Readiness: coordinator is ready only if ALL shards and replications are ready

    We call each shard's /internal/ready endpoint to check readiness.
    If any shard is not ready, we return 503 Service Unavailable.
    """
    shard_groups: Dict[int, List[str]] = app.state.shard_groups

    timeout = httpx.Timeout(1.0, connect=0.5)
    async with httpx.AsyncClient(timeout=timeout) as client:
        not_ready_groups: List[str] = []

        for shard_id, replicas in sorted(shard_groups.items()):
            tasks = [client.get(f"{rep}/internal/ready") for rep in replicas]
            responses = await asyncio.gather(*tasks, return_exceptions=True)

            any_ready = False
            details: List[str] = []

            for rep, resp in zip(replicas, responses):
                if isinstance(resp, Exception):
                    details.append(f"replica {rep} error: {type(resp).__name__}")
                    continue
                r = cast(httpx.Response, resp)
                if r.status_code == 200:
                    any_ready = True
                else:
                    details.append(f"replica {rep} status: {r.status_code}")

            if not any_ready:
                not_ready_groups.append(
                    f"shard_id {shard_id} failed replicas:={details}"
                )

        if not_ready_groups:
            return JSONResponse(
                content={
                    "status": "not ready",
                    "details": not_ready_groups,
                },
                status_code=503,
            )
        
        return JSONResponse(status_code=200, content={"status": "ready"})