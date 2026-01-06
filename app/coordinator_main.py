import os
import asyncio
from typing import Any, Dict, List, Tuple, cast, Optional
from fastapi import FastAPI, Query
from contextlib import asynccontextmanager
from fastapi.responses import JSONResponse
import httpx
import time
import uuid
from enum import Enum
from dataclasses import dataclass

from app.models.api_response import APIResponse, Meta
from app.models.search_response import SearchResult, SearchResponse
from search.nltk_setup import ensure_nltk_data

# HEARTBEAT CONSTANTS
HEARTBEAT_INTERVAL_SEC = 1.0
SUSPECT_AFTER_FAILURES = 2
DOWN_AFTER_FAILURES = 5

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

class ReplicaStatus(str, Enum):
    UP = "up"
    SUSPECT = "suspect"
    DOWN = "down"

@dataclass
class ReplicaState:
    status: ReplicaStatus = ReplicaStatus.SUSPECT
    consecutive_failures: int = 0
    last_seen_ts: Optional[float] = None
    last_rtt_ms: Optional[float] = None
    ready: bool = False

def _init_membership(shard_groups: Dict[int, List[str]]) -> Dict[str, ReplicaState]:
    membership: Dict[str, ReplicaState] = {}
    for replicas in shard_groups.values():
        for base in replicas:
            membership.setdefault(base, ReplicaState())
    return membership

async def _heartbeat_once(
        client: httpx.AsyncClient,
        base_url: str,
) -> Tuple[bool, Optional[float]]:
    """
    Send a heartbeat request to the given base_url.
    Returns success if /internal/ready returned HTTP 200.
    """
    t0 = time.perf_counter()
    r = await client.get(f"{base_url}/internal/ready")
    rtt_ms = round((time.perf_counter() - t0) * 1000.0, 2)
    return (r.status_code == 200), rtt_ms

async def _heartbeat_loop(app: "FastAPI") -> None:
    """
    Runs forever, sending heartbeat requests to all replicas periodically.
    Updating app.state.membership accordingly.
    """
    timeout = httpx.Timeout(0.6, connect=0.2)
    async with httpx.AsyncClient(timeout=timeout) as client:
        while True:
            shard_groups: Dict[int, List[str]] = app.state.shard_groups
            membership: Dict[str, ReplicaState] = app.state.membership

            for replicas in shard_groups.values():
                for base in replicas:
                    membership.setdefault(base, ReplicaState())

            # Send heartbeats
            tasks = []
            bases: List[str] = []
            for base in membership.keys():
                bases.append(base)
                tasks.append(_heartbeat_once(client, base))

            results = await asyncio.gather(*tasks, return_exceptions=True)
            now = time.time()

            for base, res in zip(bases, results):
                state = membership[base]

                if isinstance(res, Exception):
                    state.consecutive_failures += 1
                    state.ready = False
                else:
                    ready_ok, rtt_ms = cast(Tuple[bool, float], res)
                    if ready_ok:
                        state.consecutive_failures = 0
                        state.last_seen_ts = now
                        state.last_rtt_ms = rtt_ms
                        state.ready = True
                    else:
                        state.consecutive_failures += 1
                        state.ready = False
                        state.last_rtt_ms = rtt_ms
                    
                # Update status based on consecutive failures
                if state.consecutive_failures >= DOWN_AFTER_FAILURES:
                    state.status = ReplicaStatus.DOWN
                elif state.consecutive_failures >= SUSPECT_AFTER_FAILURES:
                    state.status = ReplicaStatus.SUSPECT
                else:
                    state.status = ReplicaStatus.UP

            await asyncio.sleep(HEARTBEAT_INTERVAL_SEC)

def _replica_priority(status: ReplicaStatus) -> int:
    if status == ReplicaStatus.UP:
        return 0
    elif status == ReplicaStatus.SUSPECT:
        return 1
    else:
        return 2

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
        membership: Dict[str, ReplicaState],
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

    # Prefer replicas by their status: UP > SUSPECT > DOWN
    ordered_replicas = sorted(
        replicas,
        key=lambda r: _replica_priority(app.state.membership[r].status)
    )

    for rep in ordered_replicas:
        url = f"{rep}/internal/search"
        t0 = time.perf_counter()
        try:
            resp = await _post_with_retry(client, url, payload)
            elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)

            attempts.append(
                {
                    "replica": rep,
                    "ok": resp.status_code == 200,
                    "status_code": resp.status_code,
                    "took_ms": elapsed_ms,
                    "replica_status": app.state.membership[rep].status.value,
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
            elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)
            attempts.append(
                {
                    "replica": rep,
                    "ok": False,
                    "error": type(e).__name__,
                    "took_ms": elapsed_ms,
                    "replica_status": app.state.membership[rep].status.value,
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
    app.state.membership = _init_membership(app.state.shard_groups)

    hb_task = asyncio.create_task(_heartbeat_loop(app))
    try:
        yield
    finally:
        hb_task.cancel()
        try:
            await hb_task
        except asyncio.CancelledError:
            pass

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
    membership: Dict[str, ReplicaState] = app.state.membership
    payload = {"q": q, "page": page, "page_size": k, "debug": debug}

    start_time = time.perf_counter()

    request_id = uuid.uuid4().hex[:12]

    timeout = httpx.Timeout(READ_TIMEOUT_SEC, connect=CONNECT_TIMEOUT_SEC)
    async with httpx.AsyncClient(timeout=timeout) as client:
        tasks = [
            query_shard_group(shard_id, replicas, membership, client, payload)
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

    took_ms = round((time.perf_counter() - start_time) * 1000, 2)

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
    Readiness: coordinator is ready if at least one replica per shard is ready.

    We do not ping shard directly here, we rely on heartbeat state.
    """
    shard_groups: Dict[int, List[str]] = app.state.shard_groups
    membership: Dict[str, ReplicaState] = app.state.membership

    not_ready_groups: List[str] = []

    for shard_id, replicas in sorted(shard_groups.items()):
        any_ready = False
        details: List[str] = []

        for rep in replicas:
            state = membership.get(rep)

            if state is None:
                details.append(f"{rep} status=unknown ready=False")
                continue

            details.append(f"{rep} status={state.status.value} ready={state.ready} failes={state.consecutive_failures}")

            if state.ready and state.status != ReplicaStatus.DOWN:
                any_ready = True

        if not any_ready:
            not_ready_groups.append(f"shard_id {shard_id} no ready replicas. Details:{details}")

    if not_ready_groups:
        return JSONResponse(
            status_code=503,
            content={
                "status": "not_ready",
                "details": not_ready_groups,
            },
        )
    
    return JSONResponse(status_code=200, content={"status": "ready"})