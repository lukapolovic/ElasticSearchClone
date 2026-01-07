import os
import asyncio
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from contextlib import asynccontextmanager
import httpx
from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse

# Router health check constants
ROUTER_HEALTH_INVERVAL_SEC = 1.0
COORD_CONNECT_TIMEOUT_SEC = 0.5
COORD_READ_TIMEOUT_SEC = 0.7


# Router forwarding timeouts (router -> coordinator)
FWD_CONNECT_TIMEOUT_SEC = 0.5
FWD_READ_TIMEOUT_SEC = 2.0

RETRY_NEXT_COORDINATOR_ONCE = True

@dataclass
class CoordState:
    ready: bool = False
    last_seen_ts: Optional[float] = None
    last_rtt_ms: Optional[float] = None
    consecutive_failures: int = 0
    inflight: int = 0
    total_routed: int = 0

def _parse_coordinator_urls() -> List[str]:
    raw = os.getenv("COORDINATOR_URLS", "http://127.0.0.1:9000")
    urls = [u.strip().rstrip("/") for u in raw.split(",") if u.strip()]
    if not urls:
        raise RuntimeError("No COORDINATOR_URLS provided")
    return urls

async def _probe_ready(client: httpx.AsyncClient, base: str) -> Tuple[bool, Optional[float]]:
    t0 = time.perf_counter()
    r = await client.get(f"{base}/ready")
    rtt = round((time.perf_counter() - t0) * 1000.0, 2)
    return r.status_code == 200, rtt

async def _health_loop(app: FastAPI) -> None:
    timeout = httpx.Timeout(COORD_READ_TIMEOUT_SEC, connect=COORD_CONNECT_TIMEOUT_SEC)
    async with httpx.AsyncClient(timeout=timeout) as client:
        while True:
            coords: List[str] = app.state.coordinator_urls
            states: Dict[str, CoordState] = app.state.coord_state

            for c in coords:
                states.setdefault(c, CoordState())

            tasks = [_probe_ready(client, c) for c in coords]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            now = time.time()

            for c, res in zip(coords, results):
                st = states[c]
                if isinstance(res, Exception):
                    st.consecutive_failures += 1
                    st.ready = False
                else:
                    ok, rtt = res # type: ignore
                    st.last_rtt_ms = rtt
                    if ok:
                        st.ready = True
                        st.last_seen_ts = now
                        st.consecutive_failures = 0
                    else:
                        st.ready = False
                        st.consecutive_failures += 1
                    
            await asyncio.sleep(ROUTER_HEALTH_INVERVAL_SEC)
                
def _pick_coordinator_rr(app: FastAPI) -> Optional[str]:
    """
    Round-robin coordinator picker among ready coordinators.
    """
    coords: List[str] = app.state.coordinator_urls
    states: Dict[str, CoordState] = app.state.coord_state

    ready = [c for c in coords if states.get(c, CoordState()).ready]
    if not ready:
        return None
    
    idx = app.state.rr_index % len(ready)
    app.state.rr_index += 1
    return ready[idx]

async def _forward_search(
        client: httpx.AsyncClient,
        base: str,
        q: str,
        page: int,
        page_size: int,
        debug: bool,
        request_id: str
) -> httpx.Response:
    params = {
        "q": q,
        "page": page,
        "page_size": page_size,
        "debug": str(debug).lower(),
    }
    headers = {
        "X-Request-Id": request_id
    }
    return await client.get(f"{base}/search", params=params, headers=headers)

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.coordinator_urls = _parse_coordinator_urls()
    app.state.coord_state = {c: CoordState() for c in app.state.coordinator_urls}
    app.state.rr_index = 0

    task = asyncio.create_task(_health_loop(app))
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

app = FastAPI(title="ElasticSearchClone - Router", lifespan=lifespan)

@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}

@app.get("/ready")
def ready() -> JSONResponse:
    states: Dict[str, CoordState] = app.state.coord_state
    urls: List[str] = app.state.coordinator_urls
    any_ready = any(states.get(u, CoordState()).ready for u in urls)
    if not any_ready:
        return JSONResponse(status_code=503, content={"status": "not ready"})
    return JSONResponse(status_code=200, content={"status": "ready"})

@app.get("/_router/state")
def router_state() -> Dict[str, Any]:
    states: Dict[str, CoordState] = app.state.coord_state
    urls: List[str] = app.state.coordinator_urls

    return {
        "coordinators": [
            {
                "base_url": u,
                "ready": states.get(u, CoordState()).ready,
                "last_seen_ts": states.get(u, CoordState()).last_seen_ts,
                "last_rtt_ms": states.get(u, CoordState()).last_rtt_ms,
                "consecutive_failures": states.get(u, CoordState()).consecutive_failures,
                "inflight": states.get(u, CoordState()).inflight,
                "total_routed": states.get(u, CoordState()).total_routed,
            }
            for u in urls
        ]
    }

@app.get("/search")
async def search(
    request: Request,
    q: str = Query(..., min_length=2, max_length=100),
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=50),
    debug: bool = Query(False)
):
    # Request ID: pass-through from client or generate new
    request_id = request.headers.get("X-Request-Id") or os.urandom(6).hex()

    states: Dict[str, CoordState] = app.state.coord_state

    timeout = httpx.Timeout(FWD_READ_TIMEOUT_SEC, connect=FWD_CONNECT_TIMEOUT_SEC)
    async with httpx.AsyncClient(timeout=timeout) as client:
        first = _pick_coordinator_rr(app)
        if not first:
            return JSONResponse(
                status_code=503, 
                content={"status": "not_ready", "error": "No coordinators are ready"}
            )
        
        candidates = [first]
        if RETRY_NEXT_COORDINATOR_ONCE:
            # Add another ready coordinator as a fallback if it exists
            coords = [c for c in app.state.coordinator_urls if states.get(c, CoordState()).ready]
            for c in coords:
                if c != first:
                    candidates.append(c)
                    break

        last_err: Optional[str] = None

        for base in candidates:
            st = states[base]
            st.inflight += 1
            st.total_routed += 1
            try:
                resp = await _forward_search(client, base, q, page, page_size, debug, request_id)
                # If coordinator is reachable but returns error, we still forward it
                return JSONResponse(
                    status_code=resp.status_code,
                    content=resp.json(),
                    headers={"X-Request-Id": request_id, "X-Router_To": base}
                )
            except Exception as e:
                last_err = f"{type(e).__name__}"
                # mark coordinator as not ready immediately
                st.ready = False
                st.consecutive_failures += 1
            finally:
                st.inflight -= 1

        return JSONResponse(
            status_code=503,
            content={
                "status": "unavailable", 
                "error": f"All routing attempts failed: {last_err}",
                "request_id": request_id
                },
        )