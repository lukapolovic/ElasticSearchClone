#!/usr/bin/env python3
import argparse
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

try:
    import httpx
except ImportError:
    httpx = None  # wait-ready will be disabled if httpx not installed


@dataclass
class Proc:
    name: str
    popen: subprocess.Popen


def _repo_root() -> str:
    # scripts/run_cluster.py -> repo root is parent of scripts/
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _build_shard_groups(host: str, shard_ports: Dict[int, List[int]]) -> str:
    # "0=http://127.0.0.1:8001,http://127.0.0.1:8002;1=http://127.0.0.1:8003"
    parts = []
    for shard_id in sorted(shard_ports.keys()):
        urls = [f"http://{host}:{p}" for p in shard_ports[shard_id]]
        parts.append(f"{shard_id}=" + ",".join(urls))
    return ";".join(parts)


def _spawn_uvicorn(
    name: str,
    app_import: str,
    port: int,
    env_extra: Dict[str, str],
    reload: bool,
    host: str
) -> Proc:
    env = os.environ.copy()
    env.update(env_extra)
    env["PYTHONUNBUFFERED"] = "1"

    # Use python -m uvicorn to ensure we use the same interpreter/venv
    cmd = [
        sys.executable,
        "-m",
        "uvicorn",
        app_import,
        "--port",
        str(port),
        "--host", 
        host,
    ]
    if reload:
        cmd.append("--reload")

    popen = subprocess.Popen(
        cmd,
        cwd=_repo_root(),
        env=env,
    )
    return Proc(name=name, popen=popen)


def _poll_ready(
    coordinator_ports: List[int],
    shard_ports: Dict[int, List[int]],
    host: str,
    timeout_s: float,
) -> None:
    if httpx is None:
        print("wait-ready requested but httpx is not installed; skipping readiness checks.")
        return

    deadline = time.time() + timeout_s

    async def _check_once() -> Tuple[bool, List[str]]:
        problems: List[str] = []
        timeout = httpx.Timeout(1.0, connect=0.5)

        async with httpx.AsyncClient(timeout=timeout) as client:
            for shard_id, ports in sorted(shard_ports.items()):
                any_ready = False
                details: List[str] = []

                for p in ports:
                    url = f"http://{host}:{p}/internal/ready"
                    try:
                        r = await client.get(url)
                        if r.status_code == 200:
                            any_ready = True
                        else:
                            details.append(f"replica_port={p} status={r.status_code}")
                    except Exception as e:
                        details.append(f"replica_port={p} error={type(e).__name__}")

                if not any_ready:
                    problems.append(f"shard_id={shard_id} no replicas ready: {details}")

            # Coordinators: require /ready = 200
            for p in coordinator_ports:
                url = f"http://{host}:{p}/ready"
                try:
                    r = await client.get(url)
                    if r.status_code != 200:
                        problems.append(f"coordinator_port={p} /ready {r.status_code}")
                except Exception as e:
                    problems.append(f"coordinator_port={p} /ready error={type(e).__name__}")

        return (len(problems) == 0), problems

    import asyncio

    while True:
        ok, problems = asyncio.run(_check_once())
        if ok:
            return
        if time.time() > deadline:
            print("Cluster did not become ready within timeout. Problems:")
            for line in problems[:50]:
                print("  -", line)
            raise SystemExit(2)
        time.sleep(0.5)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run local ElasticSearchClone cluster (coordinators + shard replicas).")
    parser.add_argument("--host", default="127.0.0.1", help="Host for URLs (default: 127.0.0.1)")
    parser.add_argument("--shards", type=int, default=2, help="Number of logical shards (default: 2)")
    parser.add_argument("--replicas", type=int, default=1, help="Replicas per shard (default: 1)")
    parser.add_argument("--coordinators", type=int, default=1, help="Number of coordinators (default: 1)")
    parser.add_argument("--coord-port", type=int, default=9000, help="Base port for coordinators (default: 9000)")
    parser.add_argument("--shard-port", type=int, default=8001, help="Base port for shard replicas (default: 8001)")
    parser.add_argument("--reload", action="store_true", help="Run uvicorn with --reload (dev only)")
    parser.add_argument("--wait-ready", action="store_true", help="Wait until /ready endpoints report ready")
    parser.add_argument("--ready-timeout", type=float, default=30.0, help="Seconds to wait for readiness (default: 30)")
    args = parser.parse_args()

    if args.shards < 1:
        print("--shards must be >= 1")
        return 2
    if args.replicas < 1:
        print("--replicas must be >= 1")
        return 2
    if args.coordinators < 1:
        print("--coordinators must be >= 1")
        return 2

    # Assign shard replica ports deterministically:
    # shard_id i gets a block of [base + i*replicas, base + i*replicas + replicas-1]
    shard_ports: Dict[int, List[int]] = {}
    for shard_id in range(args.shards):
        start = args.shard_port + shard_id * args.replicas
        shard_ports[shard_id] = list(range(start, start + args.replicas))

    shard_groups = _build_shard_groups(args.host, shard_ports)

    coordinator_ports = [args.coord_port + i for i in range(args.coordinators)]

    procs: List[Proc] = []

    def _shutdown(*_sig) -> None:
        print("\nShutting down cluster...")
        for proc in reversed(procs):
            if proc.popen.poll() is None:
                try:
                    proc.popen.terminate()
                except Exception:
                    pass
        # give them a moment
        time.sleep(0.8)
        for proc in reversed(procs):
            if proc.popen.poll() is None:
                try:
                    proc.popen.kill()
                except Exception:
                    pass

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    print("Starting shard replicas...")
    for shard_id, ports in sorted(shard_ports.items()):
        for rep_idx, port in enumerate(ports):
            name = f"shard{shard_id}_rep{rep_idx}"
            env = {
                "SHARD_ID": str(shard_id),
                "NUM_SHARDS": str(args.shards),
            }
            procs.append(
                _spawn_uvicorn(
                    name=name,
                    app_import="app.shard_main:app",
                    port=port,
                    env_extra=env,
                    reload=args.reload,
                    host=args.host
                )
            )
            print(f"  - {name} on http://{args.host}:{port}")

    # Small delay so shard processes start binding ports before coordinators
    time.sleep(0.8)

    print("Starting coordinators...")
    for i, port in enumerate(coordinator_ports):
        name = f"coordinator{i}"
        env = {
            "SHARD_GROUPS": shard_groups,
        }
        procs.append(
            _spawn_uvicorn(
                name=name,
                app_import="app.coordinator_main:app",
                port=port,
                env_extra=env,
                reload=args.reload,
                host=args.host
            )
        )
        print(f"  - {name} on http://{args.host}:{port}")

    print("\nTopology:")
    print(f"  shards={args.shards} replicas_per_shard={args.replicas} (total shard nodes={args.shards * args.replicas})")
    print(f"  coordinators={args.coordinators}")
    print(f"  SHARD_GROUPS={shard_groups}")

    if args.wait_ready:
        print("\nWaiting for readiness...")
        _poll_ready(coordinator_ports, shard_ports, args.host, args.ready_timeout)
        print("Cluster is ready.")

    # Keep running until Ctrl+C
    try:
        while True:
            alive: List[Proc] = []
            for proc in procs:
                rc = proc.popen.poll()
                if rc is None:
                    alive.append(proc)
                    continue

                print(f"\nProcess exited: {proc.name} rc={rc}")

                if proc.name.startswith("coordinator"):
                    print("Coordinator exited, shutting down cluster.")
                    _shutdown()
                    return 1
                
                print("Shard process exited, keeping cluster running.")

                procs = alive
                time.sleep(1.0)
    finally:
        _shutdown()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())