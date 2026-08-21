"""Live proof of the properties no unit test can reach.

The spec's phase 0 was a week-long throwaway experiment. Building the real
substrate answered its API-ergonomics questions on the way, so what is left is
the part that needs real processes and a real broker — and that is this file.
It boots a redis container and **two independent worker processes** of the same
ASGI host, then measures:

1. **on_commit delivery** — a signal emitted inside a transaction that rolls
   back must never reach a socket; one inside a transaction that commits must.
2. **Cross-worker fan-out** — a socket held by worker A receives a signal
   emitted by worker B, and vice versa. This is the property an in-memory
   channel layer silently fails (``realtime.E001``), and the reason the check
   is an error rather than a warning.
3. **The refetch-storm shape** — a bulk operation emitting 100 signals in one
   transaction: how many frames arrive, in what wall time, at what rate. The
   number is what tells the client library whether debouncing
   ``useSignalInvalidate`` is a nicety or a requirement.

Run:  /Users/apple/Projects/stapel/.venv/bin/python e2e/run_e2e.py
Exit 0 and "E2E PASS" is the gate; the measurements are printed either way.
Needs docker (OrbStack) for redis.
"""
import asyncio
import json
import os
import shutil
import signal
import statistics
import subprocess
import sys
import time
from pathlib import Path

import requests
import websockets

REPO = Path(__file__).resolve().parent.parent
# The driver is a test harness rather than the library; its scratch dir is
# not a runtime knob (declared in CONFIG.MD under the e2e host all the same).
STATE = Path(os.environ.get("REALTIME_E2E_DIR", "/tmp/stapel-realtime-e2e"))  # noqa: CFG001
PY = sys.executable
REDIS_CONTAINER = "stapel-realtime-e2e-redis"
REDIS_PORT = 6399
WORKER_A = 8771
WORKER_B = 8772
STREAM = "e2e:ws:42"
BULK = 100

ENV = {
    **os.environ,
    "DJANGO_SETTINGS_MODULE": "e2e.settings",
    "REALTIME_E2E_DB": str(STATE / "db.sqlite3"),
    "REALTIME_E2E_REDIS": f"redis://127.0.0.1:{REDIS_PORT}/0",
    "PYTHONPATH": f"{REPO}{os.pathsep}{REPO.parent}",
}

FAILURES = []


def step(name):
    print(f"\n--- {name}", flush=True)


def check(condition, message):
    if condition:
        print(f"    OK  {message}", flush=True)
    else:
        print(f"    FAIL {message}", flush=True)
        FAILURES.append(message)


def manage(*args):
    return subprocess.run(
        [PY, str(REPO / "e2e" / "manage.py"), *args],
        cwd=REPO, env=ENV, check=True, capture_output=True, text=True,
    )


def start_redis():
    subprocess.run(["docker", "rm", "-f", REDIS_CONTAINER],
                   capture_output=True, text=True)
    subprocess.run(
        ["docker", "run", "-d", "--name", REDIS_CONTAINER,
         "-p", f"{REDIS_PORT}:6379", "redis:7-alpine"],
        check=True, capture_output=True, text=True,
    )
    for _ in range(60):
        probe = subprocess.run(
            ["docker", "exec", REDIS_CONTAINER, "redis-cli", "ping"],
            capture_output=True, text=True,
        )
        if "PONG" in probe.stdout:
            return
        time.sleep(0.5)
    raise SystemExit("redis did not come up")


def stop_redis():
    subprocess.run(["docker", "rm", "-f", REDIS_CONTAINER],
                   capture_output=True, text=True)


def start_worker(port):
    process = subprocess.Popen(
        [PY, "-m", "uvicorn", "e2e.asgi:application",
         "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
        cwd=REPO, env=ENV,
    )
    for _ in range(80):
        try:
            if requests.get(f"http://127.0.0.1:{port}/health", timeout=1).ok:
                return process
        except requests.RequestException:
            pass
        time.sleep(0.25)
    process.send_signal(signal.SIGTERM)
    raise SystemExit(f"worker on {port} did not come up")


def mint_token():
    """A real JWT from the real provider — the sockets go through G14."""
    script = (
        "import django, os, json;"
        "django.setup();"
        "from django.contrib.auth import get_user_model;"
        "from stapel_core.django.jwt.provider import jwt_provider;"
        "U=get_user_model();"
        "u,_=U.objects.get_or_create(email='e2e@example.com',"
        " defaults={'username':'e2e'});"
        "access,_=jwt_provider.create_tokens(u);"
        "print(json.dumps({'token': access, 'user_id': str(u.pk)}))"
    )
    result = subprocess.run(
        [PY, "-c", script], cwd=REPO, env=ENV, check=True,
        capture_output=True, text=True,
    )
    return json.loads(result.stdout.strip().splitlines()[-1])


def emit(port, payload):
    return requests.post(
        f"http://127.0.0.1:{port}/emit", json=payload, timeout=30
    ).json()


async def open_socket(port, token, origin=None):
    url = f"ws://127.0.0.1:{port}/ws/e2e/42?token={token}"
    return await websockets.connect(
        url, origin=origin or f"http://127.0.0.1:{port}", open_timeout=10
    )


async def next_frame(socket, timeout=10):
    while True:
        raw = json.loads(await asyncio.wait_for(socket.recv(), timeout))
        if raw["type"] not in ("ping", "pong"):
            return raw


async def scenarios(token):
    # ── 1. the socket is a real G14-authenticated socket ────────────────
    step("a socket opens through G14 and gets its welcome")
    socket_a = await open_socket(WORKER_A, token)
    await socket_a.send(json.dumps({"v": 1, "type": "hello", "payload": {}}))
    welcome = await next_frame(socket_a)
    check(welcome["type"] == "welcome", f"welcome received: {welcome}")

    step("an unauthenticated socket is refused before accept (4401)")
    try:
        await websockets.connect(
            f"ws://127.0.0.1:{WORKER_A}/ws/e2e/42",
            origin=f"http://127.0.0.1:{WORKER_A}", open_timeout=10,
        )
        check(False, "an unauthenticated socket was accepted")
    except Exception as exc:  # ConnectionClosedError / InvalidStatus
        check("4401" in str(exc) or "403" in str(exc),
              f"unauthenticated socket refused: {type(exc).__name__} {exc}")

    step("an unlisted origin is refused (guard compares WITH the port)")
    try:
        await websockets.connect(
            f"ws://127.0.0.1:{WORKER_A}/ws/e2e/42?token={token}",
            origin="http://127.0.0.1:9999", open_timeout=10,
        )
        check(False, "an unlisted origin was accepted")
    except Exception as exc:
        check(True, f"unlisted origin refused: {type(exc).__name__}")

    # ── 2. on_commit ────────────────────────────────────────────────────
    step("a rolled-back transaction delivers nothing")
    requests.post(f"http://127.0.0.1:{WORKER_A}/emit-rollback",
                  json={"stream": STREAM}, timeout=30)
    try:
        stray = await next_frame(socket_a, timeout=2)
        check(False, f"a rolled-back signal was delivered: {stray}")
    except asyncio.TimeoutError:
        check(True, "nothing arrived for the rolled-back transaction")

    step("a committed transaction delivers, after the commit")
    started = time.monotonic()
    emit(WORKER_A, {"stream": STREAM, "count": 1, "type": "same-worker"})
    frame = await next_frame(socket_a)
    same_worker_ms = (time.monotonic() - started) * 1000
    check(frame["payload"]["signal"] == "same-worker",
          f"same-worker delivery in {same_worker_ms:.0f} ms")

    # ── 3. cross-worker fan-out ─────────────────────────────────────────
    step("a socket on worker A receives a signal emitted by worker B")
    started = time.monotonic()
    emit(WORKER_B, {"stream": STREAM, "count": 1, "type": "b-to-a"})
    frame = await next_frame(socket_a)
    b_to_a_ms = (time.monotonic() - started) * 1000
    check(frame["payload"]["signal"] == "b-to-a",
          f"B -> A delivery in {b_to_a_ms:.0f} ms")

    step("and the mirror: a socket on worker B receives worker A's signal")
    socket_b = await open_socket(WORKER_B, token)
    await socket_b.send(json.dumps({"v": 1, "type": "hello", "payload": {}}))
    await next_frame(socket_b)
    started = time.monotonic()
    emit(WORKER_A, {"stream": STREAM, "count": 1, "type": "a-to-b"})
    got_a = await next_frame(socket_a)
    got_b = await next_frame(socket_b)
    a_to_b_ms = (time.monotonic() - started) * 1000
    check(got_a["payload"]["signal"] == "a-to-b" and
          got_b["payload"]["signal"] == "a-to-b",
          f"one emit reached BOTH workers' sockets in {a_to_b_ms:.0f} ms")

    step("a signal on another workspace's stream reaches neither socket")
    emit(WORKER_A, {"stream": "e2e:ws:99", "count": 1, "type": "elsewhere"})
    try:
        stray = await next_frame(socket_a, timeout=2)
        check(False, f"cross-workspace leak: {stray}")
    except asyncio.TimeoutError:
        check(True, "no cross-workspace delivery (the scope is in the group name)")

    # ── 4. the refetch-storm shape ──────────────────────────────────────
    step(f"bulk: {BULK} signals in ONE transaction, two sockets watching")
    arrivals_a, arrivals_b = [], []

    async def collect(socket, sink):
        deadline = time.monotonic() + 20
        while len(sink) < BULK and time.monotonic() < deadline:
            try:
                frame = await next_frame(socket, timeout=5)
            except asyncio.TimeoutError:
                break
            if frame["payload"].get("signal") == "bulk":
                sink.append(time.monotonic())

    started = time.monotonic()
    emit_result = emit(WORKER_B, {"stream": STREAM, "count": BULK, "type": "bulk"})
    await asyncio.gather(collect(socket_a, arrivals_a), collect(socket_b, arrivals_b))
    elapsed = time.monotonic() - started

    check(len(arrivals_a) == BULK,
          f"worker-A socket received {len(arrivals_a)}/{BULK} frames")
    check(len(arrivals_b) == BULK,
          f"worker-B socket received {len(arrivals_b)}/{BULK} frames")

    spread = (max(arrivals_a) - min(arrivals_a)) if len(arrivals_a) > 1 else 0.0
    gaps = [b - a for a, b in zip(arrivals_a, arrivals_a[1:])] or [0.0]
    print(f"\n    BULK MEASUREMENT ({BULK} signals, 1 transaction, 2 sockets)")
    print(f"      emit (server-side, inside the transaction): "
          f"{emit_result['seconds'] * 1000:.0f} ms")
    print(f"      emit -> last frame on the wire:             {elapsed * 1000:.0f} ms")
    print(f"      first -> last frame arrival spread:         {spread * 1000:.0f} ms")
    print(f"      effective frame rate:                       "
          f"{(len(arrivals_a) / spread) if spread else float('inf'):.0f} frames/s")
    print(f"      median inter-frame gap:                     "
          f"{statistics.median(gaps) * 1000:.2f} ms")
    print("      frames a debounced client collapses into:   1 refetch")
    print(f"      frames an UNdebounced client would refetch: {len(arrivals_a)}"
          f" x {2} sockets = {len(arrivals_a) * 2} REST calls\n")

    await socket_a.close()
    await socket_b.close()
    return {
        "same_worker_ms": same_worker_ms,
        "b_to_a_ms": b_to_a_ms,
        "a_to_b_ms": a_to_b_ms,
        "bulk_received_a": len(arrivals_a),
        "bulk_received_b": len(arrivals_b),
        "bulk_wall_ms": elapsed * 1000,
        "bulk_spread_ms": spread * 1000,
    }


def main():
    workers = []
    try:
        step("reset state")
        shutil.rmtree(STATE, ignore_errors=True)
        STATE.mkdir(parents=True)

        step("redis (docker)")
        start_redis()

        step("migrate + system checks")
        manage("migrate", "--noinput")
        checks = subprocess.run(
            [PY, str(REPO / "e2e" / "manage.py"), "check"],
            cwd=REPO, env=ENV, capture_output=True, text=True,
        )
        check(checks.returncode == 0,
              f"manage.py check is clean on a redis host: {checks.stdout.strip() or 'no issues'}")

        step("mint a JWT from the real provider")
        identity = mint_token()

        step(f"two worker processes on {WORKER_A} and {WORKER_B}, one redis layer")
        workers = [start_worker(WORKER_A), start_worker(WORKER_B)]

        results = asyncio.run(scenarios(identity["token"]))
        print(json.dumps(results, indent=2))
    finally:
        for process in workers:
            process.send_signal(signal.SIGTERM)
        for process in workers:
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:  # pragma: no cover
                process.kill()
        stop_redis()

    if FAILURES:
        print(f"\nE2E FAIL — {len(FAILURES)} check(s) failed:")
        for failure in FAILURES:
            print(f"  - {failure}")
        raise SystemExit(1)
    print("\nE2E PASS")


if __name__ == "__main__":
    main()
