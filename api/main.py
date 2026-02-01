"""
Autonomous Ops Engineer - Example API Service

Run:
    docker compose up --build

Test endpoints:
    curl http://localhost:8000/health
    curl http://localhost:8000/cached/mykey
    curl http://localhost:8000/metrics

Prometheus queries (at http://localhost:9090):

    p95 latency for /cached:
        histogram_quantile(
          0.95,
          sum by (le) (
            rate(http_request_duration_seconds_bucket{endpoint="/cached/{key}"}[1m])
          )
        )

    p95 Redis GET latency:
        histogram_quantile(
          0.95,
          sum by (le) (
            rate(redis_op_duration_seconds_bucket{op="get"}[1m])
          )
        )
"""

import hashlib
import json
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

import redis
from fastapi import FastAPI, Response
from fastapi.responses import JSONResponse
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Histogram,
    generate_latest,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
CACHE_TTL_SECONDS = 60
CPU_WORK_ITERATIONS = 50000  # Tune for ~10-50ms on typical laptop

# ---------------------------------------------------------------------------
# Prometheus Metrics
# ---------------------------------------------------------------------------
HTTP_REQUESTS_TOTAL = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["endpoint", "method", "status"],
)

HTTP_REQUEST_DURATION = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["endpoint", "method"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

REDIS_OP_DURATION = Histogram(
    "redis_op_duration_seconds",
    "Redis operation duration in seconds",
    ["op"],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
)

REDIS_ERRORS_TOTAL = Counter(
    "redis_errors_total",
    "Total Redis errors",
    ["type"],
)

CPU_WORK_DURATION = Histogram(
    "cpu_work_duration_seconds",
    "CPU work duration in seconds",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
)

# ---------------------------------------------------------------------------
# Redis Connection Pool
# ---------------------------------------------------------------------------
redis_pool: Optional[redis.ConnectionPool] = None


def get_redis_client() -> redis.Redis:
    global redis_pool
    if redis_pool is None:
        redis_pool = redis.ConnectionPool(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True,
            socket_timeout=5.0,
            socket_connect_timeout=5.0,
        )
    return redis.Redis(connection_pool=redis_pool)


# ---------------------------------------------------------------------------
# Deterministic CPU Work
# ---------------------------------------------------------------------------
def deterministic_cpu_work(seed: str) -> str:
    """
    Perform deterministic CPU-bound work via repeated SHA256 hashing.
    Returns a hex digest that can be cached.
    """
    start = time.perf_counter()
    data = seed.encode("utf-8")
    for _ in range(CPU_WORK_ITERATIONS):
        data = hashlib.sha256(data).digest()
    duration = time.perf_counter() - start
    CPU_WORK_DURATION.observe(duration)
    return hashlib.sha256(data).hexdigest()[:16]


# ---------------------------------------------------------------------------
# JSON Logging
# ---------------------------------------------------------------------------
def log_request(
    endpoint: str,
    status_code: int,
    latency_ms: float,
    cache_hit: Optional[bool] = None,
    redis_op: Optional[str] = None,
    redis_op_latency_ms: Optional[float] = None,
    error_type: Optional[str] = None,
):
    log_entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "endpoint": endpoint,
        "status_code": status_code,
        "latency_ms": round(latency_ms, 3),
    }
    if cache_hit is not None:
        log_entry["cache_hit"] = cache_hit
    if redis_op is not None:
        log_entry["redis_op"] = redis_op
    if redis_op_latency_ms is not None:
        log_entry["redis_op_latency_ms"] = round(redis_op_latency_ms, 3)
    if error_type is not None:
        log_entry["error_type"] = error_type
    print(json.dumps(log_entry), flush=True)


# ---------------------------------------------------------------------------
# Redis Helper with Metrics
# ---------------------------------------------------------------------------
def redis_get(client: redis.Redis, key: str) -> tuple[Optional[str], float]:
    start = time.perf_counter()
    value = client.get(key)
    duration = time.perf_counter() - start
    REDIS_OP_DURATION.labels(op="get").observe(duration)
    return value, duration * 1000


def redis_set(client: redis.Redis, key: str, value: str, ttl: int) -> float:
    start = time.perf_counter()
    client.setex(key, ttl, value)
    duration = time.perf_counter() - start
    REDIS_OP_DURATION.labels(op="set").observe(duration)
    return duration * 1000


def classify_redis_error(e: Exception) -> str:
    if isinstance(e, redis.TimeoutError):
        return "timeout"
    if isinstance(e, redis.ConnectionError):
        return "connection"
    return "other"


# ---------------------------------------------------------------------------
# FastAPI Application
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: initialize connection pool
    get_redis_client()
    yield
    # Shutdown: close pool
    global redis_pool
    if redis_pool:
        redis_pool.disconnect()


app = FastAPI(title="Autonomous Ops Engineer API", lifespan=lifespan)


@app.get("/health")
async def health():
    """Health check endpoint - does not depend on Redis."""
    start = time.perf_counter()
    response = {"status": "ok"}
    latency_ms = (time.perf_counter() - start) * 1000

    HTTP_REQUESTS_TOTAL.labels(endpoint="/health", method="GET", status="200").inc()
    HTTP_REQUEST_DURATION.labels(endpoint="/health", method="GET").observe(latency_ms / 1000)
    log_request("/health", 200, latency_ms)

    return response


@app.get("/cached/{key}")
async def cached(key: str):
    """
    Cache lookup endpoint.
    - Cache hit: return cached value
    - Cache miss: perform CPU work, cache result, return
    - Redis unavailable: return 503
    """
    start = time.perf_counter()
    endpoint = "/cached/{key}"

    try:
        client = get_redis_client()

        # Try cache lookup
        cached_value, get_latency_ms = redis_get(client, key)

        if cached_value is not None:
            # Cache hit
            latency_ms = (time.perf_counter() - start) * 1000
            HTTP_REQUESTS_TOTAL.labels(endpoint=endpoint, method="GET", status="200").inc()
            HTTP_REQUEST_DURATION.labels(endpoint=endpoint, method="GET").observe(latency_ms / 1000)
            log_request(
                endpoint, 200, latency_ms,
                cache_hit=True, redis_op="GET", redis_op_latency_ms=get_latency_ms
            )
            return {
                "key": key,
                "value": cached_value,
                "cache_hit": True,
            }

        # Cache miss - perform CPU work
        computed_value = deterministic_cpu_work(key)

        # Store in Redis
        set_latency_ms = redis_set(client, key, computed_value, CACHE_TTL_SECONDS)

        latency_ms = (time.perf_counter() - start) * 1000
        HTTP_REQUESTS_TOTAL.labels(endpoint=endpoint, method="GET", status="200").inc()
        HTTP_REQUEST_DURATION.labels(endpoint=endpoint, method="GET").observe(latency_ms / 1000)
        log_request(
            endpoint, 200, latency_ms,
            cache_hit=False, redis_op="SET", redis_op_latency_ms=set_latency_ms
        )
        return {
            "key": key,
            "value": computed_value,
            "cache_hit": False,
        }

    except redis.RedisError as e:
        error_type = classify_redis_error(e)
        REDIS_ERRORS_TOTAL.labels(type=error_type).inc()

        latency_ms = (time.perf_counter() - start) * 1000
        HTTP_REQUESTS_TOTAL.labels(endpoint=endpoint, method="GET", status="503").inc()
        HTTP_REQUEST_DURATION.labels(endpoint=endpoint, method="GET").observe(latency_ms / 1000)
        log_request(endpoint, 503, latency_ms, error_type=error_type)

        return JSONResponse(
            status_code=503,
            content={"error": "Redis unavailable", "type": error_type},
        )


@app.get("/metrics")
async def metrics():
    """Prometheus scrape endpoint."""
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )
