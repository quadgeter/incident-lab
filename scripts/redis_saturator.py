# Custom script meant to saturate redis via key()

import asyncio
import json
import random
import time

from redis.asyncio import Redis

# Constants
REDIS_URL = "redis://localhost:6379/0"
KEYSPACE_SIZE = 50000
VALUE_SIZE = 128
WARMUP_SECONDS = 10
WORKERS = 50
GET_RATIO = 0.8
STEP_SECONDS = 10
QPS_STEPS = [200, 500, 1000, 2000, 3000, 4000]
TOTAL_DURATION_SECONDS = WARMUP_SECONDS + STEP_SECONDS * len(QPS_STEPS)
SEED = 256
VALUE_BYTES = b"x" * VALUE_SIZE
OP_TIMEOUT_S = 0.25

def get_redis():
    global redis
    redis = Redis.from_url(REDIS_URL)
    return redis

async def set_keys():
    for batch_start in range(0, KEYSPACE_SIZE, 1000):
        tasks = []
        for i in range(batch_start, min(batch_start + 1000, KEYSPACE_SIZE)):
            tasks.append(redis.set(f"key:{i}", b"x" * VALUE_SIZE))
        await asyncio.gather(*tasks)

# Helpers

async def timed_op(op_name: str, coro):
    start = time.perf_counter()
    try:
        result = await asyncio.wait_for(coro, timeout=OP_TIMEOUT_S)
        ok = True
        err_type = None
    except asyncio.TimeoutError:
        latency_ms = None
        result = None
        ok = False
        err_type = "timeout"
    except Exception as e:
        result = None
        ok = False
        err_type = type(e).__name__
    latency_ms = (time.perf_counter() - start) * 1000.0
    return (op_name, latency_ms, ok, err_type, result)

def percentile(values, p):
    if not values:
        return None
    values = sorted(values)
    idx = int(p * (len(values) - 1))
    return values[idx]

# Warmup Phase

async def warmup(warmup_qps):
    t0 = time.perf_counter()
    rng = random.Random(SEED)

    for s in range(WARMUP_SECONDS):
        tick_start = time.perf_counter()
        num_gets = int(warmup_qps * GET_RATIO)
        num_sets = warmup_qps - num_gets

        tasks = []

        for i in range(num_gets):
            idx = rng.randrange(KEYSPACE_SIZE)
            tasks.append(timed_op('get', redis.get(f"key:{idx}")))
        
        for i in range(num_sets):
            idx = rng.randrange(KEYSPACE_SIZE)
            tasks.append(timed_op('set', redis.set(f"key:{idx}", VALUE_BYTES)))
        results = await asyncio.gather(*tasks, return_exceptions=False)

        completed_ops = [op for op in results if op[2]]
        gets_ok = len([op for op in completed_ops if op[0] == 'get'])
        sets_ok = len([op for op in completed_ops if op[0] == 'set'])
        num_completed = len(completed_ops)

        errors_ops = [op for op in results if not op[2]]
        gets_err = len([op for op in errors_ops if op[0] == 'get'])
        sets_err = len([op for op in errors_ops if op[0] == 'set'])
        num_errors = len(errors_ops)

        latencies = [op[1] for op in completed_ops if op[1] is not None]
        p95 = percentile(latencies, 0.95)
        p50 = percentile(latencies, 0.50)
        elapsed_ms = (time.perf_counter() - tick_start) * 1000
        burst_qps = num_completed / (elapsed_ms / 1000.0)
        effective_qps = num_completed / 1.0
        behind_ms = 0

        next_tick = t0 + (s + 1) * 1.0
        sleep_for = next_tick - time.perf_counter()
        behind_ms = max(0, -sleep_for*1000)

        output = {
            "phase": "warmup",
            "t": time.perf_counter() - t0,
            "target_qps": warmup_qps,
            "effective_qps": effective_qps,
            "burst_qps": burst_qps,
            "attempted_ops": num_gets + num_sets,
            "ok_ops": num_completed,
            "error_ops": num_errors,
            "gets_ok": gets_ok,
            "sets_ok": sets_ok,
            "gets_err": gets_err,
            "sets_err": sets_err,
            "p50_ms": p50,
            "p95_ms": p95,
            "elapsed_ms": elapsed_ms,
            "behind_ms": behind_ms
        }
        print(json.dumps(output))

        if sleep_for > 0:
            await asyncio.sleep(sleep_for)

#TODO Print Injection start timestamp

#TODO Ramp phase (attack)

#TODO Exit Summary

async def main():
    get_redis()
    await set_keys()
    await warmup(warmup_qps=200)

if __name__ == "__main__":
    asyncio.run(main())
