"""End-to-end integration tests for the online feature-serving API (week3_01 §6).

These tests exercise the *real* read path against a live Redis:

1. Seed-and-serve round trip — write a per-user hash through the real
   :class:`~streaming_feature_store.sliding.sinks.RedisHashSink` (one
   :class:`~streaming_feature_store.sliding.models.SlidingFeatureRecord` per
   resolution), serve it through ``create_app`` against real Redis, and assert
   the exact typed 13-field vector.  This is the writer→store→reader contract
   the unit drift guards protect statically, verified dynamically.
2. TTL expiry semantics — seed, force a 1-second key expiry, and assert the
   read flips to ``key_present: false`` + zeros (feature decay, §2.3).
3. Cold user — a never-seen user is ``200`` with zeros, not ``404`` (§2.3).
4. Readiness — ``/readyz`` is ``200`` with Redis up.

The app is driven in-process via ``TestClient`` (which owns the lifespan and
event loop); the reader builds a *real* ``redis.asyncio`` client to localhost.
Requires a live Redis (``make infra-up``); skipped otherwise.
"""

from __future__ import annotations

import socket
import time
import uuid

import pytest
import redis
from fastapi.testclient import TestClient

from streaming_feature_store.serving.app import create_app
from streaming_feature_store.serving.models import ServingConfig
from streaming_feature_store.sliding.models import (
    SlidingConsumerConfig,
    SlidingFeatureRecord,
    WindowResolution,
)
from streaming_feature_store.sliding.sinks import RedisHashSink

pytestmark = pytest.mark.integration

_HOST = "localhost"
_REDIS_PORT = 6379


def _tcp_open(host: str, port: int, timeout: float = 2.0) -> bool:
    """Return ``True`` if a TCP connection to *host:port* succeeds."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


@pytest.fixture(scope="module")
def live_redis() -> None:
    """Skip the module unless Redis is reachable."""
    if not _tcp_open(_HOST, _REDIS_PORT):
        pytest.skip("serving integration needs live Redis; run 'make infra-up'")


@pytest.fixture
def user_id() -> str:
    """Per-test user namespacing to avoid cross-test key collisions."""
    return f"it-user-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def serving_config() -> ServingConfig:
    return ServingConfig(redis_host=_HOST, redis_port=_REDIS_PORT)


@pytest.fixture
def raw_redis(live_redis):
    """A decode_responses sync client for seeding / inspection / cleanup."""
    client = redis.Redis(host=_HOST, port=_REDIS_PORT, decode_responses=True)
    keys: list[str] = []
    yield client, keys
    for key in keys:
        client.delete(key)
    client.close()


def _seed_full_user(user_id: str) -> None:
    """Write all three resolutions' records for *user_id* via the real sink."""
    config = SlidingConsumerConfig(redis_host=_HOST, redis_port=_REDIS_PORT)
    records = [
        SlidingFeatureRecord(
            user_id=user_id,
            window_resolution=WindowResolution.W_5M_SLIDE_1M,
            click_count=3,
            page_view_count=10,
            purchase_count=1,
            revenue=59.98,
        ),
        SlidingFeatureRecord(
            user_id=user_id,
            window_resolution=WindowResolution.W_1H_SLIDE_5M,
            click_count=12,
            page_view_count=40,
            purchase_count=2,
            revenue=119.96,
            distinct_products=2,
        ),
        SlidingFeatureRecord(
            user_id=user_id,
            window_resolution=WindowResolution.W_24H_SLIDE_1H,
            purchase_count=5,
            revenue=300.0,
            distinct_products=4,
            avg_purchase_amount=60.0,
        ),
    ]
    with RedisHashSink(config) as sink:
        for record in records:
            sink.write(record)


# ---------------------------------------------------------------------------
# 1. Seed-and-serve round trip
# ---------------------------------------------------------------------------


def test_seed_and_serve_round_trip(serving_config, raw_redis, user_id) -> None:
    _, keys = raw_redis
    keys.append(f"feat:user:{user_id}")
    _seed_full_user(user_id)

    with TestClient(create_app(serving_config)) as client:
        resp = client.get(f"/v1/features/users/{user_id}")

    assert resp.status_code == 200
    body = resp.json()
    assert body["user_id"] == user_id
    assert body["key_present"] is True
    assert body["features"] == {
        "clicks_5m": 3,
        "page_views_5m": 10,
        "purchases_5m": 1,
        "revenue_5m": 59.98,
        "clicks_1h": 12,
        "page_views_1h": 40,
        "purchases_1h": 2,
        "revenue_1h": 119.96,
        "distinct_products_1h": 2,
        "purchases_24h": 5,
        "revenue_24h": 300.0,
        "distinct_products_24h": 4,
        "avg_purchase_amount_24h": 60.0,
    }


# ---------------------------------------------------------------------------
# 2. TTL expiry semantics
# ---------------------------------------------------------------------------


def test_ttl_expiry_flips_to_cold(serving_config, raw_redis, user_id) -> None:
    client_redis, keys = raw_redis
    key = f"feat:user:{user_id}"
    keys.append(key)
    _seed_full_user(user_id)
    # Override the per-resolution TTL with a 1-second expiry, then let it lapse.
    client_redis.expire(key, 1)
    time.sleep(1.5)
    assert client_redis.exists(key) == 0

    with TestClient(create_app(serving_config)) as client:
        body = client.get(f"/v1/features/users/{user_id}").json()

    assert body["key_present"] is False
    assert all(value == 0 for value in body["features"].values())


# ---------------------------------------------------------------------------
# 3. Cold user (never seen)
# ---------------------------------------------------------------------------


def test_cold_user_is_200_zeros(serving_config, live_redis, user_id) -> None:
    with TestClient(create_app(serving_config)) as client:
        resp = client.get(f"/v1/features/users/{user_id}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["key_present"] is False
    assert body["features"]["clicks_5m"] == 0


# ---------------------------------------------------------------------------
# 4. Readiness against real Redis
# ---------------------------------------------------------------------------


def test_readyz_ok_with_live_redis(serving_config, live_redis) -> None:
    with TestClient(create_app(serving_config)) as client:
        resp = client.get("/readyz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ready"}
