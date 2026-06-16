"""Unit tests for :mod:`streaming_feature_store.serving.app`.

All tests run against ``create_app(config, redis_client=stub)`` with the
injected :class:`~tests.unit.conftest.StubAsyncRedis` — no live Redis required
(design week3_01 §5).  ``TestClient`` drives the app's lifespan so the reader is
built and torn down around each block.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from streaming_feature_store.serving.app import create_app
from streaming_feature_store.serving.models import ServingConfig

_FULL_HASH = {
    "clicks_5m": "3",
    "page_views_5m": "10",
    "purchases_5m": "1",
    "revenue_5m": "59.98",
    "clicks_1h": "12",
    "page_views_1h": "40",
    "purchases_1h": "2",
    "revenue_1h": "119.96",
    "distinct_products_1h": "2",
    "purchases_24h": "5",
    "revenue_24h": "300.0",
    "distinct_products_24h": "4",
    "avg_purchase_amount_24h": "60.0",
}


def _client(stub) -> TestClient:
    return TestClient(create_app(ServingConfig(), redis_client=stub))


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_full_hash_round_trip(stub_async_redis_cls) -> None:
    with _client(stub_async_redis_cls(_FULL_HASH)) as client:
        resp = client.get("/v1/features/users/u-000042")
    assert resp.status_code == 200
    body = resp.json()
    assert body["user_id"] == "u-000042"
    assert body["key_present"] is True
    feats = body["features"]
    assert feats["clicks_5m"] == 3
    assert feats["revenue_5m"] == pytest.approx(59.98)
    assert feats["avg_purchase_amount_24h"] == pytest.approx(60.0)


def test_sparse_hash_defaults_to_zero(stub_async_redis_cls) -> None:
    with _client(stub_async_redis_cls({"clicks_5m": "3", "revenue_5m": "1.5"})) as client:
        body = client.get("/v1/features/users/u1").json()
    assert body["key_present"] is True
    assert body["features"]["clicks_5m"] == 3
    # The other 11 fields default.
    assert body["features"]["purchases_24h"] == 0
    assert body["features"]["distinct_products_1h"] == 0


def test_missing_key_returns_200_all_zeros(stub_async_redis_cls) -> None:
    with _client(stub_async_redis_cls({})) as client:
        resp = client.get("/v1/features/users/never-seen-user")
    assert resp.status_code == 200
    body = resp.json()
    assert body["key_present"] is False
    assert all(value == 0 for value in body["features"].values())


def test_unknown_field_ignored(stub_async_redis_cls) -> None:
    stub = stub_async_redis_cls({"clicks_5m": "2", "weird_new_field_5m": "x"})
    with _client(stub) as client:
        body = client.get("/v1/features/users/u1").json()
    assert body["features"]["clicks_5m"] == 2
    assert "weird_new_field_5m" not in body["features"]


# ---------------------------------------------------------------------------
# Unhappy paths
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("user_id", ["has space", "a" * 200, "bad@char"])
def test_malformed_user_id_422(stub_async_redis_cls, user_id) -> None:
    with _client(stub_async_redis_cls(_FULL_HASH)) as client:
        resp = client.get(f"/v1/features/users/{user_id}")
    assert resp.status_code == 422


def test_empty_user_id_not_found(stub_async_redis_cls) -> None:
    # An empty path segment does not match the route at all.
    with _client(stub_async_redis_cls(_FULL_HASH)) as client:
        resp = client.get("/v1/features/users/")
    assert resp.status_code == 404


def test_redis_connection_error_503(stub_async_redis_cls) -> None:
    stub = stub_async_redis_cls(fail_with=RedisConnectionError("down"))
    with _client(stub) as client:
        resp = client.get("/v1/features/users/u1")
    assert resp.status_code == 503
    assert resp.headers["Retry-After"] == "1"
    assert resp.json()["detail"] == "online store unavailable"


def test_redis_timeout_503(stub_async_redis_cls) -> None:
    stub = stub_async_redis_cls(fail_with=RedisTimeoutError("slow"))
    with _client(stub) as client:
        resp = client.get("/v1/features/users/u1")
    assert resp.status_code == 503


def test_value_coercion_garbage_field_value(stub_async_redis_cls) -> None:
    # A corrupt store is a bug, not a default: the ValidationError surfaces
    # (TestClient re-raises server exceptions) rather than silently zeroing.
    stub = stub_async_redis_cls({"clicks_5m": "abc"})
    with _client(stub) as client, pytest.raises(ValidationError):
        client.get("/v1/features/users/u1")


# ---------------------------------------------------------------------------
# Health vs readiness
# ---------------------------------------------------------------------------


def test_healthz_no_redis_io(stub_async_redis_cls) -> None:
    # /healthz must not touch Redis even when every call would fail.
    stub = stub_async_redis_cls(fail_with=RedisConnectionError("down"))
    with _client(stub) as client:
        resp = client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_readyz_ok_when_ping_succeeds(stub_async_redis_cls) -> None:
    with _client(stub_async_redis_cls(_FULL_HASH)) as client:
        resp = client.get("/readyz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ready"}


def test_readyz_503_when_ping_fails(stub_async_redis_cls) -> None:
    stub = stub_async_redis_cls(fail_with=RedisConnectionError("down"))
    with _client(stub) as client:
        resp = client.get("/readyz")
    assert resp.status_code == 503
    assert resp.headers["Retry-After"] == "1"
    assert resp.json() == {"status": "unavailable"}


def test_lifespan_closes_reader(stub_async_redis_cls) -> None:
    stub = stub_async_redis_cls(_FULL_HASH)
    with _client(stub) as client:
        client.get("/healthz")
    # Lifespan shutdown must close the per-process client exactly once.
    assert stub.aclose_calls == 1


def test_default_config_used_when_none(stub_async_redis_cls) -> None:
    # create_app(None, client=stub) falls back to ServingConfig() defaults.
    client = TestClient(create_app(redis_client=stub_async_redis_cls({})))
    with client:
        resp = client.get("/v1/features/users/u1")
    assert resp.status_code == 200
