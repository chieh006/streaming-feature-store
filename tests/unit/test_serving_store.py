"""Unit tests for :mod:`streaming_feature_store.serving.store`."""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from streaming_feature_store.serving.models import ServingConfig
from streaming_feature_store.serving.store import RedisFeatureReader


@pytest.fixture
def config() -> ServingConfig:
    return ServingConfig(redis_host="localhost", redis_port=6379, key_prefix="feat:user:")


def test_read_full_hash_round_trip(config, stub_async_redis_cls) -> None:
    stub = stub_async_redis_cls({"clicks_5m": "3", "revenue_5m": "59.98"})
    reader = RedisFeatureReader(config, client=stub)
    resp = asyncio.run(reader.read("u-42"))
    assert stub.last_key == "feat:user:u-42"
    assert resp.user_id == "u-42"
    assert resp.key_present is True
    assert resp.features.clicks_5m == 3
    assert resp.features.revenue_5m == pytest.approx(59.98)


def test_read_sparse_hash_defaults_to_zero(config, stub_async_redis_cls) -> None:
    stub = stub_async_redis_cls({"clicks_5m": "3"})
    reader = RedisFeatureReader(config, client=stub)
    resp = asyncio.run(reader.read("u-42"))
    assert resp.key_present is True
    assert resp.features.clicks_5m == 3
    assert resp.features.purchases_24h == 0
    assert resp.features.avg_purchase_amount_24h == 0.0


def test_read_missing_key_is_all_zeros(config, stub_async_redis_cls) -> None:
    stub = stub_async_redis_cls({})
    reader = RedisFeatureReader(config, client=stub)
    resp = asyncio.run(reader.read("never-seen"))
    assert resp.key_present is False
    assert resp.features.clicks_5m == 0


def test_ping_returns_true(config, stub_async_redis_cls) -> None:
    reader = RedisFeatureReader(config, client=stub_async_redis_cls())
    assert asyncio.run(reader.ping()) is True


def test_ping_propagates_failure(config, stub_async_redis_cls) -> None:
    stub = stub_async_redis_cls(fail_with=RedisConnectionError("down"))
    reader = RedisFeatureReader(config, client=stub)
    with pytest.raises(RedisConnectionError):
        asyncio.run(reader.ping())


def test_close_is_idempotent(config, stub_async_redis_cls) -> None:
    stub = stub_async_redis_cls()
    reader = RedisFeatureReader(config, client=stub)

    async def _close_twice() -> None:
        await reader.close()
        await reader.close()

    asyncio.run(_close_twice())
    assert stub.aclose_calls == 1


def test_builds_default_client_from_config(config) -> None:
    with patch("streaming_feature_store.serving.store.aredis.Redis") as redis_cls:
        RedisFeatureReader(config)
    redis_cls.assert_called_once_with(
        host="localhost",
        port=6379,
        max_connections=config.redis_pool_max_connections,
        socket_timeout=config.redis_socket_timeout_seconds,
        decode_responses=True,
    )
