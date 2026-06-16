"""Unit tests for :mod:`streaming_feature_store.serving.models`."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from streaming_feature_store.serving.models import (
    FeatureVector,
    ServingConfig,
    UserFeaturesResponse,
)

# ---------------------------------------------------------------------------
# FeatureVector
# ---------------------------------------------------------------------------


def test_feature_vector_defaults_all_zero() -> None:
    fv = FeatureVector()
    assert fv.clicks_5m == 0
    assert fv.revenue_5m == 0.0
    assert fv.distinct_products_1h == 0
    assert fv.avg_purchase_amount_24h == 0.0
    assert set(FeatureVector.model_fields) and len(FeatureVector.model_fields) == 13


def test_feature_vector_coerces_redis_strings() -> None:
    fv = FeatureVector(clicks_5m="3", revenue_5m="59.98", purchases_24h="2")
    assert fv.clicks_5m == 3
    assert isinstance(fv.clicks_5m, int)
    assert fv.revenue_5m == pytest.approx(59.98)
    assert isinstance(fv.revenue_5m, float)
    assert fv.purchases_24h == 2


def test_feature_vector_ignores_unknown_field() -> None:
    # Forward compatibility: a newer writer's field is dropped, not rejected.
    fv = FeatureVector(clicks_5m="1", weird_new_field_5m="x")
    assert fv.clicks_5m == 1
    assert not hasattr(fv, "weird_new_field_5m")


def test_feature_vector_rejects_garbage_value() -> None:
    # A corrupt store is a bug, not a default: validation must surface.
    with pytest.raises(ValidationError):
        FeatureVector(clicks_5m="abc")


# ---------------------------------------------------------------------------
# UserFeaturesResponse
# ---------------------------------------------------------------------------


def test_user_features_response_shape() -> None:
    resp = UserFeaturesResponse(
        user_id="u1", key_present=True, features=FeatureVector(clicks_5m="4")
    )
    assert resp.user_id == "u1"
    assert resp.key_present is True
    assert resp.features.clicks_5m == 4


# ---------------------------------------------------------------------------
# ServingConfig
# ---------------------------------------------------------------------------


def test_serving_config_defaults() -> None:
    cfg = ServingConfig()
    assert cfg.redis_host == "localhost"
    assert cfg.redis_port == 6379
    assert cfg.redis_pool_max_connections == 32
    assert cfg.redis_socket_timeout_seconds == 0.5
    assert cfg.api_host == "0.0.0.0"
    assert cfg.api_port == 8000
    assert cfg.workers == 1
    assert cfg.key_prefix == "feat:user:"


def test_serving_config_key_for() -> None:
    assert ServingConfig().key_for("u-42") == "feat:user:u-42"
    assert ServingConfig(key_prefix="x:").key_for("u-42") == "x:u-42"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"redis_port": -1},
        {"redis_port": 70000},
        {"api_port": 0},
        {"redis_pool_max_connections": 0},
        {"redis_socket_timeout_seconds": 0.0},
        {"workers": 0},
        {"key_prefix": ""},
    ],
)
def test_serving_config_rejects_invalid(kwargs) -> None:
    with pytest.raises(ValidationError):
        ServingConfig(**kwargs)


def test_serving_config_rejects_extra_fields() -> None:
    with pytest.raises(ValidationError):
        ServingConfig(unknown_field=1)
