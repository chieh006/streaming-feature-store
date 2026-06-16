"""Online feature-serving layer (Week 3 PR #1).

A small, read-only FastAPI service that serves the Week 2 online store: one
``HGETALL feat:user:{user_id}`` per request, returning a fixed, fully-typed
13-field feature vector with default-zero synthesis (design doc
``week3_01_feature_serving_api.md``).

The module is layered like :mod:`streaming_feature_store.sliding`:
:mod:`~streaming_feature_store.serving.models` imports no infrastructure and is
trivially unit-testable; :mod:`~streaming_feature_store.serving.store` wires the
async Redis client; :mod:`~streaming_feature_store.serving.app` wires FastAPI.
"""

from streaming_feature_store.serving.app import create_app
from streaming_feature_store.serving.models import (
    FeatureVector,
    ServingConfig,
    UserFeaturesResponse,
)
from streaming_feature_store.serving.store import RedisFeatureReader

__all__ = [
    "FeatureVector",
    "RedisFeatureReader",
    "ServingConfig",
    "UserFeaturesResponse",
    "create_app",
]
