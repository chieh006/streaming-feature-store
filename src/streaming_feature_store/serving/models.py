"""Pydantic models for the online feature-serving API (design week3_01 §4.2).

This module is infrastructure-free — it imports neither FastAPI nor Redis — so
it stays cheap to import and trivial to unit-test:

* :class:`FeatureVector` — the fixed, fully-typed 13-field online feature
  vector.  The model *is* the parser, the validator, the default-zero
  synthesizer, and the OpenAPI schema (design §2.4 / §2.7).
* :class:`UserFeaturesResponse` — the envelope returned by
  ``GET /v1/features/users/{user_id}``.
* :class:`ServingConfig` — the serving-runtime configuration (design §3.3),
  following the :class:`~streaming_feature_store.sliding.models.SlidingConsumerConfig`
  conventions.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class FeatureVector(BaseModel):
    """The fixed 13-field online feature vector for one user.

    Field names match the Redis hash fields byte-for-byte (design week3_01
    §2.7); integer counts and monetary floats are coerced from the strings
    Redis returns; absent fields take their declared zero defaults — the
    downstream-default-zero contract of week2_02 §2.7.

    Notes
    -----
    ``extra="ignore"`` (not the repo-typical ``extra="forbid"``): on the *read*
    boundary an unknown hash field means a **newer writer**, and the correct
    posture is forward compatibility (drop the field), not rejection — the same
    reasoning as ``BACKWARD`` schema compatibility on the Kafka side (design
    §4.2).
    """

    model_config = ConfigDict(extra="ignore")

    clicks_5m: int = 0
    page_views_5m: int = 0
    purchases_5m: int = 0
    revenue_5m: float = 0.0

    clicks_1h: int = 0
    page_views_1h: int = 0
    purchases_1h: int = 0
    revenue_1h: float = 0.0
    distinct_products_1h: int = 0

    purchases_24h: int = 0
    revenue_24h: float = 0.0
    distinct_products_24h: int = 0
    avg_purchase_amount_24h: float = 0.0


class UserFeaturesResponse(BaseModel):
    """Envelope for ``GET /v1/features/users/{user_id}`` (design week3_01 §2.3).

    Parameters
    ----------
    user_id : str
        The echoed path parameter.
    key_present : bool
        ``False`` when the per-user hash is absent entirely (cold / expired
        user); ``True`` when the hash exists, even if individual fields default
        to zero (design §2.3).
    features : FeatureVector
        The fixed 13-field vector — always fully populated.
    """

    user_id: str
    key_present: bool
    features: FeatureVector


class ServingConfig(BaseModel):
    """Runtime configuration for the serving API (design week3_01 §3.3).

    Parameters
    ----------
    redis_host : str
        Online-store host.  Defaults to ``"localhost"``.
    redis_port : int
        Online-store port.  Defaults to ``6379`` (Compose maps ``6379:6379``).
    redis_pool_max_connections : int
        Per-process connection-pool ceiling.  Defaults to ``32``.
    redis_socket_timeout_seconds : float
        Per-call socket timeout — a slow Redis read should ``503``, not queue.
        Defaults to ``0.5``.
    api_host : str
        uvicorn bind address.  Defaults to ``"0.0.0.0"``.
    api_port : int
        uvicorn bind port.  Defaults to ``8000`` (8081 is Schema Registry).
    workers : int
        uvicorn worker **processes** (design §2.6).  Defaults to ``1``.
    key_prefix : str
        Per-user hash key prefix; must match the ``RedisHashSink`` scheme.
        Defaults to ``"feat:user:"`` and is kept overridable for tests.
    """

    model_config = ConfigDict(extra="forbid")

    redis_host: str = "localhost"
    redis_port: int = Field(default=6379, ge=1, le=65535)
    redis_pool_max_connections: int = Field(default=32, ge=1)
    redis_socket_timeout_seconds: float = Field(default=0.5, gt=0.0)
    api_host: str = "0.0.0.0"
    api_port: int = Field(default=8000, ge=1, le=65535)
    workers: int = Field(default=1, ge=1)
    key_prefix: str = Field(default="feat:user:", min_length=1)

    def key_for(self, user_id: str) -> str:
        """Return the Redis hash key for *user_id*.

        Parameters
        ----------
        user_id : str
            Entity identifier from the request path.

        Returns
        -------
        str
            ``f"{key_prefix}{user_id}"`` — the ``feat:user:{user_id}`` hash the
            ``RedisHashSink`` writes (design §2.4).
        """
        return f"{self.key_prefix}{user_id}"
