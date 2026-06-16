"""Async read adapter over the online store (design week3_01 §4.3).

:class:`RedisFeatureReader` is the single seam between the FastAPI app and the
``feat:user:{user_id}`` Redis hash written by the Week 2
:class:`~streaming_feature_store.sliding.sinks.RedisHashSink`.  One request maps
to exactly one ``HGETALL`` (design §2.4); typing and default-zero synthesis are
delegated to :class:`~streaming_feature_store.serving.models.FeatureVector`
(design §2.3 / §2.7).
"""

from __future__ import annotations

import redis.asyncio as aredis

from streaming_feature_store.serving.models import (
    FeatureVector,
    ServingConfig,
    UserFeaturesResponse,
)


class RedisFeatureReader:
    """Async read adapter over the ``feat:user:{user_id}`` hash (design §2.4).

    Parameters
    ----------
    config : ServingConfig
        Supplies host / port / pool / timeout and the key prefix.
    client : redis.asyncio.Redis or None, optional
        Pre-built client (injected in tests, mirroring the ``RedisHashSink``
        ``client=`` convention).  When ``None`` a client with its own
        connection pool is constructed from *config* (design §2.5).
    """

    def __init__(
        self, config: ServingConfig, client: aredis.Redis | None = None
    ) -> None:
        self._config = config
        self._redis = (
            client
            if client is not None
            else aredis.Redis(
                host=config.redis_host,
                port=config.redis_port,
                max_connections=config.redis_pool_max_connections,
                socket_timeout=config.redis_socket_timeout_seconds,
                decode_responses=True,
            )
        )
        self._closed = False

    async def read(self, user_id: str) -> UserFeaturesResponse:
        """Fetch one user's feature vector (one ``HGETALL``).

        Parameters
        ----------
        user_id : str
            Entity identifier from the request path.

        Returns
        -------
        UserFeaturesResponse
            Always a complete vector.  Absent fields and an absent key both
            materialize as zero-valued features (week2_02 §2.7 / week3_01
            §2.3); ``key_present`` records which case occurred.
        """
        raw = await self._redis.hgetall(self._config.key_for(user_id))
        return UserFeaturesResponse(
            user_id=user_id,
            key_present=bool(raw),
            features=FeatureVector(**raw),
        )

    async def ping(self) -> bool:
        """Readiness probe — ``PING`` the store (design §2.9).

        Returns
        -------
        bool
            ``True`` when Redis answers the ``PING``.
        """
        return bool(await self._redis.ping())

    async def close(self) -> None:
        """Close the client and its pool (idempotent, design §2.5)."""
        if self._closed:
            return
        await self._redis.aclose()
        self._closed = True
