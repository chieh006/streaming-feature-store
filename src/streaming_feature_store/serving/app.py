"""FastAPI app factory for the online feature-serving API (design week3_01 §4.4).

The app is a thin, read-only adapter from the Redis write contract to HTTP: one
``HGETALL`` per request, a fixed 13-field typed response, and the liveness /
readiness split that makes the Phase 4 Kubernetes deployment a config exercise
(design §2.9).  Clients are injectable via the factory so tests need no live
Redis (design §2.5).
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated

import redis.asyncio as aredis
from fastapi import FastAPI, HTTPException, Path
from fastapi.responses import JSONResponse
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from streaming_feature_store.serving.models import ServingConfig, UserFeaturesResponse
from streaming_feature_store.serving.store import RedisFeatureReader

logger = logging.getLogger(__name__)

# Path-parameter guard: identifiers, hyphens, dots, colons; length-bounded so a
# pathological key never reaches Redis (design §4.4 error surface → 422).
_USER_ID_PATTERN = r"^[\w\-.:]+$"
_USER_ID_MAX_LENGTH = 128

# Hint clients to retry a dependency outage rather than treat it as a bug
# (design §2.9: 503 ≠ 500).
_RETRY_AFTER_HEADER = {"Retry-After": "1"}


def create_app(
    config: ServingConfig | None = None,
    redis_client: aredis.Redis | None = None,
) -> FastAPI:
    """Build the serving app (factory pattern; client injectable for tests).

    Parameters
    ----------
    config : ServingConfig or None, optional
        Serving configuration.  Defaults to :class:`ServingConfig` defaults.
    redis_client : redis.asyncio.Redis or None, optional
        Pre-built async client (injected in tests).  When ``None`` each worker
        process builds its own client + pool inside the lifespan (design §2.5).

    Returns
    -------
    FastAPI
        The configured application with the feature, health, and readiness
        routes wired.
    """
    cfg = config if config is not None else ServingConfig()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        """Own the per-process reader (and its pool) for the app's lifetime."""
        reader = RedisFeatureReader(cfg, client=redis_client)
        app.state.reader = reader
        try:
            yield
        finally:
            await reader.close()

    app = FastAPI(
        title="streaming-feature-store serving API",
        version="1.0.0",
        lifespan=lifespan,
    )

    @app.get("/v1/features/users/{user_id}", response_model=UserFeaturesResponse)
    async def get_user_features(
        user_id: Annotated[
            str,
            Path(min_length=1, max_length=_USER_ID_MAX_LENGTH, pattern=_USER_ID_PATTERN),
        ],
    ) -> UserFeaturesResponse:
        """Serve one user's 13-field feature vector (one ``HGETALL``).

        Parameters
        ----------
        user_id : str
            Entity identifier; validated against :data:`_USER_ID_PATTERN`.

        Returns
        -------
        UserFeaturesResponse
            Always a complete vector — zeros for absent fields / key (§2.3).

        Raises
        ------
        fastapi.HTTPException
            ``503`` when the online store is unreachable or times out (§2.9).
        """
        reader: RedisFeatureReader = app.state.reader
        try:
            return await reader.read(user_id)
        except (RedisConnectionError, RedisTimeoutError) as exc:
            logger.error(f"Redis unavailable serving user_id={user_id!r}: {exc}")
            raise HTTPException(
                status_code=503,
                detail="online store unavailable",
                headers=_RETRY_AFTER_HEADER,
            ) from exc

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        """Liveness probe: the process is up.  Performs no I/O (design §2.9).

        Returns
        -------
        dict of str to str
            ``{"status": "ok"}`` with HTTP 200.
        """
        return {"status": "ok"}

    @app.get("/readyz")
    async def readyz() -> JSONResponse:
        """Readiness probe: ``PING`` the online store (design §2.9).

        Returns
        -------
        fastapi.responses.JSONResponse
            ``200`` with ``{"status": "ready"}`` when Redis answers; ``503``
            with ``{"status": "unavailable"}`` and a ``Retry-After`` hint when
            it does not.
        """
        reader: RedisFeatureReader = app.state.reader
        try:
            await reader.ping()
        except (RedisConnectionError, RedisTimeoutError) as exc:
            logger.error(f"readiness probe failed: {exc}")
            return JSONResponse(
                status_code=503,
                content={"status": "unavailable"},
                headers=_RETRY_AFTER_HEADER,
            )
        return JSONResponse(status_code=200, content={"status": "ready"})

    return app
