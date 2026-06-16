"""CLI entry point for the online feature-serving API (design week3_01 §4.5).

Maps ``argparse`` flags (matching the repo CLI conventions) to a
:class:`~streaming_feature_store.serving.models.ServingConfig`, then launches
uvicorn.

Scaling is by worker **processes**, not threads (design §2.6).  With
``--workers 1`` (the laptop default) the built app object is handed straight to
uvicorn.  With ``--workers > 1`` uvicorn requires an import string so it can
fork workers that each build their own app, event loop, and connection pool;
the per-process config is carried to those workers through environment
variables and rebuilt by :func:`app_factory`.

``print()`` is acceptable in this CLI layer only (CLAUDE.md §5); the app itself
logs with the :mod:`logging` module and f-strings.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

import uvicorn
from fastapi import FastAPI

from streaming_feature_store.serving.app import create_app
from streaming_feature_store.serving.models import ServingConfig

logger = logging.getLogger(__name__)

# Import string uvicorn uses to (re)build the app inside each forked worker.
_FACTORY_TARGET = "scripts.run_feature_api:app_factory"

# Repo root — added to the workers' import path so the ``scripts`` namespace
# package resolves in spawned worker subprocesses (running a script directly
# only puts the script's own directory on ``sys.path``).
_REPO_ROOT = Path(__file__).resolve().parent.parent

# Environment-variable names carrying per-process config to forked workers.
_ENV_REDIS_HOST = "SFS_SERVING_REDIS_HOST"
_ENV_REDIS_PORT = "SFS_SERVING_REDIS_PORT"
_ENV_POOL_MAX = "SFS_SERVING_REDIS_POOL_MAX_CONNECTIONS"
_ENV_SOCKET_TIMEOUT = "SFS_SERVING_REDIS_SOCKET_TIMEOUT_SECONDS"
_ENV_KEY_PREFIX = "SFS_SERVING_KEY_PREFIX"


def _build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser.

    Returns
    -------
    argparse.ArgumentParser
        Configured parser (design §7.4).
    """
    parser = argparse.ArgumentParser(description="Run the feature-serving API.")
    parser.add_argument("--redis-host", default="localhost")
    parser.add_argument("--redis-port", type=int, default=6379)
    parser.add_argument("--host", dest="api_host", default="0.0.0.0")
    parser.add_argument("--port", dest="api_port", type=int, default=8000)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--log-level", default="info")
    return parser


def _config_from_args(args: argparse.Namespace) -> ServingConfig:
    """Build a :class:`ServingConfig` from parsed CLI flags.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed CLI arguments.

    Returns
    -------
    ServingConfig
        Validated serving configuration.
    """
    return ServingConfig(
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        api_host=args.api_host,
        api_port=args.api_port,
        workers=args.workers,
    )


def _ensure_repo_importable() -> None:
    """Make the ``scripts`` namespace package importable for the factory target.

    Running a script directly only puts the script's own directory on
    ``sys.path``, so ``import scripts.run_feature_api`` fails.  uvicorn imports
    :data:`_FACTORY_TARGET` in the parent (to validate the app) and again in
    each spawned worker, so the repo root is added both to the live
    ``sys.path`` (parent / forked workers) and to ``PYTHONPATH`` (inherited by
    spawned workers) (§2.6).
    """
    repo_root = str(_REPO_ROOT)
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    existing = os.environ.get("PYTHONPATH", "")
    parts = [repo_root, *([existing] if existing else [])]
    os.environ["PYTHONPATH"] = os.pathsep.join(parts)


def _export_config_to_env(config: ServingConfig) -> None:
    """Publish *config* into the environment for forked uvicorn workers (§2.6).

    Parameters
    ----------
    config : ServingConfig
        Configuration to serialize into :data:`os.environ`.
    """
    os.environ[_ENV_REDIS_HOST] = config.redis_host
    os.environ[_ENV_REDIS_PORT] = str(config.redis_port)
    os.environ[_ENV_POOL_MAX] = str(config.redis_pool_max_connections)
    os.environ[_ENV_SOCKET_TIMEOUT] = str(config.redis_socket_timeout_seconds)
    os.environ[_ENV_KEY_PREFIX] = config.key_prefix


def _config_from_env() -> ServingConfig:
    """Rebuild a :class:`ServingConfig` from worker environment variables.

    Returns
    -------
    ServingConfig
        Configuration with any field present in :data:`os.environ` applied over
        the model defaults (design §2.6).
    """
    defaults = ServingConfig()
    return ServingConfig(
        redis_host=os.environ.get(_ENV_REDIS_HOST, defaults.redis_host),
        redis_port=int(os.environ.get(_ENV_REDIS_PORT, defaults.redis_port)),
        redis_pool_max_connections=int(
            os.environ.get(_ENV_POOL_MAX, defaults.redis_pool_max_connections)
        ),
        redis_socket_timeout_seconds=float(
            os.environ.get(_ENV_SOCKET_TIMEOUT, defaults.redis_socket_timeout_seconds)
        ),
        key_prefix=os.environ.get(_ENV_KEY_PREFIX, defaults.key_prefix),
    )


def app_factory() -> FastAPI:
    """uvicorn factory target: build the app from worker environment (§2.6).

    Returns
    -------
    fastapi.FastAPI
        A fresh app whose reader builds its own per-process Redis pool.
    """
    return create_app(_config_from_env())


def _run(args: argparse.Namespace) -> int:
    """Launch uvicorn for the parsed *args*.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed CLI arguments.

    Returns
    -------
    int
        Process exit code (``0``).
    """
    config = _config_from_args(args)
    if config.workers > 1:
        _ensure_repo_importable()
        _export_config_to_env(config)
        logger.info(
            f"serving on {config.api_host}:{config.api_port} "
            f"with {config.workers} worker processes"
        )
        uvicorn.run(
            _FACTORY_TARGET,
            factory=True,
            host=config.api_host,
            port=config.api_port,
            workers=config.workers,
            log_level=args.log_level,
        )
    else:
        logger.info(f"serving on {config.api_host}:{config.api_port} (1 worker)")
        uvicorn.run(
            create_app(config),
            host=config.api_host,
            port=config.api_port,
            log_level=args.log_level,
        )
    return 0


def main(argv: list[str] | None = None) -> int:
    """CLI entry point.

    Parameters
    ----------
    argv : list of str, optional
        Argument vector.  Uses :data:`sys.argv` when ``None``.

    Returns
    -------
    int
        Process exit code.
    """
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    parser = _build_parser()
    args = parser.parse_args(argv)
    return _run(args)


if __name__ == "__main__":  # pragma: no cover - manual run only
    import sys

    sys.exit(main())
