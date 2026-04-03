"""Shared pytest fixtures for unit and integration tests."""

import logging
import socket
import subprocess
import time

import pytest

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_KAFKA_EXTERNAL_PORTS = [19092, 19093, 19094]
_POSTGRES_PORT = 5432
_HOST = "localhost"
_HEALTH_TIMEOUT_SECONDS = 60


def _wait_for_tcp(host: str, port: int, timeout: int = _HEALTH_TIMEOUT_SECONDS) -> bool:
    """Poll a TCP endpoint until it accepts connections or timeout expires.

    Parameters
    ----------
    host : str
        Hostname to connect to.
    port : int
        TCP port to probe.
    timeout : int
        Maximum seconds to wait before returning ``False``.

    Returns
    -------
    bool
        ``True`` if the port became reachable within *timeout* seconds.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2):
                return True
        except OSError:
            time.sleep(1)
    return False


# ---------------------------------------------------------------------------
# Session-scoped infrastructure fixtures (integration tests only)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def docker_compose_file() -> str:
    """Return the path to the Docker Compose file.

    Returns
    -------
    str
        Relative path from the project root to the Compose file.
    """
    return "docker/docker-compose.yml"


@pytest.fixture(scope="session")
def docker_services_up(docker_compose_file: str) -> None:
    """Verify (and optionally start) Docker Compose services before integration tests.

    The fixture checks TCP connectivity on all Kafka external ports and the
    PostgreSQL port.  If any port is unreachable it attempts to start the
    services via ``docker compose up -d`` and waits for them to become ready.

    Parameters
    ----------
    docker_compose_file : str
        Path to the Docker Compose file.

    Raises
    ------
    RuntimeError
        If services do not become healthy within the configured timeout.
    """
    all_ports = _KAFKA_EXTERNAL_PORTS + [_POSTGRES_PORT]
    all_up = all(_wait_for_tcp(_HOST, p, timeout=2) for p in all_ports)

    if not all_up:
        logger.info("Docker services not detected — starting via docker compose up -d")
        subprocess.run(
            ["docker", "compose", "-f", docker_compose_file, "up", "-d"],
            check=True,
        )

    # Wait for each required port to be reachable.
    for port in all_ports:
        if not _wait_for_tcp(_HOST, port):
            raise RuntimeError(
                f"Service on {_HOST}:{port} did not become reachable within "
                f"{_HEALTH_TIMEOUT_SECONDS}s. Run 'make infra-up' and retry."
            )

    logger.info("All Docker services are reachable")


# ---------------------------------------------------------------------------
# Kafka admin client fixture (integration)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def kafka_bootstrap_servers() -> str:
    """Return the Kafka bootstrap servers string for the test cluster.

    Returns
    -------
    str
        Comma-separated broker addresses accessible from the host.
    """
    return "localhost:19092,localhost:19093,localhost:19094"


# ---------------------------------------------------------------------------
# PostgreSQL connection fixture (integration)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def postgres_dsn() -> str:
    """Return the PostgreSQL DSN for the test database.

    Returns
    -------
    str
        Full DSN connection string including credentials.
    """
    from streaming_feature_store.config import PostgresConfig

    return PostgresConfig().dsn_with_password()
