"""Pydantic configuration models for Kafka and PostgreSQL connections."""

import logging
from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings

logger = logging.getLogger(__name__)


class KafkaConfig(BaseSettings):
    """Configuration for connecting to the Kafka cluster.

    Parameters
    ----------
    bootstrap_servers : str
        Comma-separated list of broker addresses.
    security_protocol : str
        Security protocol for broker connections.
    topic : str
        Topic name for event ingestion.
    num_partitions : int
        Number of partitions for new topics.
    replication_factor : int
        Replication factor for new topics.

    Notes
    -----
    Values can be overridden via environment variables
    prefixed with ``KAFKA_`` (e.g. ``KAFKA_BOOTSTRAP_SERVERS``).
    """

    bootstrap_servers: str = Field(
        default="localhost:19092,localhost:19093,localhost:19094",
        description="Comma-separated list of broker addresses",
    )
    security_protocol: str = Field(
        default="PLAINTEXT",
        description="Security protocol for broker connections",
    )
    topic: str = Field(
        default="e-commerce-events",
        description="Topic name for event ingestion",
    )
    num_partitions: int = Field(
        default=12,
        ge=1,
        description="Number of partitions for new topics",
    )
    replication_factor: int = Field(
        default=3,
        ge=1,
        le=3,
        description="Replication factor for new topics",
    )

    model_config = {"env_prefix": "KAFKA_"}

    @property
    def bootstrap_servers_list(self) -> list[str]:
        """Return bootstrap servers as a list.

        Returns
        -------
        list[str]
            Each broker address as a separate string element.
        """
        return [s.strip() for s in self.bootstrap_servers.split(",")]


class PostgresConfig(BaseSettings):
    """Configuration for connecting to PostgreSQL.

    Parameters
    ----------
    host : str
        PostgreSQL server hostname.
    port : int
        PostgreSQL server port.
    database : str
        Database name.
    user : str
        Database user.
    password : SecretStr
        Database password (masked in logs).

    Notes
    -----
    Values can be overridden via environment variables
    prefixed with ``POSTGRES_`` (e.g. ``POSTGRES_HOST``).
    """

    host: str = Field(default="localhost", description="PostgreSQL server hostname")
    port: int = Field(default=5432, ge=1, le=65535, description="PostgreSQL server port")
    database: str = Field(default="feature_store", description="Database name")
    user: str = Field(default="featurestore", description="Database user")
    password: SecretStr = Field(
        default=SecretStr("featurestore_dev"),
        description="Database password",
    )

    model_config = {"env_prefix": "POSTGRES_"}

    @property
    def dsn(self) -> str:
        """Return a PostgreSQL DSN connection string with the password masked.

        Returns
        -------
        str
            DSN string with ``***`` in place of the real password.
        """
        return f"postgresql://{self.user}:***@{self.host}:{self.port}/{self.database}"

    def dsn_with_password(self) -> str:
        """Return a PostgreSQL DSN with the actual password.

        Returns
        -------
        str
            Full DSN connection string including the password.
        """
        return (
            f"postgresql://{self.user}:{self.password.get_secret_value()}"
            f"@{self.host}:{self.port}/{self.database}"
        )
