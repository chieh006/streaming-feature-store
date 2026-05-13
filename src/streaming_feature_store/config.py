"""Pydantic configuration models for Kafka, PostgreSQL, and Schema Registry."""

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


class ProducerTuning(BaseSettings):
    """Throughput-oriented librdkafka knobs for :class:`SerializingProducer`.

    Parameters
    ----------
    linger_ms : int
        ``linger.ms`` — wait this many ms before sending a batch so it has a
        chance to fill. Higher values trade latency for batch size / throughput.
    compression_type : str
        ``compression.type`` — one of ``none``, ``gzip``, ``snappy``, ``lz4``,
        ``zstd``. ``lz4`` gives 3-5× wire-payload reduction on Avro at near-zero
        CPU cost.
    queue_buffering_max_messages : int
        ``queue.buffering.max.messages`` — hard ceiling on in-flight messages
        in the producer's local queue. Default 100k is the cause of
        ``BufferError`` retries when ``max_in_flight`` is large.
    queue_buffering_max_kbytes : int
        ``queue.buffering.max.kbytes`` — hard ceiling on local queue bytes.
    acks : str
        ``acks`` — ``0``, ``1``, or ``all``. ``1`` skips replication round-trip
        and is **load-test / dev only**, NOT safe for production.
    batch_size : int
        ``batch.size`` — max bytes per physical batch sent to the broker.
        ``2_000_000`` allows ~2 MB batches; default librdkafka is 1 MB.

    Notes
    -----
    Field names use Python underscores; the mapping to librdkafka dotted keys
    happens in :meth:`as_librdkafka_conf`.

    Values can be overridden via environment variables prefixed with
    ``KAFKA_PRODUCER_`` (e.g. ``KAFKA_PRODUCER_LINGER_MS=50``).

    Defaults are tuned for the Week 1 load-test scenario (12 worker threads,
    ``max_in_flight=50_000``, single-broker dev cluster).
    """

    linger_ms: int = Field(default=20, ge=0, description="librdkafka linger.ms")
    compression_type: str = Field(
        default="lz4",
        pattern=r"^(none|gzip|snappy|lz4|zstd)$",
        description="librdkafka compression.type",
    )
    queue_buffering_max_messages: int = Field(
        default=1_000_000,
        ge=1,
        description="librdkafka queue.buffering.max.messages",
    )
    queue_buffering_max_kbytes: int = Field(
        default=1_048_576,
        ge=1,
        description="librdkafka queue.buffering.max.kbytes (1 GiB default)",
    )
    acks: str = Field(
        default="1",
        pattern=r"^(0|1|all)$",
        description="librdkafka acks (load-test default '1' — NOT for production)",
    )
    batch_size: int = Field(
        default=2_000_000,
        ge=1,
        description="librdkafka batch.size in bytes",
    )

    model_config = {"env_prefix": "KAFKA_PRODUCER_"}

    def as_librdkafka_conf(self) -> dict[str, object]:
        """Return the tuning fields as a librdkafka-keyed dict.

        Returns
        -------
        dict[str, object]
            Mapping suitable for merging into the
            :class:`SerializingProducer` config dict.
        """
        return {
            "linger.ms": self.linger_ms,
            "compression.type": self.compression_type,
            "queue.buffering.max.messages": self.queue_buffering_max_messages,
            "queue.buffering.max.kbytes": self.queue_buffering_max_kbytes,
            "acks": self.acks,
            "batch.size": self.batch_size,
        }


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


class SchemaRegistryConfig(BaseSettings):
    """Configuration for connecting to Confluent Schema Registry.

    Parameters
    ----------
    url : str
        Base HTTP URL for the Schema Registry REST API.
    default_compatibility : str
        Default subject compatibility level applied when registering new
        schemas. One of ``BACKWARD``, ``BACKWARD_TRANSITIVE``, ``FORWARD``,
        ``FORWARD_TRANSITIVE``, ``FULL``, ``FULL_TRANSITIVE`` or ``NONE``.
    request_timeout_s : float
        HTTP request timeout in seconds for Schema Registry calls.

    Notes
    -----
    This is a **client-side** config holding the connection details needed
    to talk to the Schema Registry over HTTP. It does not configure the
    Schema Registry server itself, nor the Kafka broker. It is shared by
    any client that needs the registry: producers (to register/fetch
    schemas when serializing), consumers (to fetch schemas by ID when
    deserializing), schema-management/CI tooling (to set subject
    compatibility or list subjects), and integration tests.

    Values can be overridden via environment variables
    prefixed with ``SCHEMA_REGISTRY_`` (e.g. ``SCHEMA_REGISTRY_URL``).
    """

    url: str = Field(
        default="http://localhost:8081",
        description="Base HTTP URL for the Schema Registry REST API",
    )
    default_compatibility: str = Field(
        default="BACKWARD",
        pattern=(
            r"^(BACKWARD|BACKWARD_TRANSITIVE|FORWARD|FORWARD_TRANSITIVE"
            r"|FULL|FULL_TRANSITIVE|NONE)$"
        ),
        description="Default subject compatibility level",
    )
    request_timeout_s: float = Field(
        default=5.0,
        gt=0,
        description="HTTP request timeout in seconds",
    )

    model_config = {"env_prefix": "SCHEMA_REGISTRY_"}

    @property
    def subjects_url(self) -> str:
        """Return the fully-qualified ``/subjects`` endpoint URL.

        Returns
        -------
        str
            URL for the Schema Registry ``/subjects`` endpoint.
        """
        return f"{self.url.rstrip('/')}/subjects"

    @property
    def config_url(self) -> str:
        """Return the fully-qualified ``/config`` endpoint URL.

        Returns
        -------
        str
            URL for the Schema Registry global ``/config`` endpoint.
        """
        return f"{self.url.rstrip('/')}/config"
