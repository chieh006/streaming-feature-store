"""Unit tests for KafkaConfig and PostgresConfig Pydantic models.

These tests run without Docker — no external services required.
"""

import pytest
from pydantic import SecretStr, ValidationError

from streaming_feature_store.config import KafkaConfig, PostgresConfig


# ---------------------------------------------------------------------------
# KafkaConfig
# ---------------------------------------------------------------------------


class TestKafkaConfigDefaults:
    """Verify default values for KafkaConfig."""

    def test_bootstrap_servers(self) -> None:
        cfg = KafkaConfig()
        assert cfg.bootstrap_servers == "localhost:19092,localhost:19093,localhost:19094"

    def test_security_protocol(self) -> None:
        cfg = KafkaConfig()
        assert cfg.security_protocol == "PLAINTEXT"

    def test_topic(self) -> None:
        cfg = KafkaConfig()
        assert cfg.topic == "e-commerce-events"

    def test_num_partitions(self) -> None:
        cfg = KafkaConfig()
        assert cfg.num_partitions == 12

    def test_replication_factor(self) -> None:
        cfg = KafkaConfig()
        assert cfg.replication_factor == 3


class TestKafkaConfigBootstrapServersList:
    """Verify the bootstrap_servers_list property."""

    def test_default_splits_to_three_items(self) -> None:
        cfg = KafkaConfig()
        servers = cfg.bootstrap_servers_list
        assert len(servers) == 3

    def test_default_contains_expected_addresses(self) -> None:
        cfg = KafkaConfig()
        assert cfg.bootstrap_servers_list == [
            "localhost:19092",
            "localhost:19093",
            "localhost:19094",
        ]

    def test_single_server_returns_list_of_one(self) -> None:
        cfg = KafkaConfig(bootstrap_servers="localhost:9092")
        assert cfg.bootstrap_servers_list == ["localhost:9092"]

    def test_whitespace_trimmed(self) -> None:
        cfg = KafkaConfig(bootstrap_servers="host1:9092 , host2:9092 , host3:9092")
        assert cfg.bootstrap_servers_list == ["host1:9092", "host2:9092", "host3:9092"]


class TestKafkaConfigCustomValues:
    """Verify that custom constructor values are accepted."""

    def test_custom_bootstrap_servers(self) -> None:
        cfg = KafkaConfig(bootstrap_servers="broker-a:9092,broker-b:9092")
        assert "broker-a:9092" in cfg.bootstrap_servers_list

    def test_custom_topic(self) -> None:
        cfg = KafkaConfig(topic="my-topic")
        assert cfg.topic == "my-topic"

    def test_custom_partitions(self) -> None:
        cfg = KafkaConfig(num_partitions=6)
        assert cfg.num_partitions == 6

    def test_custom_replication_factor(self) -> None:
        cfg = KafkaConfig(replication_factor=1)
        assert cfg.replication_factor == 1


class TestKafkaConfigValidation:
    """Verify Pydantic validation rejects invalid values."""

    def test_rejects_zero_partitions(self) -> None:
        with pytest.raises(ValidationError):
            KafkaConfig(num_partitions=0)

    def test_rejects_negative_partitions(self) -> None:
        with pytest.raises(ValidationError):
            KafkaConfig(num_partitions=-1)

    def test_rejects_replication_factor_above_three(self) -> None:
        with pytest.raises(ValidationError):
            KafkaConfig(replication_factor=4)

    def test_rejects_replication_factor_zero(self) -> None:
        with pytest.raises(ValidationError):
            KafkaConfig(replication_factor=0)


class TestKafkaConfigEnvOverride:
    """Verify environment-variable-based overrides via monkeypatch."""

    def test_bootstrap_servers_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker-env:9092")
        cfg = KafkaConfig()
        assert cfg.bootstrap_servers == "broker-env:9092"

    def test_topic_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KAFKA_TOPIC", "env-topic")
        cfg = KafkaConfig()
        assert cfg.topic == "env-topic"

    def test_num_partitions_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KAFKA_NUM_PARTITIONS", "6")
        cfg = KafkaConfig()
        assert cfg.num_partitions == 6


# ---------------------------------------------------------------------------
# PostgresConfig
# ---------------------------------------------------------------------------


class TestPostgresConfigDefaults:
    """Verify default values for PostgresConfig."""

    def test_host(self) -> None:
        cfg = PostgresConfig()
        assert cfg.host == "localhost"

    def test_port(self) -> None:
        cfg = PostgresConfig()
        assert cfg.port == 5432

    def test_database(self) -> None:
        cfg = PostgresConfig()
        assert cfg.database == "feature_store"

    def test_user(self) -> None:
        cfg = PostgresConfig()
        assert cfg.user == "featurestore"

    def test_password_is_secret_str(self) -> None:
        cfg = PostgresConfig()
        assert isinstance(cfg.password, SecretStr)


class TestPostgresConfigDSN:
    """Verify DSN generation and password masking."""

    def test_dsn_masks_password(self) -> None:
        cfg = PostgresConfig()
        assert "***" in cfg.dsn
        assert "featurestore_dev" not in cfg.dsn

    def test_dsn_contains_correct_host(self) -> None:
        cfg = PostgresConfig()
        assert "localhost" in cfg.dsn

    def test_dsn_contains_correct_port(self) -> None:
        cfg = PostgresConfig()
        assert "5432" in cfg.dsn

    def test_dsn_contains_correct_database(self) -> None:
        cfg = PostgresConfig()
        assert "feature_store" in cfg.dsn

    def test_dsn_with_password_contains_real_password(self) -> None:
        cfg = PostgresConfig()
        assert "featurestore_dev" in cfg.dsn_with_password()

    def test_dsn_with_password_does_not_mask(self) -> None:
        cfg = PostgresConfig()
        assert "***" not in cfg.dsn_with_password()

    def test_dsn_format(self) -> None:
        cfg = PostgresConfig()
        assert cfg.dsn.startswith("postgresql://featurestore:***@localhost:5432/feature_store")


class TestPostgresConfigPasswordSecrecy:
    """Verify that SecretStr prevents accidental password exposure."""

    def test_str_repr_does_not_reveal_password(self) -> None:
        cfg = PostgresConfig()
        assert "featurestore_dev" not in str(cfg.password)

    def test_repr_does_not_reveal_password(self) -> None:
        cfg = PostgresConfig()
        assert "featurestore_dev" not in repr(cfg.password)

    def test_get_secret_value_returns_real_password(self) -> None:
        cfg = PostgresConfig()
        assert cfg.password.get_secret_value() == "featurestore_dev"


class TestPostgresConfigValidation:
    """Verify Pydantic validation rejects invalid port values."""

    def test_rejects_port_zero(self) -> None:
        with pytest.raises(ValidationError):
            PostgresConfig(port=0)

    def test_rejects_port_above_65535(self) -> None:
        with pytest.raises(ValidationError):
            PostgresConfig(port=70000)

    def test_accepts_port_1(self) -> None:
        cfg = PostgresConfig(port=1)
        assert cfg.port == 1

    def test_accepts_port_65535(self) -> None:
        cfg = PostgresConfig(port=65535)
        assert cfg.port == 65535


class TestPostgresConfigEnvOverride:
    """Verify environment-variable-based overrides via monkeypatch."""

    def test_host_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("POSTGRES_HOST", "db-server")
        cfg = PostgresConfig()
        assert cfg.host == "db-server"

    def test_port_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("POSTGRES_PORT", "5433")
        cfg = PostgresConfig()
        assert cfg.port == 5433

    def test_database_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("POSTGRES_DATABASE", "my_db")
        cfg = PostgresConfig()
        assert cfg.database == "my_db"

    def test_user_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("POSTGRES_USER", "admin")
        cfg = PostgresConfig()
        assert cfg.user == "admin"

    def test_password_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("POSTGRES_PASSWORD", "s3cr3t")
        cfg = PostgresConfig()
        assert cfg.password.get_secret_value() == "s3cr3t"
