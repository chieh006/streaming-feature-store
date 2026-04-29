"""Thin wrapper over ``confluent_kafka.schema_registry.SchemaRegistryClient``.

Exposes the small subset of registry operations needed by this project:
schema registration, latest-version lookup, compatibility configuration, and
subject introspection.  All methods emit structured logs and surface registry
errors via ``RegistryError``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from confluent_kafka.schema_registry import (
    RegisteredSchema as _ConfluentRegisteredSchema,
    Schema,
    SchemaRegistryClient,
)
from confluent_kafka.schema_registry.error import SchemaRegistryError

from streaming_feature_store.config import SchemaRegistryConfig

logger = logging.getLogger(__name__)


class RegistryError(RuntimeError):
    """Raised when a Schema Registry operation fails."""


@dataclass(frozen=True)
class RegisteredSchema:
    """Lightweight snapshot of a registered Schema Registry version.

    Parameters
    ----------
    subject : str
        Subject under which the schema is registered.
    schema_id : int
        Globally-unique schema ID assigned by the registry.
    version : int
        Subject-local version number.
    schema_str : str
        The raw Avro schema JSON string.
    """

    subject: str
    schema_id: int
    version: int
    schema_str: str


class SchemaRegistry:
    """Project-facing facade over ``SchemaRegistryClient``.

    Parameters
    ----------
    config : SchemaRegistryConfig
        Connection settings (URL and request timeout).

    Notes
    -----
    Construction is lazy on the underlying ``SchemaRegistryClient``: it is
    instantiated immediately, but no network I/O occurs until a method is
    called.
    """

    def __init__(self, config: SchemaRegistryConfig) -> None:
        self._config = config
        self._client = SchemaRegistryClient(self._build_client_conf(config))

    @staticmethod
    def _build_client_conf(config: SchemaRegistryConfig) -> dict:
        """Translate :class:`SchemaRegistryConfig` to ``SchemaRegistryClient`` config.

        Parameters
        ----------
        config : SchemaRegistryConfig
            Project-level registry configuration.

        Returns
        -------
        dict
            Configuration dict accepted by ``SchemaRegistryClient``.
        """
        return {
            "url": config.url,
            "timeout": config.request_timeout_s,
        }

    @property
    def client(self) -> SchemaRegistryClient:
        """Return the underlying ``SchemaRegistryClient`` instance.

        Returns
        -------
        SchemaRegistryClient
            The wrapped low-level client.
        """
        return self._client

    def register(self, subject: str, schema_str: str) -> int:
        """Register *schema_str* under *subject*; idempotent.

        Parameters
        ----------
        subject : str
            Schema Registry subject (typically ``<topic>-value``).
        schema_str : str
            Avro schema JSON string.

        Returns
        -------
        int
            Globally-unique schema ID.

        Raises
        ------
        RegistryError
            If the registry rejects the schema (e.g. incompatible change).
        """
        schema = Schema(schema_str=schema_str, schema_type="AVRO")
        try:
            schema_id = self._client.register_schema(subject, schema)
        except SchemaRegistryError as exc:
            logger.error(
                f"Schema registration failed for subject={subject!r}: "
                f"http={exc.http_status_code} code={exc.error_code} msg={exc.error_message}"
            )
            raise RegistryError(
                f"Failed to register schema under {subject!r}: {exc.error_message}"
            ) from exc
        logger.info(f"Registered schema for subject={subject!r} schema_id={schema_id}")
        return schema_id

    def get_latest(self, subject: str) -> RegisteredSchema:
        """Fetch the latest version registered under *subject*.

        Parameters
        ----------
        subject : str
            Schema Registry subject.

        Returns
        -------
        RegisteredSchema
            Snapshot of the latest version.

        Raises
        ------
        RegistryError
            If the subject does not exist or the request fails.
        """
        try:
            registered: _ConfluentRegisteredSchema = self._client.get_latest_version(subject)
        except SchemaRegistryError as exc:
            logger.error(
                f"get_latest failed for subject={subject!r}: "
                f"http={exc.http_status_code} code={exc.error_code} msg={exc.error_message}"
            )
            raise RegistryError(
                f"Could not fetch latest version for {subject!r}: {exc.error_message}"
            ) from exc
        return RegisteredSchema(
            subject=registered.subject,
            schema_id=registered.schema_id,
            version=registered.version,
            schema_str=registered.schema.schema_str,
        )

    def set_compatibility(self, subject: str, level: str) -> None:
        """Set per-subject compatibility level.

        Parameters
        ----------
        subject : str
            Schema Registry subject.
        level : str
            One of ``BACKWARD``, ``BACKWARD_TRANSITIVE``, ``FORWARD``,
            ``FORWARD_TRANSITIVE``, ``FULL``, ``FULL_TRANSITIVE``, ``NONE``.

        Raises
        ------
        RegistryError
            If the registry rejects the request.
        """
        try:
            self._client.set_compatibility(subject_name=subject, level=level)
        except SchemaRegistryError as exc:
            logger.error(
                f"set_compatibility failed for subject={subject!r} level={level!r}: "
                f"http={exc.http_status_code} code={exc.error_code} msg={exc.error_message}"
            )
            raise RegistryError(
                f"Could not set compatibility {level!r} on {subject!r}: {exc.error_message}"
            ) from exc
        logger.info(f"Set compatibility level={level!r} on subject={subject!r}")

    def list_subjects(self) -> list[str]:
        """Return all subjects currently registered.

        Returns
        -------
        list of str
            Subject names.

        Raises
        ------
        RegistryError
            If the request fails.
        """
        try:
            return list(self._client.get_subjects())
        except SchemaRegistryError as exc:
            logger.error(
                f"list_subjects failed: "
                f"http={exc.http_status_code} code={exc.error_code} msg={exc.error_message}"
            )
            raise RegistryError(f"Could not list subjects: {exc.error_message}") from exc

    def delete_subject(self, subject: str, *, permanent: bool = False) -> list[int]:
        """Delete a subject (soft by default).

        Parameters
        ----------
        subject : str
            Subject to delete.
        permanent : bool, optional
            If ``True``, perform a hard delete after the soft delete.  Used by
            integration test teardown.

        Returns
        -------
        list of int
            Versions that were deleted.

        Raises
        ------
        RegistryError
            If the request fails.
        """
        try:
            versions = list(self._client.delete_subject(subject, permanent=permanent))
        except SchemaRegistryError as exc:
            logger.error(
                f"delete_subject failed for subject={subject!r} permanent={permanent}: "
                f"http={exc.http_status_code} code={exc.error_code} msg={exc.error_message}"
            )
            raise RegistryError(
                f"Could not delete subject {subject!r}: {exc.error_message}"
            ) from exc
        logger.info(
            f"Deleted subject={subject!r} permanent={permanent} versions={versions}"
        )
        return versions
