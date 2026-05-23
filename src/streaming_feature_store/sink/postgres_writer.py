"""Bulk idempotent inserts of :class:`EcommerceEvent` into ``raw_events``.

The writer wraps a single ``psycopg`` connection and exposes one operation:
:meth:`PostgresWriter.flush`, which performs a single round-trip
``INSERT ... ON CONFLICT (event_id) DO NOTHING`` for every row in the supplied
batch.  ``ON CONFLICT DO NOTHING`` is the idempotency lever — replaying a
batch after a crash leaves the table contents unchanged but increments the
``skipped`` counter so the sink can distinguish "no-op" from "missed write".

Per
``docs/design/week1_06_postgres_sink_and_continuous_feeder.md`` §2.2 the
``event_id`` UUID primary key is the idempotency key and the
read-batch-write-commit loop in :class:`SinkRunner` flushes through this
writer *before* committing Kafka offsets.

Notes
-----
The project pins ``psycopg`` (v3); the design doc's ``execute_values`` notation
refers to the psycopg2 helper.  In psycopg3 a single round-trip multi-row
insert is built by templating ``(%s, %s, ...)`` placeholders ``N`` times into
one ``INSERT`` statement, then passing the flattened argument vector to
:meth:`Cursor.execute`.  This preserves the "single ``BEGIN; INSERT; COMMIT;``"
shape from the design doc and gives a reliable ``cur.rowcount`` (the count of
*actually-inserted* rows after the ``ON CONFLICT`` filter).
"""

from __future__ import annotations

import logging
from types import TracebackType
from typing import Any

import psycopg
from psycopg.types.json import Json
from pydantic import BaseModel, ConfigDict, Field

from streaming_feature_store.schemas import EcommerceEvent

logger = logging.getLogger(__name__)


_INSERT_COLUMNS: tuple[str, ...] = (
    "event_id",
    "event_type",
    "user_id",
    "session_id",
    "event_timestamp",
    "properties",
)

_ROW_PLACEHOLDER: str = "(" + ", ".join(["%s"] * len(_INSERT_COLUMNS)) + ")"


class BatchInsertResult(BaseModel):
    """Outcome of a single :meth:`PostgresWriter.flush` call.

    Parameters
    ----------
    inserted : int
        Number of rows actually inserted (``cur.rowcount`` after the
        ``ON CONFLICT`` filter).
    skipped : int
        Number of rows that hit the ``ON CONFLICT (event_id) DO NOTHING``
        clause — i.e. duplicates that were already present.  Non-zero only
        in the at-least-once-replay path (design doc §2.2).
    """

    model_config = ConfigDict(frozen=True)

    inserted: int = Field(..., ge=0)
    skipped: int = Field(..., ge=0)

    @property
    def total(self) -> int:
        """Total rows processed in this flush.

        Returns
        -------
        int
            ``inserted + skipped``.
        """
        return self.inserted + self.skipped


def _build_insert_sql(n_rows: int) -> str:
    """Build the multi-row idempotent ``INSERT`` statement for *n_rows* rows.

    Parameters
    ----------
    n_rows : int
        Number of rows in the batch.  Must be ``>= 1``.

    Returns
    -------
    str
        ``INSERT ... VALUES (%s, ...), (%s, ...) ON CONFLICT DO NOTHING``
        with one ``(%s, ...)`` group per row.

    Raises
    ------
    ValueError
        If ``n_rows`` is less than 1.
    """
    if n_rows < 1:
        raise ValueError(f"n_rows must be >= 1, got {n_rows}")
    columns = ", ".join(_INSERT_COLUMNS)
    values = ", ".join([_ROW_PLACEHOLDER] * n_rows)
    return (
        f"INSERT INTO raw_events ({columns}) VALUES {values} "
        f"ON CONFLICT (event_id) DO NOTHING"
    )


def _event_to_row(event: EcommerceEvent) -> tuple[Any, ...]:
    """Flatten one :class:`EcommerceEvent` into a positional INSERT row.

    Parameters
    ----------
    event : EcommerceEvent
        Validated event instance.

    Returns
    -------
    tuple
        ``(event_id, event_type, user_id, session_id, event_timestamp,
        properties)`` in the order declared by :data:`_INSERT_COLUMNS`.

    Notes
    -----
    The ``event_id`` is passed as :class:`uuid.UUID`; psycopg3 adapts it to
    PostgreSQL ``uuid``.  The ``event_timestamp`` is passed as the original
    timezone-aware :class:`datetime`; psycopg3 adapts it to ``timestamptz``.
    The payload is dumped to a plain dict and wrapped in :class:`Json` so
    psycopg3 binds it as JSONB.
    """
    return (
        event.event_id,
        event.event_type.value,
        event.user_id,
        event.session_id,
        event.event_timestamp,
        Json(event.payload.model_dump()),
    )


class PostgresWriter:
    """Bulk idempotent inserts into ``raw_events``.

    Parameters
    ----------
    dsn : str
        psycopg connection string (``host=... user=... ...``) or libpq URL
        (``postgresql://...``).  Passed unchanged to
        :func:`psycopg.connect`.
    statement_timeout_ms : int, optional
        PostgreSQL ``statement_timeout`` applied per session.  Defaults to
        ``30_000`` (30 s) — well above the worst-case 1000-row batch on
        laptop hardware.

    Notes
    -----
    The connection is opened lazily on first :meth:`flush` and held for the
    lifetime of the writer (one persistent connection per sink process).
    Re-connection on transient failure (``OperationalError``) is the
    caller's responsibility; :class:`SinkRunner` wraps :meth:`flush` in a
    single retry with backoff (design doc §4.2).

    Autocommit is disabled — each :meth:`flush` opens a transaction
    implicitly on first ``execute``, the ``INSERT`` runs, and the writer
    commits before returning.  On failure the writer rolls back and
    re-raises so the caller can decide whether to retry.
    """

    def __init__(self, dsn: str, *, statement_timeout_ms: int = 30_000) -> None:
        if statement_timeout_ms < 1:
            raise ValueError(
                f"statement_timeout_ms must be >= 1, got {statement_timeout_ms}"
            )
        self._dsn = dsn
        self._statement_timeout_ms = statement_timeout_ms
        self._conn: psycopg.Connection | None = None
        self._closed = False

    @property
    def dsn(self) -> str:
        """Configured connection string (may include the password).

        Returns
        -------
        str
            DSN supplied at construction.
        """
        return self._dsn

    @property
    def statement_timeout_ms(self) -> int:
        """Per-session ``statement_timeout`` in milliseconds.

        Returns
        -------
        int
            Configured timeout.
        """
        return self._statement_timeout_ms

    def _connect(self) -> psycopg.Connection:
        """Open the underlying connection lazily.

        Returns
        -------
        psycopg.Connection
            Opened connection with autocommit disabled and
            ``statement_timeout`` applied to the session.

        Raises
        ------
        RuntimeError
            If the writer has already been closed.
        """
        if self._closed:
            raise RuntimeError("PostgresWriter is closed")
        if self._conn is None:
            self._conn = psycopg.connect(self._dsn, autocommit=False)
            with self._conn.cursor() as cur:
                cur.execute(f"SET statement_timeout = {self._statement_timeout_ms}")
            self._conn.commit()
        return self._conn

    def flush(self, events: list[EcommerceEvent]) -> BatchInsertResult:
        """Insert *events* with ``ON CONFLICT (event_id) DO NOTHING``.

        Parameters
        ----------
        events : list of EcommerceEvent
            Batch to insert.  An empty list short-circuits and returns a
            zero-count result without touching the connection.

        Returns
        -------
        BatchInsertResult
            ``inserted`` is the number of rows the database actually wrote
            (``cur.rowcount`` after the ``ON CONFLICT`` filter); ``skipped``
            is ``len(events) - inserted``.

        Raises
        ------
        Exception
            Re-raises any database error after rolling back the
            transaction; the caller is expected to retry or surface it.
        """
        if not events:
            return BatchInsertResult(inserted=0, skipped=0)
        conn = self._connect()
        sql = _build_insert_sql(len(events))
        # Flatten one tuple per row into a single positional argument list.
        params: list[Any] = []
        for event in events:
            params.extend(_event_to_row(event))
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                inserted = cur.rowcount if cur.rowcount is not None else 0
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        skipped = max(0, len(events) - inserted)
        return BatchInsertResult(inserted=inserted, skipped=skipped)

    def close(self) -> None:
        """Close the underlying connection.  Safe to call more than once."""
        if self._closed:
            return
        self._closed = True
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning(f"PostgresWriter.close() suppressed error: {exc}")
            self._conn = None

    def __enter__(self) -> PostgresWriter:
        """Return ``self`` for use as a context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Close on context-manager exit."""
        self.close()
