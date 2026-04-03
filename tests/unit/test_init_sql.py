"""Unit tests for docker/postgres/init.sql.

These tests parse the SQL file on disk — no database connection required.
"""

from pathlib import Path

import pytest
import sqlparse


# ---------------------------------------------------------------------------
# Fixture: load the SQL file once per test session
# ---------------------------------------------------------------------------

_SQL_PATH = Path(__file__).parents[2] / "docker" / "postgres" / "init.sql"

_EXPECTED_COLUMNS = [
    "event_id",
    "event_type",
    "user_id",
    "session_id",
    "event_timestamp",
    "properties",
    "ingested_at",
]

_EXPECTED_INDEXES = [
    "idx_raw_events_timestamp",
    "idx_raw_events_user_id",
    "idx_raw_events_event_type",
    "idx_raw_events_user_timestamp",
]


@pytest.fixture(scope="module")
def sql_content() -> str:
    """Read and return the contents of init.sql.

    Returns
    -------
    str
        Raw SQL text.
    """
    return _SQL_PATH.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# File-existence test
# ---------------------------------------------------------------------------


def test_init_sql_file_exists() -> None:
    """init.sql must exist at the expected path."""
    assert _SQL_PATH.exists(), f"Expected file not found: {_SQL_PATH}"
    assert _SQL_PATH.is_file()


# ---------------------------------------------------------------------------
# Table definition tests
# ---------------------------------------------------------------------------


def test_init_sql_creates_raw_events_table(sql_content: str) -> None:
    """SQL must contain a CREATE TABLE statement for raw_events."""
    assert "CREATE TABLE IF NOT EXISTS raw_events" in sql_content


def test_init_sql_has_required_columns(sql_content: str) -> None:
    """SQL must reference all expected columns."""
    for column in _EXPECTED_COLUMNS:
        assert column in sql_content, f"Column '{column}' not found in init.sql"


def test_init_sql_event_id_is_primary_key(sql_content: str) -> None:
    """event_id must be declared as PRIMARY KEY."""
    assert "event_id" in sql_content
    assert "PRIMARY KEY" in sql_content.upper()


def test_init_sql_event_id_is_uuid(sql_content: str) -> None:
    """event_id column must use UUID type."""
    # Look for the pattern "event_id UUID" (case-insensitive, allowing whitespace)
    import re
    assert re.search(r"event_id\s+UUID", sql_content, re.IGNORECASE), (
        "event_id must be of type UUID"
    )


def test_init_sql_properties_is_jsonb(sql_content: str) -> None:
    """properties column must use JSONB type."""
    import re
    assert re.search(r"properties\s+JSONB", sql_content, re.IGNORECASE), (
        "properties must be of type JSONB"
    )


def test_init_sql_event_timestamp_is_timestamptz(sql_content: str) -> None:
    """event_timestamp must use TIMESTAMPTZ type."""
    import re
    assert re.search(r"event_timestamp\s+TIMESTAMPTZ", sql_content, re.IGNORECASE)


# ---------------------------------------------------------------------------
# Index tests
# ---------------------------------------------------------------------------


def test_init_sql_has_required_indexes(sql_content: str) -> None:
    """SQL must contain all four expected CREATE INDEX statements."""
    for index_name in _EXPECTED_INDEXES:
        assert index_name in sql_content, f"Index '{index_name}' not found in init.sql"


def test_init_sql_index_count(sql_content: str) -> None:
    """SQL must contain exactly four CREATE INDEX statements."""
    import re
    matches = re.findall(r"CREATE INDEX IF NOT EXISTS", sql_content, re.IGNORECASE)
    assert len(matches) == 4, f"Expected 4 indexes, found {len(matches)}"


def test_init_sql_composite_index_covers_user_and_timestamp(sql_content: str) -> None:
    """The composite index must include both user_id and event_timestamp."""
    assert "idx_raw_events_user_timestamp" in sql_content
    # Verify both columns appear after the composite index name
    idx = sql_content.index("idx_raw_events_user_timestamp")
    after = sql_content[idx:]
    assert "user_id" in after
    assert "event_timestamp" in after


# ---------------------------------------------------------------------------
# Syntax validation via sqlparse
# ---------------------------------------------------------------------------


def test_init_sql_is_parseable(sql_content: str) -> None:
    """sqlparse must be able to parse the file without raising an exception."""
    statements = sqlparse.parse(sql_content)
    assert len(statements) > 0, "sqlparse returned no statements"


def test_init_sql_has_no_unclosed_parentheses(sql_content: str) -> None:
    """The SQL file must have balanced parentheses."""
    assert sql_content.count("(") == sql_content.count(")"), (
        "Unbalanced parentheses detected in init.sql"
    )


def test_init_sql_all_statements_end_with_semicolon(sql_content: str) -> None:
    """Every non-empty, non-comment statement must end with a semicolon."""
    statements = sqlparse.parse(sql_content)
    for stmt in statements:
        # Skip empty statements and pure-comment statements
        flat = stmt.value.strip()
        if not flat or flat.startswith("--"):
            continue
        # Strip trailing whitespace/newlines before checking for semicolon
        assert flat.endswith(";"), f"Statement does not end with ';':\n{flat[:80]}..."
