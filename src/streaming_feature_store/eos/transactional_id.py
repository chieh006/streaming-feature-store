"""Derivation of a stable, unique-per-process ``transactional.id`` (design §2.3).

A transactional producer needs a ``transactional.id`` that is **stable across
restarts** (so the transaction coordinator recognises a restarted process as
the *same* producer and fences its zombie predecessor) and **unique across the
live consumer group** (two live producers sharing an id fence each other in a
loop).  ``f"{group_id}-{ordinal}"`` with a process-pinned ordinal satisfies
both: deterministic per process, disjoint across the group.

This is the per-process-id half of the plan's caveat #1 — the multi-process
design means *N* ids and *N* independent transaction scopes, not one shared
distributed transaction.
"""

from __future__ import annotations


def derive_transactional_id(group_id: str, ordinal: int) -> str:
    """Build a stable, unique-per-process ``transactional.id``.

    Parameters
    ----------
    group_id : str
        The Kafka consumer group id shared by every member of the group
        (e.g. ``"sliding-features-job"``).
    ordinal : int
        This process's fixed member ordinal in ``[0, num_workers)``.  Supplied
        by the multi-process supervisor (or a single ``0`` for a lone process)
        and pinned across restarts so the id keeps mapping to the same
        partition subset.

    Returns
    -------
    str
        ``f"{group_id}-{ordinal}"`` — deterministic per process, disjoint
        across the group, stable across restarts.

    Raises
    ------
    ValueError
        If *group_id* is empty/blank, or *ordinal* is negative.

    Examples
    --------
    >>> derive_transactional_id("sliding-features-job", 3)
    'sliding-features-job-3'
    """
    if not group_id or not group_id.strip():
        raise ValueError("group_id must be a non-empty string")
    if ordinal < 0:
        raise ValueError(f"ordinal must be >= 0, got {ordinal}")
    return f"{group_id}-{ordinal}"
