"""Exactly-once semantics (EOS) transactional layer (design doc week2_03).

This package wraps the existing consume-process-produce loops (the validator
and the sliding-window consumer) in a Kafka transaction so the input-offset
commit and the Kafka-side output writes are atomic across Kafka topics +
consumer offsets.

Public surface
--------------
- :func:`derive_transactional_id` — stable, unique-per-process
  ``transactional.id`` (design §2.3).
- :class:`TransactionalConfig` — per-process EOS knob bag (design §3.4).
- :class:`TransactionalAvroProducer` — single multi-topic transactional
  producer (design §2.4 / §4.2).
- :class:`CommitStrategy`, :class:`AtLeastOnceCommit`,
  :class:`TransactionalCommit` — the commit-lifecycle seam (design §2.2 / §4.3).
- :func:`requires_abort` — classify a transactional API error (design §2.8).

The transaction boundary is one poll-batch (design §2.7); Redis / Postgres
writes stay *outside* the transaction on the idempotent-write contract
(design §2.6).
"""

from __future__ import annotations

from streaming_feature_store.eos.commit_strategy import (
    AtLeastOnceCommit,
    CommitStrategy,
    TransactionalCommit,
    requires_abort,
)
from streaming_feature_store.eos.route_adapters import (
    TransactionalDlqRoute,
    TransactionalValidatedRoute,
)
from streaming_feature_store.eos.transactional_id import derive_transactional_id
from streaming_feature_store.eos.transactional_producer import (
    TransactionalAvroProducer,
    TransactionalConfig,
    transactional_producer_conf,
)

__all__ = [
    "AtLeastOnceCommit",
    "CommitStrategy",
    "TransactionalCommit",
    "TransactionalAvroProducer",
    "TransactionalConfig",
    "TransactionalDlqRoute",
    "TransactionalValidatedRoute",
    "derive_transactional_id",
    "requires_abort",
    "transactional_producer_conf",
]
