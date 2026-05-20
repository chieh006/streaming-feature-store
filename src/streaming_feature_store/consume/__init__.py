"""Single-member consume harness — end-to-end latency + lag.

This sub-package is the consume-side mirror of
:mod:`streaming_feature_store.load`: ``consume_runner`` ⇄ ``load_runner``,
``accountant`` ⇄ ``accountant``, ``report`` ⇄ ``report``.  One
:class:`ConsumeRunner` is one consumer-group member; the multi-process
escape lives in :mod:`streaming_feature_store.consume_mp`.
"""

from streaming_feature_store.consume.accountant import (
    ConsumeAccountant,
    ConsumeSnapshot,
)
from streaming_feature_store.consume.consume_runner import ConsumeRunner
from streaming_feature_store.consume.report import (
    ConsumeRunConfig,
    ConsumeRunReport,
    render_markdown,
)

__all__ = [
    "ConsumeAccountant",
    "ConsumeRunConfig",
    "ConsumeRunReport",
    "ConsumeRunner",
    "ConsumeSnapshot",
    "render_markdown",
]
