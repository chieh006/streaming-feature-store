"""Engine-neutral models for the sliding-window feature consumer.

This module is the PyFlink-free core relocated from the superseded
``flink/sliding`` package (design doc §2.13).  It carries:

* :class:`WindowResolution` — the three sliding resolutions, whose string
  values double as Redis-field suffixes and Kafka-key discriminators and whose
  *symbol names* are the Avro enum symbols.
* :class:`SlidingAccumulator` — the per-pane accumulator mutated incrementally
  by the aggregators (design doc §2.3).
* :class:`SlidingFeatureRecord` — the Pydantic mirror of the
  ``sliding_feature_record.avsc`` schema emitted per ``(user, window-end,
  resolution)`` slide tick.
* :class:`SlidingConsumerConfig` — the consumer-runtime configuration
  (design doc §3.3), replacing the retired PyFlink ``SlidingJobConfig``.

The module imports no Kafka / Redis / PyFlink symbols so it stays cheap to
import and trivial to unit-test.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field, model_validator

from streaming_feature_store.schemas import EcommerceEvent


class WindowResolution(str, Enum):
    """One of the three sliding-window resolutions.

    Notes
    -----
    The string values (``"5m"`` / ``"1h"`` / ``"24h"``) double as Redis-field
    suffixes and the Kafka-key discriminator, so renames are coordinated across
    all layers by editing this enum.  The *symbol names* (``W_5M_SLIDE_1M`` …)
    are the Avro enum symbols and are append-only under ``BACKWARD``
    compatibility.
    """

    W_5M_SLIDE_1M = "5m"
    W_1H_SLIDE_5M = "1h"
    W_24H_SLIDE_1H = "24h"

    @property
    def window_size_seconds(self) -> int:
        """Window duration in seconds.

        Returns
        -------
        int
            Size of the lookback window (300 / 3600 / 86400).
        """
        return _RESOLUTION_SPECS[self][0]

    @property
    def slide_seconds(self) -> int:
        """Slide interval in seconds.

        Returns
        -------
        int
            Emission cadence of the window (60 / 300 / 3600).
        """
        return _RESOLUTION_SPECS[self][1]

    @property
    def window_size_ms(self) -> int:
        """Window duration in milliseconds.

        Returns
        -------
        int
            :attr:`window_size_seconds` × 1000.
        """
        return self.window_size_seconds * 1000

    @property
    def slide_ms(self) -> int:
        """Slide interval in milliseconds.

        Returns
        -------
        int
            :attr:`slide_seconds` × 1000.
        """
        return self.slide_seconds * 1000

    @property
    def panes_per_window(self) -> int:
        """Number of slide-sized panes composing one window.

        Returns
        -------
        int
            ``window_size_seconds // slide_seconds`` (5 / 12 / 24).
        """
        return self.window_size_seconds // self.slide_seconds


# (window_size_seconds, slide_seconds) per resolution — the three canonical
# resolutions from design doc §1.
_RESOLUTION_SPECS: dict[WindowResolution, tuple[int, int]] = {
    WindowResolution.W_5M_SLIDE_1M: (300, 60),
    WindowResolution.W_1H_SLIDE_5M: (3600, 300),
    WindowResolution.W_24H_SLIDE_1H: (86400, 3600),
}


@dataclass
class SlidingAccumulator:
    """Pane-level accumulator updated incrementally by an aggregator.

    Parameters
    ----------
    user_id : str
        Owning user identifier.
    click_count : int
        Number of ``CLICK`` events folded into this pane.
    page_view_count : int
        Number of ``PAGE_VIEW`` events folded into this pane.
    purchase_count : int
        Number of ``PURCHASE`` events folded into this pane.
    revenue : float
        Sum of ``price_cents / 100 × quantity`` over purchases.
    distinct_products : set of str
        Distinct ``product_id`` values seen in purchases (design doc §2.14).

    Notes
    -----
    Per design doc §2.3 this is the *per-pane* state, not the per-window state.
    The driver merges several panes via the aggregator's ``merge`` to produce a
    window aggregate on emission.
    """

    user_id: str = ""
    click_count: int = 0
    page_view_count: int = 0
    purchase_count: int = 0
    revenue: float = 0.0
    distinct_products: set[str] = field(default_factory=set)


class SlidingFeatureRecord(BaseModel):
    """Pydantic mirror of the ``sliding_feature_record.avsc`` Avro record.

    Parameters
    ----------
    user_id : str
        User identifier; matches the Kafka message-key prefix.
    window_resolution : WindowResolution
        Which of the three sliding resolutions this record belongs to.
    window_start_ms : int
        Inclusive lower bound of the window (ms since the Unix epoch).
    window_end_ms : int
        Exclusive upper bound of the window; the stable per-window identifier
        for idempotency (design doc §2.9).
    emission_seq : int
        ``0`` on the first fire of a window; ``+1`` per allowed-lateness
        re-fire.
    click_count, page_view_count, purchase_count : int or None
        Per-event-type counts; populated only for the resolutions that carry
        them (design doc §2.14).
    revenue, avg_purchase_amount : float or None
        Monetary aggregates; populated per resolution.
    distinct_products : int or None
        Count-distinct of purchased ``product_id`` in the window.

    Notes
    -----
    The model is intentionally **mutable**: the windowing driver fills
    ``window_start_ms`` / ``window_end_ms`` after the aggregator produces the
    record and stamps ``emission_seq`` at emission time (design doc §4.3).
    """

    model_config = ConfigDict(extra="forbid")

    user_id: str
    window_resolution: WindowResolution
    window_start_ms: int = 0
    window_end_ms: int = 0
    emission_seq: int = 0
    click_count: int | None = None
    page_view_count: int | None = None
    purchase_count: int | None = None
    revenue: float | None = None
    distinct_products: int | None = None
    avg_purchase_amount: float | None = None

    def idempotency_key(self) -> str:
        """Natural dedup key for this emission.

        Returns
        -------
        str
            ``"{user_id}:{resolution}:{window_end_ms}:{emission_seq}"`` — the
            4-tuple downstream consumers dedupe on (design doc §2.9).
        """
        return (
            f"{self.user_id}:{self.window_resolution.value}:"
            f"{self.window_end_ms}:{self.emission_seq}"
        )

    def kafka_key(self) -> str:
        """Kafka message key for the ``sliding-features`` produce.

        Returns
        -------
        str
            ``"{user_id}:{resolution}"`` so log compaction (if ever enabled)
            retains the latest record per ``(user, resolution)`` (design doc
            §2.8).
        """
        return f"{self.user_id}:{self.window_resolution.value}"

    def redis_field_updates(self) -> dict[str, str]:
        """Resolution-suffixed Redis hash fields for this record.

        Returns
        -------
        dict of str to str
            Mapping of ``"{feature}_{resolution}"`` to the stringified value,
            omitting any feature that is ``None`` (design doc §2.7 / §2.8).
        """
        suffix = self.window_resolution.value
        updates: dict[str, str] = {}
        for field_name, prefix in _REDIS_FIELD_PREFIXES.items():
            value = getattr(self, field_name)
            if value is None:
                continue
            updates[f"{prefix}_{suffix}"] = _format_redis_value(value)
        return updates

    def to_avro_dict(self) -> dict:
        """Convert to the dict shape accepted by ``AvroSerializer``.

        Returns
        -------
        dict
            Avro-shaped dict; ``window_resolution`` is rendered as the Avro
            enum *symbol name* (e.g. ``"W_5M_SLIDE_1M"``).
        """
        return {
            "user_id": self.user_id,
            "window_resolution": self.window_resolution.name,
            "window_start_ms": self.window_start_ms,
            "window_end_ms": self.window_end_ms,
            "emission_seq": self.emission_seq,
            "click_count": self.click_count,
            "page_view_count": self.page_view_count,
            "purchase_count": self.purchase_count,
            "revenue": self.revenue,
            "distinct_products": self.distinct_products,
            "avg_purchase_amount": self.avg_purchase_amount,
        }

    @classmethod
    def from_avro_dict(cls, d: dict) -> SlidingFeatureRecord:
        """Reconstruct a record from an Avro-deserialized dict.

        Parameters
        ----------
        d : dict
            Dict produced by ``AvroDeserializer`` (or :meth:`to_avro_dict`),
            with ``window_resolution`` as the Avro enum symbol name.

        Returns
        -------
        SlidingFeatureRecord
            Reconstructed record.
        """
        return cls(
            user_id=d["user_id"],
            window_resolution=WindowResolution[d["window_resolution"]],
            window_start_ms=d["window_start_ms"],
            window_end_ms=d["window_end_ms"],
            emission_seq=d.get("emission_seq", 0),
            click_count=d.get("click_count"),
            page_view_count=d.get("page_view_count"),
            purchase_count=d.get("purchase_count"),
            revenue=d.get("revenue"),
            distinct_products=d.get("distinct_products"),
            avg_purchase_amount=d.get("avg_purchase_amount"),
        )


# Mapping from record field name to the Redis-field prefix (design doc §2.8 /
# §7.3): counts are pluralised, monetary / cardinality fields keep their names.
_REDIS_FIELD_PREFIXES: dict[str, str] = {
    "click_count": "clicks",
    "page_view_count": "page_views",
    "purchase_count": "purchases",
    "revenue": "revenue",
    "distinct_products": "distinct_products",
    "avg_purchase_amount": "avg_purchase_amount",
}


def _format_redis_value(value: int | float) -> str:
    """Render a numeric feature value for storage in a Redis hash.

    Parameters
    ----------
    value : int or float
        Feature value to encode.

    Returns
    -------
    str
        ``str(value)`` — integer counts stay integral, floats keep a decimal
        point so the read adapter can coerce unambiguously.
    """
    return str(value)


def event_timestamp_ms(event: EcommerceEvent) -> int:
    """Return an event's event-time as milliseconds since the Unix epoch.

    Parameters
    ----------
    event : EcommerceEvent
        Decoded event whose ``event_timestamp`` is a timezone-aware datetime.

    Returns
    -------
    int
        Event time in milliseconds since the epoch — the value the windowing
        layer keys panes and the watermark on (design doc §2.4).
    """
    return int(event.event_timestamp.timestamp() * 1000)


class SlidingConsumerConfig(BaseModel):
    """Runtime configuration for :class:`SlidingFeaturesConsumer` (design §3.3).

    Parameters
    ----------
    bootstrap : str
        Comma-separated Kafka bootstrap servers.
    registry_url : str
        Schema Registry base URL.
    source_topic : str
        Topic to consume (``validated-events``).
    sink_topic : str
        Topic for emitted feature records (``sliding-features``).
    late_sink_topic : str
        Side-output topic for very-late raw events (``sliding-features-late``).
    consumer_group : str
        Kafka consumer group id (``sliding-features-job``).
    out_of_orderness_seconds : int
        Watermark skew budget (design doc §2.4).
    idleness_seconds : int
        Wall-clock idleness before the watermark falls back (design doc §2.4).
    allowed_lateness_seconds : int
        Pane-retention / re-emission budget; must be below the smallest window
        (design doc §2.6).
    emit_tick_seconds : float
        Periodic emission tick when the poll loop is idle.
    poll_timeout_seconds : float
        Consumer ``poll`` timeout.
    isolation_level : str
        librdkafka ``isolation.level``.  Defaults to ``"read_committed"``
        because ``validated-events`` is produced transactionally once EOS is
        enabled, so this consumer must read only past the broker's Last Stable
        Offset and filter aborted records (design week2_03 §2.5).
    num_workers : int
        Number of processes in the consumer group (design doc §2.11).
    warmup_seek_back : bool
        Whether to seek back one window of event-time on assignment to rebuild
        pane state (design doc §2.10).
    redis_host, redis_port : str, int
        Redis connection for the online-store sink.
    ttl_factor : float
        Per-resolution TTL multiplier (TTL = ``ttl_factor × window_size``);
        design doc §2.7.
    """

    model_config = ConfigDict(extra="forbid")

    bootstrap: str = "kafka-1:9092,kafka-2:9092,kafka-3:9092"
    registry_url: str = "http://schema-registry:8081"
    source_topic: str = "validated-events"
    sink_topic: str = "sliding-features"
    late_sink_topic: str = "sliding-features-late"
    consumer_group: str = "sliding-features-job"

    out_of_orderness_seconds: int = Field(default=5, ge=0)
    idleness_seconds: int = Field(default=30, ge=0)
    allowed_lateness_seconds: int = Field(default=30, ge=0)
    emit_tick_seconds: float = Field(default=1.0, gt=0.0)
    poll_timeout_seconds: float = Field(default=1.0, gt=0.0)
    isolation_level: str = Field(
        default="read_committed",
        pattern=r"^(read_uncommitted|read_committed)$",
    )

    num_workers: int = Field(default=1, ge=1, le=12)
    warmup_seek_back: bool = True

    redis_host: str = "redis"
    redis_port: int = Field(default=6379, ge=1, le=65535)
    ttl_factor: float = Field(default=1.5, gt=0.0)

    @model_validator(mode="after")
    def _validate_cross_fields(self) -> SlidingConsumerConfig:
        """Enforce distinct topics and a lateness below the smallest window.

        Returns
        -------
        SlidingConsumerConfig
            The validated instance.

        Raises
        ------
        ValueError
            If the source / sink / late topics are not pairwise distinct, or if
            ``allowed_lateness_seconds`` is not strictly below the 5 m window.
        """
        topics = [self.source_topic, self.sink_topic, self.late_sink_topic]
        if len(set(topics)) != len(topics):
            raise ValueError(
                f"source/sink/late topics must be pairwise distinct; got {topics}"
            )
        smallest_window_s = WindowResolution.W_5M_SLIDE_1M.window_size_seconds
        if self.allowed_lateness_seconds >= smallest_window_s:
            raise ValueError(
                f"allowed_lateness_seconds must be below the smallest window "
                f"({smallest_window_s}s); got {self.allowed_lateness_seconds}"
            )
        return self

    def ttl_seconds_for(self, resolution: WindowResolution) -> int:
        """Return the Redis TTL for *resolution* (design doc §2.7).

        Parameters
        ----------
        resolution : WindowResolution
            Resolution whose TTL is requested.

        Returns
        -------
        int
            ``ttl_factor × window_size_seconds``, truncated to an int.
        """
        return int(self.ttl_factor * resolution.window_size_seconds)
