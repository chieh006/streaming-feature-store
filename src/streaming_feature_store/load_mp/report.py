"""Pydantic models and Markdown renderer for the multi-process load run.

The threading runner already defines
:class:`~streaming_feature_store.load.report.LoadRunConfig` and
:class:`~streaming_feature_store.load.report.LoadRunReport`; this module
adds the *outer* multi-process wrappers:

* :class:`MultiprocessLoadConfig` — the parent-process knobs (total target
  rate, ``processes``, ``workers_per_process``).
* :class:`ProcessOutcome` — one child's report plus its raw latency
  samples (needed to re-percentile across processes).
* :class:`MultiprocessLoadReport` — aggregate result.

The per-process :class:`LoadRunConfig` is derived from
:class:`MultiprocessLoadConfig` via :meth:`MultiprocessLoadConfig.to_per_process_run_config`.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict, Field, field_validator

from streaming_feature_store.load.accountant import AccountantSnapshot
from streaming_feature_store.load.report import LoadRunConfig, LoadRunReport

logger = logging.getLogger(__name__)

_SEED_DECORRELATION_PRIME: int = 1_000_003


class MultiprocessLoadConfig(BaseModel):
    """Configuration for a multi-process load run.

    Parameters
    ----------
    duration_s : float
        Wall-clock duration per child process.  Must be ``> 0``.
    target_rate : float or None
        Aggregate target events/sec across all processes; ``None`` disables
        pacing.  Each child paces at ``target_rate / processes``.
    processes : int
        Number of producer processes.  Must be ``>= 1``.
    workers_per_process : int
        Worker threads inside each process.  Must be ``>= 1``.
    batch_size : int, optional
        Synthetic generator batch size per loop iteration.  Defaults to
        ``1024``.
    max_in_flight : int, optional
        In-process backpressure cap.  Applied **per process** (each child
        accountant tracks its own in-flight count).  Defaults to
        ``50_000``.
    seed : int, optional
        Seed for the parent run.  Each child receives
        ``seed + process_index * 1_000_003`` to decorrelate streams.
        Defaults to ``42``.
    topic : str, optional
        Target Kafka topic.  Defaults to ``"e-commerce-events"``.
    eos : bool, optional
        Records which producer profile the run used, purely for the
        rendered report.  ``True`` = EOS (idempotent, ``acks=all``,
        ``max.in.flight=5``); ``False`` (default) = throughput
        (``acks=1``, no idempotence).  The actual switch is the
        ``KAFKA_PRODUCER_ENABLE_IDEMPOTENCE`` env var read by
        :class:`~streaming_feature_store.config.ProducerTuning` in each
        child; this flag only labels the artifact.

    Notes
    -----
    The fields here describe the **parent's** intent; the per-process
    :class:`LoadRunConfig` is derived via :meth:`to_per_process_run_config`.
    """

    model_config = ConfigDict(frozen=True)

    duration_s: float = Field(..., gt=0)
    target_rate: float | None = None
    processes: int = Field(..., ge=1)
    workers_per_process: int = Field(..., ge=1)
    batch_size: int = Field(default=1024, ge=1)
    max_in_flight: int = Field(default=50_000, ge=1)
    seed: int = 42
    topic: str = "e-commerce-events"
    eos: bool = False

    @field_validator("target_rate")
    @classmethod
    def _target_rate_positive_or_none(cls, value: float | None) -> float | None:
        """Reject ``target_rate <= 0`` (``None`` is allowed).

        Parameters
        ----------
        value : float or None
            Candidate value.

        Returns
        -------
        float or None
            Validated value.
        """
        if value is not None and value <= 0:
            raise ValueError(f"target_rate must be > 0 or None, got {value}")
        return value

    def to_per_process_run_config(self, process_index: int) -> LoadRunConfig:
        """Derive the :class:`LoadRunConfig` for child ``process_index``.

        Parameters
        ----------
        process_index : int
            Zero-based child index.  Must be ``>= 0`` and ``< self.processes``.

        Returns
        -------
        LoadRunConfig
            Per-process config with ``target_rate / processes`` and a
            decorrelated seed.

        Raises
        ------
        ValueError
            If ``process_index`` is out of range.
        """
        if not (0 <= process_index < self.processes):
            raise ValueError(
                f"process_index must be in [0, {self.processes}), "
                f"got {process_index}"
            )
        per_proc_rate: float | None
        if self.target_rate is None:
            per_proc_rate = None
        else:
            per_proc_rate = float(self.target_rate) / float(self.processes)
        return LoadRunConfig(
            duration_s=self.duration_s,
            target_rate=per_proc_rate,
            workers=self.workers_per_process,
            batch_size=self.batch_size,
            max_in_flight=self.max_in_flight,
            seed=self.seed + process_index * _SEED_DECORRELATION_PRIME,
            topic=self.topic,
        )


class ProcessOutcome(BaseModel):
    """One child process's contribution to the aggregate report.

    Parameters
    ----------
    process_index : int
        Zero-based child index.  ``>= 0``.
    report : LoadRunReport
        The per-process report returned by the child's
        :class:`~streaming_feature_store.load.load_runner.LoadRunner`.
    latency_samples_s : list of float
        Raw reservoir samples (seconds) from the child's accountant.  The
        parent re-percentiles the union of these lists to obtain the
        aggregate p50 / p95 / p99.
    """

    model_config = ConfigDict(frozen=True)

    process_index: int = Field(..., ge=0)
    report: LoadRunReport
    latency_samples_s: list[float] = Field(default_factory=list)


class MultiprocessLoadReport(BaseModel):
    """Aggregate result of a multi-process load run.

    Parameters
    ----------
    config : MultiprocessLoadConfig
        The parent-level config used for the run.
    started_at : datetime
        Wall-clock start of the run (parent process, UTC).
    process_outcomes : list of ProcessOutcome
        Per-child reports.  Length equals ``config.processes``.
    aggregate_snapshot : AccountantSnapshot
        Counters summed across processes; ``ack_latency_p*`` recomputed
        from the union of per-process reservoirs.
    sustained_rate_eps : float
        ``aggregate_snapshot.acked / max_child_wallclock_s``.
    floor_eps : float
        Throughput floor used for the pass / fail verdict.
    """

    model_config = ConfigDict(frozen=True)

    config: MultiprocessLoadConfig
    started_at: datetime
    process_outcomes: list[ProcessOutcome]
    aggregate_snapshot: AccountantSnapshot
    sustained_rate_eps: float
    floor_eps: float = 50_000.0

    @property
    def passed(self) -> bool:
        """Return ``True`` iff sustained rate meets the floor and no failures.

        Returns
        -------
        bool
            Pass / fail verdict.
        """
        return (
            self.sustained_rate_eps >= self.floor_eps
            and self.aggregate_snapshot.failed == 0
        )


def _format_target(rate: float | None) -> str:
    """Format a target-rate value for the rendered report.

    Parameters
    ----------
    rate : float or None
        Target rate.

    Returns
    -------
    str
        Human-readable value.
    """
    if rate is None:
        return "unpaced"
    return f"{int(rate):_} evt/s"


def render_markdown(report: MultiprocessLoadReport) -> str:
    """Render *report* as the Markdown artifact written to ``docs/results/``.

    Parameters
    ----------
    report : MultiprocessLoadReport
        Aggregate result.

    Returns
    -------
    str
        Markdown text.
    """
    cfg = report.config
    snap = report.aggregate_snapshot
    verdict_mark = "✅" if report.passed else "❌"
    verdict_word = "PASSED" if report.passed else "FAILED"
    started = report.started_at.astimezone(timezone.utc).isoformat()
    lines: list[str] = []
    lines.append("# Multi-Process Synthetic Event Load Test Results")
    lines.append("")
    lines.append(f"**Generated:** {started}")
    lines.append(f"**Topic:** {cfg.topic}")
    lines.append("")
    lines.append("## Configuration")
    lines.append("")
    lines.append("| Field | Value |")
    lines.append("|---|---|")
    lines.append(f"| Duration | {cfg.duration_s} s |")
    lines.append(f"| Target rate (aggregate) | {_format_target(cfg.target_rate)} |")
    lines.append(f"| Processes | {cfg.processes} |")
    lines.append(f"| Workers per process | {cfg.workers_per_process} |")
    lines.append(f"| Batch size | {cfg.batch_size} |")
    lines.append(f"| Max in-flight (per process) | {cfg.max_in_flight} |")
    lines.append(f"| Seed | {cfg.seed} |")
    if cfg.eos:
        profile = "EOS (idempotent, acks=all, max.in.flight=5)"
    else:
        profile = "throughput (acks=1, no idempotence)"
    lines.append(f"| Producer profile | {profile} |")
    lines.append("")
    lines.append("## Aggregate results")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|---|---|")
    lines.append(f"| Produced | {snap.produced:_} |")
    lines.append(f"| Acked | {snap.acked:_} |")
    lines.append(f"| Failed | {snap.failed:_} |")
    lines.append(
        f"| Sustained rate | {report.sustained_rate_eps:,.0f} evt/s "
        f"{verdict_mark} (floor {int(report.floor_eps):_}) |"
    )
    lines.append(
        f"| Ack latency p50 / p95 / p99 | "
        f"{snap.ack_latency_p50_ms:.1f} / "
        f"{snap.ack_latency_p95_ms:.1f} / "
        f"{snap.ack_latency_p99_ms:.1f} ms |"
    )
    lines.append(f"| Errors by class | {snap.errors_by_class} |")
    lines.append(f"| Max child wallclock | {snap.wallclock_s:.2f} s |")
    lines.append("")
    lines.append("## Per-process breakdown")
    lines.append("")
    lines.append(
        "| # | Produced | Acked | Failed | Sustained evt/s | "
        "p50 ms | p95 ms | p99 ms | Wallclock s |"
    )
    lines.append("|---|---|---|---|---|---|---|---|---|")
    for outcome in report.process_outcomes:
        psnap = outcome.report.snapshot
        lines.append(
            f"| {outcome.process_index} | {psnap.produced:_} | "
            f"{psnap.acked:_} | {psnap.failed:_} | "
            f"{outcome.report.sustained_rate_eps:,.0f} | "
            f"{psnap.ack_latency_p50_ms:.1f} | "
            f"{psnap.ack_latency_p95_ms:.1f} | "
            f"{psnap.ack_latency_p99_ms:.1f} | "
            f"{psnap.wallclock_s:.2f} |"
        )
    lines.append("")
    lines.append("## Verdict")
    lines.append("")
    lines.append(
        f"{verdict_mark} {verdict_word}: sustained "
        f"{report.sustained_rate_eps:,.0f} evt/s vs floor "
        f"{int(report.floor_eps):_} evt/s."
    )
    lines.append("")
    return "\n".join(lines)
