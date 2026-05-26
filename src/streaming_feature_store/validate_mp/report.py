"""Pydantic models for the multi-process validator run.

Mirrors :mod:`streaming_feature_store.consume_mp.report`: a parent-level
:class:`MultiprocessValidatorConfig`, per-member
:class:`ValidatorOutcome`, and an aggregate
:class:`MultiprocessValidatorReport`.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from streaming_feature_store.validate.report import ValidatorRunReport
from streaming_feature_store.validate.runner import ValidatorRunConfig


class MultiprocessValidatorConfig(BaseModel):
    """Configuration for a multi-process validator run.

    Parameters
    ----------
    members : int
        Number of member processes to spawn.  ``>= 1``.
    base_config : ValidatorRunConfig
        Shared per-member config.  Every member inherits this verbatim;
        the broker (not the app) assigns each a disjoint partition
        subset using ``base_config.consumer_group_id``.
    """

    model_config = ConfigDict(frozen=True)

    members: int = Field(..., ge=1)
    base_config: ValidatorRunConfig

    def to_per_process_run_config(self, process_index: int) -> ValidatorRunConfig:
        """Return the per-member :class:`ValidatorRunConfig`.

        Parameters
        ----------
        process_index : int
            Zero-based member index.  Must be ``>= 0`` and ``< members``.

        Returns
        -------
        ValidatorRunConfig
            A copy of :attr:`base_config` (every member is identical;
            the broker shards the work).

        Raises
        ------
        ValueError
            If ``process_index`` is out of range.
        """
        if not (0 <= process_index < self.members):
            raise ValueError(
                f"process_index must be in [0, {self.members}), "
                f"got {process_index}"
            )
        return self.base_config.model_copy()


class ValidatorOutcome(BaseModel):
    """One member process's contribution to the aggregate report.

    Parameters
    ----------
    process_index : int
        Zero-based member index.
    report : ValidatorRunReport
        The per-member :class:`ValidatorRunReport`.
    """

    model_config = ConfigDict(frozen=True)

    process_index: int = Field(..., ge=0)
    report: ValidatorRunReport


class MultiprocessValidatorReport(BaseModel):
    """Aggregate result of a multi-process validator run.

    Parameters
    ----------
    config : MultiprocessValidatorConfig
        Parent-level config used for the run.
    started_at : datetime
        Wall-clock start of the run (parent process, UTC).
    process_outcomes : list of ValidatorOutcome
        Per-member outcomes, ordered by ``process_index``.
    total_consumed : int
        Sum of per-member ``snapshot.consumed``.
    total_validated : int
        Sum of per-member ``snapshot.validated``.
    total_invalid : int
        Sum of per-member ``snapshot.invalid_total``.
    sustained_consume_eps : float
        ``total_consumed / max_member_wallclock_s``.
    """

    model_config = ConfigDict(frozen=True)

    config: MultiprocessValidatorConfig
    started_at: datetime
    process_outcomes: list[ValidatorOutcome]
    total_consumed: int
    total_validated: int
    total_invalid: int
    sustained_consume_eps: float
