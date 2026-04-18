"""Walk-forward split generator for backtests.

Anchored walk-forward: the train window grows over time while the validate
window slides forward in fixed steps. This is what the risk committee
expects to see in a promotion package — parameters selected on train[0:k]
evaluated on validate[k:k+step], then the whole thing rolls.

SLICE 5 SKELETON. The splits are real and tested. What is NOT wired:

  * Param-sweep driver: given a set of YAML param candidates, run each
    candidate against every split's train -> validate pair and emit a
    results table. Blocked on a) tick archive availability and b) a
    `BacktestEngine.run(params, events)` orchestrator that threads params
    through `RiskSettings` (current `replay.py` hard-codes settings).
  * Multi-market splits: the hourly BTC market is single-instrument so
    per-market splits aren't required yet; generalize when Slice 2 adds
    a second market.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass

NS_PER_DAY = 24 * 60 * 60 * 1_000_000_000


@dataclass(frozen=True, slots=True)
class WalkForwardSplit:
    train_start_ns: int
    train_end_ns: int
    validate_start_ns: int
    validate_end_ns: int

    @property
    def train_days(self) -> float:
        return (self.train_end_ns - self.train_start_ns) / NS_PER_DAY

    @property
    def validate_days(self) -> float:
        return (self.validate_end_ns - self.validate_start_ns) / NS_PER_DAY


def walk_forward_splits(
    *,
    total_start_ns: int,
    total_end_ns: int,
    train_days: int,
    validate_days: int,
    anchored: bool = True,
) -> Iterator[WalkForwardSplit]:
    """Yield non-overlapping validation windows over [total_start, total_end).

    With `anchored=True` the train window starts at `total_start_ns` and
    grows monotonically (standard walk-forward). With `anchored=False` the
    train window slides — each split uses the most recent `train_days`.

    Validation windows never overlap; this is the invariant that makes
    aggregated validate-period metrics trustworthy. A split is yielded
    only if both its train and validate windows fit entirely in
    [total_start_ns, total_end_ns).
    """
    if train_days <= 0 or validate_days <= 0:
        raise ValueError("train_days and validate_days must be > 0")
    if total_end_ns <= total_start_ns:
        raise ValueError("total_end_ns must be > total_start_ns")

    train_ns = train_days * NS_PER_DAY
    val_ns = validate_days * NS_PER_DAY
    validate_start = total_start_ns + train_ns
    while validate_start + val_ns <= total_end_ns:
        train_start = total_start_ns if anchored else validate_start - train_ns
        yield WalkForwardSplit(
            train_start_ns=train_start,
            train_end_ns=validate_start,
            validate_start_ns=validate_start,
            validate_end_ns=validate_start + val_ns,
        )
        validate_start += val_ns
