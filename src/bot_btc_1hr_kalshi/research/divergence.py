"""Backtest <-> live divergence harness.

The fear: backtest picks a param, paper validates it, live ships — and
some subtle path difference (book snapshot rebuild, clock rounding,
IOC escalation timing) means the live code is effectively running a
different strategy than backtest measured. This module compares two
DecisionRecord streams from the SAME tick input and flags divergences.

The check is strict: same decision_id space is not required (UUIDs
differ across runs), but at each tick boundary the pair of runs must
agree on the decision sequence — approve/reject, trap name, side,
rounded price in cents, and approved-contract count. Differences in
order are legitimate only if neither produced a decision at that tick.

SLICE 5 SKELETON. The comparator is real and tested. What is NOT wired:

  * The driver that runs the backtest engine and the live code path
    against a captured tick stream side-by-side. Blocked on a) tick
    capture being live (gs://bot-btc-1hr-kalshi-tick-archive-*) and
    b) a "live-decision-only" shim that runs the live signal/risk/OMS
    wiring with the ShadowBroker so no orders go out. Partial
    implementation: ShadowBroker exists (Slice 4F) — the remaining
    piece is a harness entrypoint and a captured fixture stream.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from bot_btc_1hr_kalshi.obs.schemas import DecisionRecord


@dataclass(frozen=True, slots=True)
class DecisionMismatch:
    index: int
    field: str
    a_value: object
    b_value: object


@dataclass(frozen=True, slots=True)
class DivergenceReport:
    n_a: int
    n_b: int
    mismatches: list[DecisionMismatch] = field(default_factory=list)

    @property
    def diverged(self) -> bool:
        return self.n_a != self.n_b or bool(self.mismatches)


# Comparison keys: fields that must match across runs for the strategy to
# be considered reproducible. decision_id + ts_ns are deliberately OMITTED —
# UUIDs differ per run and ns drift is legitimate if the injected clock is
# derived differently. Price-in-cents matches because rounding happens
# inside the signal layer, which is deterministic.
_COMPARE_FIELDS = (
    "trap",
    "side",
    "entry_price_cents",
    "approved",
    "reject_reason",
)


def compare_decisions(
    a: list[DecisionRecord], b: list[DecisionRecord]
) -> DivergenceReport:
    mismatches: list[DecisionMismatch] = []
    n = min(len(a), len(b))
    for i in range(n):
        for fld in _COMPARE_FIELDS:
            va = getattr(a[i], fld)
            vb = getattr(b[i], fld)
            if va != vb:
                mismatches.append(DecisionMismatch(
                    index=i, field=fld, a_value=va, b_value=vb,
                ))
        # Contract count mismatches via sizing.contracts — nested
        # so handled explicitly.
        if a[i].sizing.contracts != b[i].sizing.contracts:
            mismatches.append(DecisionMismatch(
                index=i, field="sizing.contracts",
                a_value=a[i].sizing.contracts, b_value=b[i].sizing.contracts,
            ))
    return DivergenceReport(n_a=len(a), n_b=len(b), mismatches=mismatches)
