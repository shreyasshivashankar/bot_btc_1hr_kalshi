"""Pure risk gate — `risk.check()` runs before every order submission.

It is deliberately a pure function of its inputs (no I/O, no hidden state) so
it can be unit-tested deterministically and audited.

Checks, in order (first reject wins):
  1. contracts > 0
  2. no breaker tripped
  3. signal confidence >= configured floor (defense-in-depth — registry already
     filtered, but we re-check here)
  4. correlation cap: count of open positions on the SAME settlement hour +
     SAME side is below `max_correlated_positions`. Under the multi-strike
     architecture, three YES bets on adjacent strikes of the same hourly
     session are structurally one directional bet on BTC — the aggregate
     notional cap alone does not enforce diversification intent.
  5. daily-loss ceiling not breached
  6. per-position notional cap
  7. aggregate open-exposure cap (3x single-position cap by default)
"""

from __future__ import annotations

from dataclasses import dataclass

from bot_btc_1hr_kalshi.config.settings import RiskSettings
from bot_btc_1hr_kalshi.risk.breakers import BreakerState
from bot_btc_1hr_kalshi.signal.types import TrapSignal

AGGREGATE_EXPOSURE_MULT = 3.0


@dataclass(frozen=True, slots=True)
class RiskInput:
    signal: TrapSignal
    contracts: int
    bankroll_usd: float
    open_positions_notional_usd: float
    daily_realized_pnl_usd: float
    breakers: BreakerState
    now_ns: int
    min_signal_confidence: float
    # Count of open positions on the same (settlement_ts_ns, side) as the
    # pending signal. Computed by the caller (OMS) against the Portfolio;
    # defaults to 0 so legacy tests that pre-date the correlation cap stay
    # green when the cap is the default (>=1).
    correlated_open_positions_count: int = 0


@dataclass(frozen=True, slots=True)
class Approve:
    contracts: int


@dataclass(frozen=True, slots=True)
class Reject:
    reason: str


RiskDecision = Approve | Reject


def check(req: RiskInput, settings: RiskSettings) -> RiskDecision:
    if req.contracts <= 0:
        return Reject("zero_contracts")

    if req.breakers.any_tripped(req.now_ns):
        return Reject(f"breaker_tripped:{req.breakers.reason(req.now_ns)}")

    if req.signal.confidence < req.min_signal_confidence:
        return Reject("below_confidence_floor")

    if req.correlated_open_positions_count >= settings.max_correlated_positions:
        return Reject("correlation_cap")

    loss_cap_usd = -settings.max_daily_loss_pct * req.bankroll_usd
    if req.daily_realized_pnl_usd <= loss_cap_usd:
        return Reject("daily_loss_limit")

    notional_usd = req.contracts * (req.signal.entry_price_cents / 100.0)
    if notional_usd > settings.max_position_notional_usd:
        return Reject("position_notional_cap")

    if (
        req.open_positions_notional_usd + notional_usd
        > settings.max_position_notional_usd * AGGREGATE_EXPOSURE_MULT
    ):
        return Reject("aggregate_exposure_cap")

    return Approve(req.contracts)
