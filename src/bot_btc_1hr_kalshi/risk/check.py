"""Pure risk gate — `risk.check()` runs before every order submission.

It is deliberately a pure function of its inputs (no I/O, no hidden state) so
it can be unit-tested deterministically and audited.

Checks, in order (first reject wins):
  1. contracts > 0
  2. no breaker tripped
  3. calendar blockout window (tier-1 macro event at T-60s .. T+30min) —
     docs/RISK.md §Macro-blockers. `CalendarGuard.tick()` already drives a
     pre-event flatten; this gate is the "no new entries" half of the window.
  4. signal confidence >= configured floor (defense-in-depth — registry already
     filtered, but we re-check here)
  5. premium cap: entry price <= `max_entry_price_cents` (Slice 11 Phase 3.1).
     Inverted-risk guard — at 75¢ you risk 75 to make 25, and one loss erases
     three wins. Kelly's math alone tolerates this; prudent practice does not.
  6. correlation cap: count of open positions on the SAME settlement hour +
     SAME side is below `max_correlated_positions`. Under the multi-strike
     architecture, three YES bets on adjacent strikes of the same hourly
     session are structurally one directional bet on BTC — the aggregate
     notional cap alone does not enforce diversification intent.
  7. daily-loss ceiling not breached
  8. per-position notional cap
  9. aggregate open-exposure cap (3x single-position cap by default)

Ordering note: confidence floor runs BEFORE premium/correlation caps so
below-confidence ticks don't pollute the decision journal with cap rejects
that would never have fired if the signal were strong enough to surface.
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
    # Tier-1 macro-event blockout (docs/RISK.md §Macro-blockers, Slice 11 P1).
    # True when `now_ns` falls inside `[ev.ts_ns - lead_ns, ev.ts_ns +
    # cooldown_ns]` for any tier-1 `ScheduledEvent`. Computed by the caller
    # via `CalendarGuard.is_blocked(now_ns)`; defaults to False so legacy
    # tests (and the calendar-disabled path) stay green.
    calendar_blocked: bool = False


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

    if req.calendar_blocked:
        return Reject("calendar_blocked")

    if req.signal.confidence < req.min_signal_confidence:
        return Reject("below_confidence_floor")

    if req.signal.entry_price_cents > settings.max_entry_price_cents:
        return Reject("premium_cap")

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
