"""In-memory portfolio: open positions + cash + PnL tracking.

Broker state is authoritative (hard rule #7). This is the local projection used
between reconciles, and is rebuilt from the broker on each reconcile tick.

Position convention: we always *buy* a side's contract. A YES-buy at 30c pays
(100 - 30) = 70c per contract if YES settles, and loses 30c otherwise. At any
mid-tick, gross PnL on that leg = contracts * (mark_cents - entry_cents) / 100.
Symmetric for NO-buys: entry is the NO-side price, mark is the NO-side mark.

Units: bankroll and PnL are stored internally as integer micro-dollars
(1 USD = 1_000_000 micros) so that a long sequence of small credits/debits
never accumulates float drift. The `*_usd` properties convert back to float
at read-time for risk/OMS/telemetry. Only the internal running counters use
micros — per-bet amounts (BetOutcome, OpenPosition.fees_paid_usd) stay in
float USD so the BigQuery schema and broker Fill surface are unchanged.
"""

from __future__ import annotations

from dataclasses import dataclass

from bot_btc_1hr_kalshi.execution.broker.base import Fill
from bot_btc_1hr_kalshi.obs.schemas import BetOutcome, ExitReason, Features, Side

_MICROS_PER_USD = 1_000_000


def _to_micros(usd: float) -> int:
    return round(usd * _MICROS_PER_USD)


@dataclass(slots=True)
class OpenPosition:
    position_id: str
    decision_id: str
    market_id: str
    side: Side
    entry_price_cents: int
    contracts: int
    opened_at_ns: int
    fees_paid_usd: float
    trap: str
    features_at_entry: Features

    @property
    def notional_usd(self) -> float:
        return self.contracts * self.entry_price_cents / 100.0


@dataclass(slots=True, init=False)
class Portfolio:
    _bankroll_micros: int
    _positions: dict[str, OpenPosition]
    _daily_realized_pnl_micros: int
    _lifetime_realized_pnl_micros: int

    def __init__(self, *, bankroll_usd: float) -> None:
        self._bankroll_micros = _to_micros(bankroll_usd)
        self._positions = {}
        self._daily_realized_pnl_micros = 0
        self._lifetime_realized_pnl_micros = 0

    @property
    def bankroll_usd(self) -> float:
        return self._bankroll_micros / _MICROS_PER_USD

    @property
    def open_positions(self) -> tuple[OpenPosition, ...]:
        return tuple(self._positions.values())

    @property
    def open_positions_notional_usd(self) -> float:
        return sum(p.notional_usd for p in self._positions.values())

    @property
    def daily_realized_pnl_usd(self) -> float:
        return self._daily_realized_pnl_micros / _MICROS_PER_USD

    def get(self, position_id: str) -> OpenPosition | None:
        return self._positions.get(position_id)

    def has(self, position_id: str) -> bool:
        return position_id in self._positions

    def open_from_fill(
        self,
        *,
        position_id: str,
        decision_id: str,
        fill: Fill,
        trap: str,
        features_at_entry: Features,
    ) -> OpenPosition:
        if position_id in self._positions:
            raise ValueError(f"position already open: {position_id}")
        if fill.action != "BUY":
            raise ValueError(f"entry fill must be a BUY, got {fill.action}")
        pos = OpenPosition(
            position_id=position_id,
            decision_id=decision_id,
            market_id=fill.market_id,
            side=fill.side,
            entry_price_cents=fill.price_cents,
            contracts=fill.contracts,
            opened_at_ns=fill.ts_ns,
            fees_paid_usd=fill.fees_usd,
            trap=trap,
            features_at_entry=features_at_entry,
        )
        self._positions[position_id] = pos
        self._bankroll_micros -= _to_micros(pos.notional_usd + fill.fees_usd)
        return pos

    def close(
        self,
        *,
        position_id: str,
        exit_fill: Fill,
        exit_reason: ExitReason,
        counterfactual_held_pnl_usd: float | None = None,
    ) -> BetOutcome:
        pos = self._positions.pop(position_id, None)
        if pos is None:
            raise ValueError(f"no open position: {position_id}")
        if exit_fill.action != "SELL":
            raise ValueError(f"exit fill must be a SELL, got {exit_fill.action}")
        if exit_fill.contracts != pos.contracts:
            raise ValueError(
                f"partial close unsupported: filled {exit_fill.contracts} of {pos.contracts}"
            )

        gross = pos.contracts * (exit_fill.price_cents - pos.entry_price_cents) / 100.0
        total_fees = pos.fees_paid_usd + exit_fill.fees_usd
        net = gross - total_fees

        self._bankroll_micros += _to_micros(
            pos.contracts * exit_fill.price_cents / 100.0 - exit_fill.fees_usd
        )
        net_micros = _to_micros(net)
        self._daily_realized_pnl_micros += net_micros
        self._lifetime_realized_pnl_micros += net_micros

        hold_sec = max(0.0, (exit_fill.ts_ns - pos.opened_at_ns) / 1_000_000_000)
        return BetOutcome(
            bet_id=position_id,
            decision_id=pos.decision_id,
            market_id=pos.market_id,
            trap=pos.trap,  # type: ignore[arg-type]
            side=pos.side,
            opened_at_ns=pos.opened_at_ns,
            closed_at_ns=exit_fill.ts_ns,
            hold_duration_sec=hold_sec,
            entry_price_cents=pos.entry_price_cents,
            exit_price_cents=exit_fill.price_cents,
            contracts=pos.contracts,
            gross_pnl_usd=gross,
            fees_usd=total_fees,
            net_pnl_usd=net,
            counterfactual_held_pnl_usd=counterfactual_held_pnl_usd,
            exit_reason=exit_reason,
            features_at_entry=pos.features_at_entry,
        )

    def partial_close(
        self,
        *,
        position_id: str,
        exit_fill: Fill,
        exit_reason: ExitReason,
        partial_seq: int,
        counterfactual_held_pnl_usd: float | None = None,
    ) -> BetOutcome:
        """Close part of a position (e.g. IOC exit that partially filled).

        Shrinks the open position by `exit_fill.contracts`, credits bankroll for
        the partial fill, emits a `BetOutcome` tagged with `bet_id = {position_id}-p{seq}`
        so BigQuery keeps a distinct record per partial. Entry fees are allocated
        proportionally to the closed slice.
        """
        pos = self._positions.get(position_id)
        if pos is None:
            raise ValueError(f"no open position: {position_id}")
        if exit_fill.action != "SELL":
            raise ValueError(f"exit fill must be a SELL, got {exit_fill.action}")
        closed = exit_fill.contracts
        if closed <= 0 or closed >= pos.contracts:
            raise ValueError(
                f"partial close must be 1..{pos.contracts - 1}, got {closed}"
            )

        entry_fee_share = pos.fees_paid_usd * closed / pos.contracts
        gross = closed * (exit_fill.price_cents - pos.entry_price_cents) / 100.0
        total_fees = entry_fee_share + exit_fill.fees_usd
        net = gross - total_fees

        self._bankroll_micros += _to_micros(
            closed * exit_fill.price_cents / 100.0 - exit_fill.fees_usd
        )
        net_micros = _to_micros(net)
        self._daily_realized_pnl_micros += net_micros
        self._lifetime_realized_pnl_micros += net_micros

        # Shrink the live position in place. OpenPosition is mutable (non-frozen
        # dataclass); remaining entry-fee share stays with the open remainder.
        pos.contracts -= closed
        pos.fees_paid_usd -= entry_fee_share

        hold_sec = max(0.0, (exit_fill.ts_ns - pos.opened_at_ns) / 1_000_000_000)
        return BetOutcome(
            bet_id=f"{position_id}-p{partial_seq}",
            decision_id=pos.decision_id,
            market_id=pos.market_id,
            trap=pos.trap,  # type: ignore[arg-type]
            side=pos.side,
            opened_at_ns=pos.opened_at_ns,
            closed_at_ns=exit_fill.ts_ns,
            hold_duration_sec=hold_sec,
            entry_price_cents=pos.entry_price_cents,
            exit_price_cents=exit_fill.price_cents,
            contracts=closed,
            gross_pnl_usd=gross,
            fees_usd=total_fees,
            net_pnl_usd=net,
            counterfactual_held_pnl_usd=counterfactual_held_pnl_usd,
            exit_reason=exit_reason,
            features_at_entry=pos.features_at_entry,
        )

    def settle(
        self,
        *,
        position_id: str,
        settlement_cents: int,
        settled_at_ns: int,
        counterfactual_held_pnl_usd: float | None = None,
    ) -> BetOutcome:
        """Mark a position as settled at 0c or 100c (no exit fill — Kalshi settlement)."""
        pos = self._positions.pop(position_id, None)
        if pos is None:
            raise ValueError(f"no open position: {position_id}")
        if settlement_cents not in (0, 100):
            raise ValueError(f"settlement must be 0 or 100, got {settlement_cents}")

        gross = pos.contracts * (settlement_cents - pos.entry_price_cents) / 100.0
        net = gross - pos.fees_paid_usd

        self._bankroll_micros += _to_micros(pos.contracts * settlement_cents / 100.0)
        net_micros = _to_micros(net)
        self._daily_realized_pnl_micros += net_micros
        self._lifetime_realized_pnl_micros += net_micros

        return BetOutcome(
            bet_id=position_id,
            decision_id=pos.decision_id,
            market_id=pos.market_id,
            trap=pos.trap,  # type: ignore[arg-type]
            side=pos.side,
            opened_at_ns=pos.opened_at_ns,
            closed_at_ns=settled_at_ns,
            hold_duration_sec=max(0.0, (settled_at_ns - pos.opened_at_ns) / 1_000_000_000),
            entry_price_cents=pos.entry_price_cents,
            exit_price_cents=None,
            contracts=pos.contracts,
            gross_pnl_usd=gross,
            fees_usd=pos.fees_paid_usd,
            net_pnl_usd=net,
            counterfactual_held_pnl_usd=counterfactual_held_pnl_usd,
            exit_reason="settled",
            features_at_entry=pos.features_at_entry,
        )

    def reset_daily_pnl(self) -> None:
        self._daily_realized_pnl_micros = 0
