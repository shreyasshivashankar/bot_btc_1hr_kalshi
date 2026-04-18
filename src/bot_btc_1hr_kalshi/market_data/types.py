"""Immutable value types for market data.

Kalshi L2 updates and spot-venue trades flow through these types. Frozen/slotted
dataclasses keep allocation cheap on the hot path.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from bot_btc_1hr_kalshi.obs.money import MICROS_PER_USD, Micros
from bot_btc_1hr_kalshi.obs.schemas import Side

Venue = Literal["kalshi", "coinbase", "kraken"]
AggressorSide = Literal["buy", "sell"]


@dataclass(frozen=True, slots=True)
class BookLevel:
    price_cents: int
    size: int


@dataclass(frozen=True, slots=True)
class BookUpdate:
    """A Kalshi L2 update.

    `is_snapshot=True`: each `BookLevel.size` is the absolute resting quantity
    at that price (0 = no resting liquidity; the book replaces its state).

    `is_snapshot=False`: each `BookLevel.size` is a *signed delta* — the
    quantity change at that price level. The consumer accumulates it onto
    the existing size and removes the level if the running total falls to
    ≤0. This matches Kalshi's wire semantics; masking negative deltas to 0
    would wipe out resting liquidity on every partial fill (see
    `L2Book.apply`)."""

    seq: int
    ts_ns: int
    market_id: str
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    is_snapshot: bool


@dataclass(frozen=True, slots=True)
class TradeEvent:
    """A printed trade on Kalshi."""

    seq: int
    ts_ns: int
    market_id: str
    price_cents: int
    size: int
    aggressor: AggressorSide
    taker_side: Side


@dataclass(frozen=True, slots=True)
class SpotTick:
    """A trade print on a spot venue (Coinbase primary / Kraken confirmation).

    Price is stored as integer micro-dollars (1 USD = 1_000_000 micros).
    BTC-USD spot venues quote to cent precision; micros gives us 4 extra
    decimals of headroom for sub-cent aggregations and keeps the feature
    engine's rolling windows out of float space entirely. `price_usd` is
    a read-only convenience property for logging and human-facing
    telemetry — never use it inside arithmetic accumulations."""

    ts_ns: int
    venue: Venue
    price_micros: Micros
    size: float

    @property
    def price_usd(self) -> float:
        return self.price_micros / MICROS_PER_USD


FeedEvent = BookUpdate | TradeEvent | SpotTick
