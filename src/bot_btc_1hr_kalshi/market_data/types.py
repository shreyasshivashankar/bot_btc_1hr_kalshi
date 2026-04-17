"""Immutable value types for market data.

Kalshi L2 updates and spot-venue trades flow through these types. Frozen/slotted
dataclasses keep allocation cheap on the hot path.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from bot_btc_1hr_kalshi.obs.schemas import Side

Venue = Literal["kalshi", "coinbase", "binance"]
AggressorSide = Literal["buy", "sell"]


@dataclass(frozen=True, slots=True)
class BookLevel:
    price_cents: int
    size: int


@dataclass(frozen=True, slots=True)
class BookUpdate:
    """A Kalshi L2 update. `is_snapshot=True` replaces the book; False applies
    as a delta where size=0 means remove that price level."""

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
    """A trade print on a spot venue (Coinbase/Binance)."""

    ts_ns: int
    venue: Venue
    price_usd: float
    size: float


FeedEvent = BookUpdate | TradeEvent | SpotTick
