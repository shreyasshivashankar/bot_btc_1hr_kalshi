"""Signal-layer types: the inputs a trap sees and the output it produces."""

from __future__ import annotations

from dataclasses import dataclass

from bot_btc_1hr_kalshi.market_data import L2Book
from bot_btc_1hr_kalshi.obs.schemas import Features, Side, TrapName


@dataclass(frozen=True, slots=True)
class MarketSnapshot:
    """Everything a trap needs to decide at a single point in time."""

    market_id: str
    book: L2Book
    features: Features
    spot_btc_usd: float
    minutes_to_settlement: float


@dataclass(frozen=True, slots=True)
class TrapSignal:
    """A trap's proposal to open a position. Risk still has final say."""

    trap: TrapName
    side: Side
    entry_price_cents: int
    confidence: float
    edge_cents: float
    features: Features
