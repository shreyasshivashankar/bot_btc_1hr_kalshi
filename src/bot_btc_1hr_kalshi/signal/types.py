"""Signal-layer types: the inputs a trap sees and the output it produces."""

from __future__ import annotations

from dataclasses import dataclass

from bot_btc_1hr_kalshi.market_data import L2Book
from bot_btc_1hr_kalshi.market_data.types import (
    LiquidationHeatmapSample,
    OpenInterestSample,
)
from bot_btc_1hr_kalshi.obs.schemas import Features, Side, TrapName


@dataclass(frozen=True, slots=True)
class MarketSnapshot:
    """Everything a trap needs to decide at a single point in time.

    `open_interest` is populated from the Coinglass poller (Slice 11 P2)
    when available; traps treat it as *observational* until shadow-mode
    data justifies a microstructure-gated entry. `None` is the
    pre-wiring default and also the value during warmup / fetch failure.

    `liquidation_heatmap` follows the same contract (Slice 11 P3).
    """

    market_id: str
    book: L2Book
    features: Features
    spot_btc_usd: float
    minutes_to_settlement: float
    strike_usd: float
    open_interest: OpenInterestSample | None = None
    liquidation_heatmap: LiquidationHeatmapSample | None = None


@dataclass(frozen=True, slots=True)
class TrapSignal:
    """A trap's proposal to open a position. Risk still has final say."""

    trap: TrapName
    side: Side
    entry_price_cents: int
    confidence: float
    edge_cents: float
    features: Features
