"""Signal-layer types: the inputs a trap sees and the output it produces."""

from __future__ import annotations

from dataclasses import dataclass

from bot_btc_1hr_kalshi.market_data import L2Book
from bot_btc_1hr_kalshi.market_data.types import OpenInterestSample
from bot_btc_1hr_kalshi.obs.schemas import Features, Side, TrapName


@dataclass(frozen=True, slots=True)
class LiquidationPressure:
    """Recent liquidation pressure within a price band around spot (PR-C).

    Pre-aggregated by the snapshot builder so each trap only does a
    single threshold compare. Built from the FeatureEngine rolling
    deque, which is fed live by the Bybit liquidation stream via
    DerivativesOracle.subscribe_liquidations.

    `long_usd_below_spot`: USD notional of long-side liquidations whose
    print price sat strictly below current spot inside the lookback
    window. A floor (long-side) trap reads this — large recent long
    wipes below us means a downside cascade is in progress, not a dip
    to fade.

    `short_usd_above_spot`: symmetric, mirrored above spot. A ceiling
    (short-side) trap reads this — large recent short wipes above us
    means an upside cascade is in progress, not a rip to fade.
    """

    long_usd_below_spot: float
    short_usd_above_spot: float


@dataclass(frozen=True, slots=True)
class MarketSnapshot:
    """Everything a trap needs to decide at a single point in time.

    `open_interest` is populated from `App.latest_open_interest`, which
    DerivativesOracle writes from the Hyperliquid `metaAndAssetCtxs`
    and Bybit `tickers` push streams. Traps treat it as observational
    until shadow-soak data justifies a microstructure-gated entry. `None`
    is the cold-start / disabled-feed value.

    `liquidation_pressure` aggregates the FeatureEngine rolling
    liquidation deque against current spot. Driven by the live Bybit
    liquidation-prints stream via DerivativesOracle; shadow contract
    (tag only, no gate) until risk-committee sign-off.
    """

    market_id: str
    book: L2Book
    features: Features
    spot_btc_usd: float
    minutes_to_settlement: float
    strike_usd: float
    open_interest: OpenInterestSample | None = None
    liquidation_pressure: LiquidationPressure | None = None


@dataclass(frozen=True, slots=True)
class TrapSignal:
    """A trap's proposal to open a position. Risk still has final say."""

    trap: TrapName
    side: Side
    entry_price_cents: int
    confidence: float
    edge_cents: float
    features: Features
