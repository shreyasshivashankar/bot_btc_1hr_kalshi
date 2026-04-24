"""Immutable value types for market data.

Kalshi L2 updates and spot-venue trades flow through these types. Frozen/slotted
dataclasses keep allocation cheap on the hot path.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from bot_btc_1hr_kalshi.obs.money import MICROS_PER_USD, Micros
from bot_btc_1hr_kalshi.obs.schemas import Side

Venue = Literal["kalshi", "coinbase", "kraken", "hyperliquid", "bybit"]
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
    telemetry — never use it inside arithmetic accumulations.

    `aggressor` is the taker side of the underlying match, normalized to
    aggressor semantics at the parser layer — Coinbase's `ticker` feed is
    maker-centric and gets inverted there; Kraken V2's `trade` feed already
    reports the taker side directly. `None` means the venue frame did not
    carry side information (initial ticker, quote-only update, or a v1
    archive written before Slice 9). Bar aggregators that care about signed
    volume skip `None`-tagged ticks from the buy/sell accumulators."""

    ts_ns: int
    venue: Venue
    price_micros: Micros
    size: float
    aggressor: AggressorSide | None = None

    @property
    def price_usd(self) -> float:
        return self.price_micros / MICROS_PER_USD


@dataclass(frozen=True, slots=True)
class OpenInterestSample:
    """Aggregated BTC-futures open-interest snapshot.

    Sourced from `DerivativesOracle` (Hyperliquid `metaAndAssetCtxs` and
    Bybit `tickers`, most-recent wins). Observational only — not gating
    live signals yet. Emitted as structured log records so a future
    promotion decision has paper-soak data to lean on.
    """

    ts_ns: int
    symbol: str
    total_oi_usd: float
    exchanges_count: int | None = None
    source: str = "derivatives_oracle"


@dataclass(frozen=True, slots=True)
class LiquidationEvent:
    """A single liquidation print from a derivatives venue.

    Discrete event emitted every time the venue's liquidation engine
    closes a position. The `FeatureEngine` accumulates these into a
    rolling deque so floor/ceiling traps can read a live "recent
    liquidation pressure" signal via `LiquidationPressure` aggregated
    at snapshot construction time.

    `side` is the direction of the *order that got liquidated* — a
    `long` liquidation means a long position was force-closed (aggressor
    side of the closing fill was a sell). Venues report this as the
    liquidated position's own direction, which is what traps want to
    reason about ("how many longs just got wiped?").

    `size_usd` is the USD notional, pre-computed at the parser layer
    from quantity * price. Keeping it denormalized lets the rolling
    deque sum without re-multiplying on every access.
    """

    ts_ns: int
    symbol: str
    side: Literal["long", "short"]
    price_usd: float
    size_usd: float
    source: str = "bybit"


FeedEvent = BookUpdate | TradeEvent | SpotTick
