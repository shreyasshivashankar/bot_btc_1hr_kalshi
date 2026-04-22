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
    """Aggregated BTC-futures open-interest snapshot (Slice 11 P2 — shadow).

    Sourced from Coinglass; *observed only* — not gating live signals yet.
    Emitted as structured log records so a future promotion decision has
    paper-soak data to lean on. Fields map directly onto the Coinglass v4
    aggregated-open-interest payload so provenance is obvious in log
    readers without round-tripping the raw response.
    """

    ts_ns: int
    symbol: str
    total_oi_usd: float
    exchanges_count: int | None = None
    source: str = "coinglass"


@dataclass(frozen=True, slots=True)
class LiquidationHeatmapSample:
    """Aggregated BTC liquidation-heatmap snapshot (Slice 11 P3 — shadow).

    Sourced from the Coinglass liquidation-heatmap endpoint. Full heatmap
    payloads are large 2D grids of (price, time, liquidation_usd); we
    compress each poll to three summary stats that are cheap to log and
    sufficient for the observational-only question we want to answer in
    paper-soak: "where are the nearest liquidation clusters relative to
    spot, and how dense are they?"

    * `total_liquidation_usd` — sum over the grid (activity proxy).
    * `peak_cluster_price_usd` — price coordinate of the densest bucket.
    * `peak_cluster_liquidation_usd` — the density of that bucket.

    Any future decision to gate trap entries on proximity to a cluster
    (a microstructure change) would require risk-committee sign-off per
    docs/RISK.md — same contract as `OpenInterestSample`.
    """

    ts_ns: int
    symbol: str
    total_liquidation_usd: float
    peak_cluster_price_usd: float
    peak_cluster_liquidation_usd: float
    source: str = "coinglass"


@dataclass(frozen=True, slots=True)
class WhaleAlertSample:
    """Rolling whale-alert summary (Slice 11 P4 — shadow only).

    Sourced from the Whale Alert v1 `transactions` endpoint. Same
    observational-only contract as `OpenInterestSample` /
    `LiquidationHeatmapSample`: the poller emits samples onto the App
    state and structured logs, but no trap gates on them until shadow-
    soak justifies a specific threshold — docs/RISK.md sign-off path.

    Compressed to four summary stats per poll window (the raw endpoint
    returns a per-transaction list that would dominate the log budget if
    emitted unshaped):

    * `net_exchange_flow_usd` — sum over the window of (to-exchange USD)
      minus (from-exchange USD). Positive = net whales *depositing* to
      exchanges = a supply-to-sellers proxy (bearish prior). Negative =
      net withdrawals = removal from trading venues (bullish prior).
    * `largest_txn_usd` — biggest single whale-tagged transaction in
      the window. Discrete shocks matter; a single $500M inflow prints
      differently than ten $50M mid-tier moves.
    * `txn_count` — number of whale-tagged transactions in the window.
      A zero sample after a known large move means we're losing events
      to rate limits or filter drift.
    * `window_sec` — length of the polling window the summary was
      accumulated over, so the log reader can normalize across a
      future poll-cadence change without re-parsing history.

    `source` is always `"whale_alert"` today; kept as a field so a
    future multi-source aggregator can stamp provenance without a
    schema migration.
    """

    ts_ns: int
    symbol: str
    net_exchange_flow_usd: float
    largest_txn_usd: float
    txn_count: int
    window_sec: float
    source: str = "whale_alert"


FeedEvent = BookUpdate | TradeEvent | SpotTick
