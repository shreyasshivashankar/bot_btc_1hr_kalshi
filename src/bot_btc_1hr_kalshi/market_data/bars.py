"""Multi-timeframe OHLCV bar aggregator (Slice 7).

Folds a single `SpotTick` stream into boundary-aligned OHLCV bars across
multiple timeframes simultaneously (1m / 5m / 15m / 1h / 1d). This is the
bedrock primitive for the missing feature surface in DESIGN.md §5 —
per-timeframe RSI, the `ATR(15m) / ATR(4h)` vol ratio, `SMA_200(1h)`, the
top-down 1H alignment veto, and session VWAP all need closed-bar streams.

Design choices (see the Slice 7 discussion):

* **Integer-micros prices** — matches the money-discipline from commit
  `c5a7023` so running accumulators cannot drift. Volume stays `float`
  because `SpotTick.size` comes in as float from the spot feed; we'll
  revisit if we ever want signed integer micros there.

* **UTC-aligned boundaries** — a 60s bar opens at `HH:MM:00` UTC, a 3600s
  bar at the top of the hour, a 86400s bar at midnight UTC. `bar_open =
  (tick.ts_ns // tf_ns) * tf_ns`. This matches "candles on TradingView"
  and makes multi-timeframe alignment naturally nested: every 5m boundary
  is also a 1m boundary, every 1h boundary is also a 5m boundary, etc.

* **Event-driven, not batch** — no `df.resample` on each tick; each ingest
  is O(1). The new-bar branch fires a `BarClose` callback synchronously,
  matching the `SpotOracle.subscribe_primary` pattern so downstream
  features register exactly once at startup and get tick-accurate deliveries.

* **Tick-driven close** — a bar closes when the first tick of the *next*
  bar arrives, not on a timer. If no tick arrives for 90s the 1m bar just
  sits open; for BTC this never happens in practice and for backtests it
  is deterministic. `flush()` force-closes the open bar on teardown.

Slice 9 extends the bar with **side-split USD notional** (`buy_volume_usd` /
`sell_volume_usd`). The aggressor flag lives on `SpotTick`; the aggregator
computes `notional_usd = price_usd * size` per tick and routes it to the
buy or sell lane based on `tick.aggressor`. Ticks with `aggressor is None`
(initial ticker frames, v1 archive lines) still update OHLC and the
unsigned `volume` counter, but contribute to neither signed lane — this
is the conservative choice for CVD (counts only verified taker prints).

Not in this slice (intentionally): per-timeframe FeatureEngine wiring was
Slice 8, and VWAP state is Slice 10.
"""

from __future__ import annotations

import contextlib
from collections.abc import Callable
from dataclasses import dataclass

import structlog

from bot_btc_1hr_kalshi.market_data.types import SpotTick
from bot_btc_1hr_kalshi.obs.money import MICROS_PER_USD, Micros

_log = structlog.get_logger("bot_btc_1hr_kalshi.bars")


@dataclass(frozen=True, slots=True)
class Bar:
    """A closed OHLCV bar for one timeframe.

    `ts_open_ns` is the inclusive open timestamp (always divisible by
    `tf_sec * 1e9`). `ts_close_ns` is the exclusive upper bound — equal
    to the open of the next bar — so consumers can compute bar age with
    `(now_ns - ts_close_ns)` without off-by-one arithmetic.

    `buy_volume_usd` / `sell_volume_usd` are the **USD-notional** taker
    flow split (Slice 9 / CVD). USD-denomination is deliberate: a
    BTC-denominated threshold would fire 2.5x more readily at $40k than at
    $100k for the same "real" aggression, so the veto would decay in high
    regimes. `volume` (unsigned BTC size) is retained for any caller that
    wants coin-denominated flow, but the CVD gate in the traps consumes
    only the USD lanes.
    """

    tf_sec: int
    ts_open_ns: int
    ts_close_ns: int
    open_micros: Micros
    high_micros: Micros
    low_micros: Micros
    close_micros: Micros
    volume: float
    buy_volume_usd: float
    sell_volume_usd: float
    trade_count: int

    @property
    def open_usd(self) -> float:
        return self.open_micros / MICROS_PER_USD

    @property
    def high_usd(self) -> float:
        return self.high_micros / MICROS_PER_USD

    @property
    def low_usd(self) -> float:
        return self.low_micros / MICROS_PER_USD

    @property
    def close_usd(self) -> float:
        return self.close_micros / MICROS_PER_USD

    @property
    def cvd_usd(self) -> float:
        """Net aggressor-driven USD flow inside this bar: taker buys minus
        taker sells. Positive = net buying pressure; negative = net selling.
        Ticks without an aggressor tag are excluded from both lanes, so the
        CVD is "verified taker flow only."""
        return self.buy_volume_usd - self.sell_volume_usd


BarCloseCallback = Callable[[Bar], None]


class TimeframeAggregator:
    """Folds a tick stream into boundary-aligned OHLCV bars for one timeframe.

    Construct once per timeframe (1m / 5m / 1h / …), feed ticks via
    `ingest(tick)`, receive closed `Bar` objects through subscribed
    callbacks. Ticks whose timestamp falls before the current bar's open
    are dropped with a warning — out-of-order prints corrupt OHLC if we
    let them in.
    """

    __slots__ = (
        "_close_cbs",
        "_cur_buy_usd",
        "_cur_close_micros",
        "_cur_close_ns",
        "_cur_high_micros",
        "_cur_low_micros",
        "_cur_open_micros",
        "_cur_open_ns",
        "_cur_sell_usd",
        "_cur_trade_count",
        "_cur_volume",
        "_tf_ns",
        "_tf_sec",
    )

    def __init__(self, *, tf_sec: int) -> None:
        if tf_sec <= 0:
            raise ValueError("tf_sec must be > 0")
        self._tf_sec = tf_sec
        self._tf_ns = tf_sec * 1_000_000_000
        self._close_cbs: list[BarCloseCallback] = []
        self._cur_open_ns: int | None = None
        self._cur_close_ns: int | None = None
        self._cur_open_micros: Micros = Micros(0)
        self._cur_high_micros: Micros = Micros(0)
        self._cur_low_micros: Micros = Micros(0)
        self._cur_close_micros: Micros = Micros(0)
        self._cur_volume: float = 0.0
        self._cur_buy_usd: float = 0.0
        self._cur_sell_usd: float = 0.0
        self._cur_trade_count: int = 0

    @property
    def tf_sec(self) -> int:
        return self._tf_sec

    def subscribe(self, cb: BarCloseCallback) -> Callable[[], None]:
        """Register a close-bar callback. Returns an unsubscribe closure."""
        self._close_cbs.append(cb)

        def _unsub() -> None:
            with contextlib.suppress(ValueError):
                self._close_cbs.remove(cb)

        return _unsub

    def ingest(self, tick: SpotTick) -> None:
        bar_open = (tick.ts_ns // self._tf_ns) * self._tf_ns
        if self._cur_open_ns is None:
            self._start_bar(bar_open, tick)
            return
        if bar_open < self._cur_open_ns:
            _log.warning(
                "bars.backward_tick_dropped",
                tf_sec=self._tf_sec,
                tick_ts_ns=tick.ts_ns,
                current_bar_open_ns=self._cur_open_ns,
            )
            return
        if bar_open != self._cur_open_ns:
            self._emit_close()
            self._start_bar(bar_open, tick)
            return
        # Same bar — update HLCV.
        if tick.price_micros > self._cur_high_micros:
            self._cur_high_micros = tick.price_micros
        if tick.price_micros < self._cur_low_micros:
            self._cur_low_micros = tick.price_micros
        self._cur_close_micros = tick.price_micros
        self._cur_volume += tick.size
        notional_usd = (tick.price_micros / MICROS_PER_USD) * tick.size
        if tick.aggressor == "buy":
            self._cur_buy_usd += notional_usd
        elif tick.aggressor == "sell":
            self._cur_sell_usd += notional_usd
        self._cur_trade_count += 1

    def flush(self) -> None:
        """Force-close the open bar (if any). Use at teardown / end of replay."""
        if self._cur_open_ns is not None:
            self._emit_close()
            self._cur_open_ns = None
            self._cur_close_ns = None

    def _start_bar(self, bar_open: int, tick: SpotTick) -> None:
        self._cur_open_ns = bar_open
        self._cur_close_ns = bar_open + self._tf_ns
        self._cur_open_micros = tick.price_micros
        self._cur_high_micros = tick.price_micros
        self._cur_low_micros = tick.price_micros
        self._cur_close_micros = tick.price_micros
        self._cur_volume = tick.size
        notional_usd = (tick.price_micros / MICROS_PER_USD) * tick.size
        self._cur_buy_usd = notional_usd if tick.aggressor == "buy" else 0.0
        self._cur_sell_usd = notional_usd if tick.aggressor == "sell" else 0.0
        self._cur_trade_count = 1

    def _emit_close(self) -> None:
        if self._cur_open_ns is None or self._cur_close_ns is None:
            return
        bar = Bar(
            tf_sec=self._tf_sec,
            ts_open_ns=self._cur_open_ns,
            ts_close_ns=self._cur_close_ns,
            open_micros=self._cur_open_micros,
            high_micros=self._cur_high_micros,
            low_micros=self._cur_low_micros,
            close_micros=self._cur_close_micros,
            volume=self._cur_volume,
            buy_volume_usd=self._cur_buy_usd,
            sell_volume_usd=self._cur_sell_usd,
            trade_count=self._cur_trade_count,
        )
        for cb in list(self._close_cbs):
            try:
                cb(bar)
            except Exception as exc:  # pragma: no cover — consumer bug
                _log.warning(
                    "bars.callback_error",
                    tf_sec=self._tf_sec,
                    error=str(exc),
                )


class MultiTimeframeBus:
    """Fans one tick stream across a fixed set of timeframe aggregators.

    Lifetime mirrors `SpotOracle` — construct once at App scope, hand
    `bus.ingest` to `spot_oracle.subscribe_primary(bus.ingest)` at startup,
    and let downstream consumers register per-timeframe callbacks via
    `bus.subscribe(tf_sec=300, cb=...)`. The bus owns no async state and
    requires no `run()` task; it is driven purely by the oracle's callback
    pump.
    """

    __slots__ = ("_aggs",)

    def __init__(self, *, tf_secs: list[int]) -> None:
        if not tf_secs:
            raise ValueError("tf_secs must not be empty")
        if len(set(tf_secs)) != len(tf_secs):
            raise ValueError("tf_secs must be unique")
        self._aggs: dict[int, TimeframeAggregator] = {
            tf: TimeframeAggregator(tf_sec=tf) for tf in tf_secs
        }

    @property
    def timeframes(self) -> tuple[int, ...]:
        return tuple(self._aggs.keys())

    def subscribe(self, *, tf_sec: int, cb: BarCloseCallback) -> Callable[[], None]:
        agg = self._aggs.get(tf_sec)
        if agg is None:
            raise ValueError(
                f"tf_sec={tf_sec} not registered at construction; "
                f"available={sorted(self._aggs.keys())}"
            )
        return agg.subscribe(cb)

    def ingest(self, tick: SpotTick) -> None:
        for agg in self._aggs.values():
            agg.ingest(tick)

    def flush(self) -> None:
        for agg in self._aggs.values():
            agg.flush()
