"""Persistent derivatives oracle: single source of truth for BTC OI / liq.

Sibling to `SpotOracle`. Lives at App scope, owns one or more
`DerivativesFeed` tasks, and exposes fail-closed accessors that raise
when the last sample is older than the caller's `max_age_ms`. The
non-raising `_or_none` variants match `SpotOracle.get_primary_or_none`
for snapshot / feature paths that handle "no data yet" upstream.

Why an oracle and not just exposed feeds:

  * Promotion path. The Coinglass HTTP poller this is replacing was
    polled on a 30s cadence with no staleness contract — `App.latest_
    open_interest` could silently age across a wedged poll without the
    snapshot consumer noticing. The oracle inherits the `SpotOracle`
    fail-closed posture so a future trap that gates on OI movement or
    liquidation-cluster density gets the same staleness guarantees as
    one that gates on spot price.
  * Multi-source aggregation. Hyperliquid and Bybit both feed OI
    snapshots; Bybit also feeds discrete liquidation prints. A PR-C
    rolling deque in the `FeatureEngine` subscribes to liquidations
    here once — the feeds side doesn't need to know which downstreams
    consume what.

Source preference today: any OI sample wins (most-recent timestamp).
Once multiple sources are live we may want a primary/confirmation
split analogous to spot (Coinbase primary / Kraken confirmation) —
that is a behavioral change requiring its own slice + sign-off.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable, Coroutine
from typing import Any

import structlog

from bot_btc_1hr_kalshi.market_data.feeds.derivatives import DerivativesFeed
from bot_btc_1hr_kalshi.market_data.types import LiquidationEvent, OpenInterestSample
from bot_btc_1hr_kalshi.obs.clock import Clock

_log = structlog.get_logger("bot_btc_1hr_kalshi.derivatives_oracle")

OpenInterestCallback = Callable[[OpenInterestSample], None]
LiquidationCallback = Callable[[LiquidationEvent], None]


class DerivativesStaleError(RuntimeError):
    """Last-known derivatives sample is older than `max_age_ms` (or absent)."""


class DerivativesOracle:
    """Persistent, fail-closed accessor for BTC derivatives metrics.

    Constructed with two tuples of typed `DerivativesFeed` instances —
    OI and liquidation — and an App-injected clock. The App spawns
    `run()` as a background task. Each feed's events stream into a
    shared latest-slot, with subscribers notified on every new event.

    Empty `oi_feeds` + `liq_feeds` is allowed — the oracle then exposes
    a no-op `run()` and all accessors return None. This keeps boot
    wiring simple in dev/test where no derivatives feed is configured.
    """

    __slots__ = (
        "_clock",
        "_latest_liq",
        "_latest_oi",
        "_liq_cbs",
        "_liq_feeds",
        "_oi_cbs",
        "_oi_feeds",
    )

    def __init__(
        self,
        *,
        oi_feeds: tuple[DerivativesFeed[OpenInterestSample], ...] = (),
        liq_feeds: tuple[DerivativesFeed[LiquidationEvent], ...] = (),
        clock: Clock,
    ) -> None:
        self._oi_feeds = oi_feeds
        self._liq_feeds = liq_feeds
        self._clock = clock
        self._latest_oi: OpenInterestSample | None = None
        self._latest_liq: LiquidationEvent | None = None
        self._oi_cbs: list[OpenInterestCallback] = []
        self._liq_cbs: list[LiquidationCallback] = []

    async def run(self) -> None:
        """Consume all configured feeds for the lifetime of the App.

        If any feed coroutine raises unexpectedly, the gather propagates
        and the App crashes so Cloud Run restarts the container — same
        contract as `SpotOracle.run`. With zero feeds, returns
        immediately so the App startup path doesn't hang waiting on a
        no-op gather.
        """
        tasks: list[Coroutine[Any, Any, None]] = []
        tasks.extend(self._consume_oi(f) for f in self._oi_feeds)
        tasks.extend(self._consume_liq(f) for f in self._liq_feeds)
        if not tasks:
            return
        await asyncio.gather(*tasks)

    async def _consume_oi(self, feed: DerivativesFeed[OpenInterestSample]) -> None:
        async for sample in feed.events():
            self._latest_oi = sample
            for cb in list(self._oi_cbs):
                try:
                    cb(sample)
                except Exception as exc:  # pragma: no cover — consumer bug
                    _log.warning(
                        "derivatives_oracle.oi_callback_error",
                        source=sample.source,
                        error=str(exc),
                    )

    async def _consume_liq(self, feed: DerivativesFeed[LiquidationEvent]) -> None:
        async for event in feed.events():
            self._latest_liq = event
            for cb in list(self._liq_cbs):
                try:
                    cb(event)
                except Exception as exc:  # pragma: no cover — consumer bug
                    _log.warning(
                        "derivatives_oracle.liq_callback_error",
                        source=event.source,
                        error=str(exc),
                    )

    def subscribe_open_interest(self, cb: OpenInterestCallback) -> Callable[[], None]:
        """Register a callback for every fresh OI sample (any source).

        Returns an unsubscribe function. The latest known sample (if any)
        is delivered immediately on subscribe so a consumer that
        registers mid-session doesn't have to wait for the next push to
        warm up.
        """
        self._oi_cbs.append(cb)
        if self._latest_oi is not None:
            cb(self._latest_oi)

        def _unsub() -> None:
            with contextlib.suppress(ValueError):
                self._oi_cbs.remove(cb)

        return _unsub

    def subscribe_liquidations(self, cb: LiquidationCallback) -> Callable[[], None]:
        """Register a callback for every liquidation event (any source).

        Liquidation events are discrete, so — unlike OI — there is no
        meaningful "latest snapshot" to warm-start a new subscriber.
        The FeatureEngine rolling deque (PR-C) seeds from history via
        the Binance public-data backfill; the live stream only catches
        up from subscribe-time onward, which is the contract we want
        (no duplicate replay of already-seen events).
        """
        self._liq_cbs.append(cb)

        def _unsub() -> None:
            with contextlib.suppress(ValueError):
                self._liq_cbs.remove(cb)

        return _unsub

    def get_open_interest(self, *, max_age_ms: int) -> OpenInterestSample:
        """Return the freshest OI sample or raise `DerivativesStaleError`.

        Use from gating decisions only — the fail-closed contract is
        what guarantees a wedged feed cannot silently degrade signal
        quality. Snapshot / telemetry paths should use
        `get_open_interest_or_none`.
        """
        sample = self._latest_oi
        now_ns = self._clock.now_ns()
        if sample is None:
            raise DerivativesStaleError("no derivatives sample received yet")
        age_ms = (now_ns - sample.ts_ns) / 1_000_000
        if age_ms > max_age_ms:
            raise DerivativesStaleError(
                f"derivatives sample stale: age_ms={age_ms:.0f} > threshold_ms={max_age_ms}"
            )
        return sample

    def get_open_interest_or_none(
        self, *, max_age_ms: int
    ) -> OpenInterestSample | None:
        try:
            return self.get_open_interest(max_age_ms=max_age_ms)
        except DerivativesStaleError:
            return None

    @property
    def latest_open_interest(self) -> OpenInterestSample | None:
        return self._latest_oi

    @property
    def latest_liquidation(self) -> LiquidationEvent | None:
        return self._latest_liq
