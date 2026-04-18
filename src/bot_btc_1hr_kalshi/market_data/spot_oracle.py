"""Persistent spot oracle: single source of truth for BTC spot across the app.

Before Slice 6, SpotFeeds were torn down and rebuilt each hour-roll (inside
the per-session FeedLoop) and each consumer (FeatureEngine, IntegrityTracker,
market discovery) reached into the feed with its own local state. Two
things were fragile:

  1. Hour-boundary discovery ran with no BTC spot reference, so strike
     tiebreak fell back to alphabetical and we picked deep-ITM markets
     (e.g. $66k strike while spot was $78k) with no tradeable edge.
  2. Silent degradation — if the Coinbase feed stalled, downstream callers
     saw a cached last-known price with no stale signal, and emitted
     features anyway.

The SpotOracle fixes both: it runs at App level (one lifetime per
container, not per hour), and the only `get_primary` surface is
**fail-closed** — it raises `SpotStaleError` when the last-known price is older
than the caller's `max_age_ms`. Callers that cannot fail (feature compute,
telemetry) use the `_or_none` variant and skip this tick.

Hard rule #9 ("never invalidate book-derived features silently on a seq
gap") applies by analogy: stale spot is invalid spot — return fresh data
or raise. No middle ground.

Coinbase = primary (drives features + discovery); Kraken = confirmation
(feeds the integrity gate only). The oracle mirrors the routing in
`market_data/feeds/spot.py` and `signal/integrity.py`.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable

import structlog

from bot_btc_1hr_kalshi.market_data.feeds.spot import SpotFeed
from bot_btc_1hr_kalshi.market_data.types import SpotTick
from bot_btc_1hr_kalshi.obs.clock import Clock

_log = structlog.get_logger("bot_btc_1hr_kalshi.spot_oracle")

SpotCallback = Callable[[SpotTick], None]


class SpotStaleError(RuntimeError):
    """Last-known spot tick is older than the caller's max_age_ms (or absent)."""


class SpotOracle:
    """Persistent, fail-closed accessor for BTC spot prices.

    Owns long-lived Coinbase (primary) and Kraken (confirmation) feed tasks
    spawned by `run()`. Consumers either subscribe for tick streams or poll
    `get_primary` for the latest-known value with a staleness ceiling.
    """

    __slots__ = (
        "_clock",
        "_confirmation",
        "_confirmation_cbs",
        "_latest_confirmation",
        "_latest_primary",
        "_primary",
        "_primary_cbs",
    )

    def __init__(
        self,
        *,
        primary: SpotFeed,
        confirmation: SpotFeed,
        clock: Clock,
    ) -> None:
        self._primary = primary
        self._confirmation = confirmation
        self._clock = clock
        self._latest_primary: SpotTick | None = None
        self._latest_confirmation: SpotTick | None = None
        self._primary_cbs: list[SpotCallback] = []
        self._confirmation_cbs: list[SpotCallback] = []

    async def run(self) -> None:
        """Consume primary + confirmation feeds for the lifetime of the App.

        The two coroutines are tied together via `gather`: if either one
        raises unexpectedly we want the App to crash so Cloud Run restarts
        the container rather than silently running half-deaf. SpotFeed
        catches its own reconnect errors internally, so reaching this
        level means something genuinely unrecoverable.
        """
        await asyncio.gather(
            self._consume(self._primary, is_primary=True),
            self._consume(self._confirmation, is_primary=False),
        )

    async def _consume(self, feed: SpotFeed, *, is_primary: bool) -> None:
        async for tick in feed.events():
            if is_primary:
                self._latest_primary = tick
                cbs = list(self._primary_cbs)
            else:
                self._latest_confirmation = tick
                cbs = list(self._confirmation_cbs)
            for cb in cbs:
                try:
                    cb(tick)
                except Exception as exc:  # pragma: no cover — consumer bug
                    _log.warning(
                        "spot_oracle.callback_error",
                        venue=tick.venue,
                        error=str(exc),
                    )

    def subscribe_primary(self, cb: SpotCallback) -> Callable[[], None]:
        """Register a callback for every primary (Coinbase) tick.

        Returns an unsubscribe function — callers MUST invoke it when they
        tear down (end of hour session) to avoid a leak that keeps feeding
        a dead FeatureEngine.
        """
        self._primary_cbs.append(cb)
        if self._latest_primary is not None:
            cb(self._latest_primary)

        def _unsub() -> None:
            with contextlib.suppress(ValueError):
                self._primary_cbs.remove(cb)

        return _unsub

    def subscribe_confirmation(self, cb: SpotCallback) -> Callable[[], None]:
        self._confirmation_cbs.append(cb)
        if self._latest_confirmation is not None:
            cb(self._latest_confirmation)

        def _unsub() -> None:
            with contextlib.suppress(ValueError):
                self._confirmation_cbs.remove(cb)

        return _unsub

    def get_primary(self, *, max_age_ms: int) -> float:
        """Return the last-known primary price in USD, or raise `SpotStaleError`.

        Raises immediately if no tick has ever been received (cold start)
        or if the last tick is older than `max_age_ms`. Callers that gate
        trading decisions (market discovery, entry) MUST use this variant;
        the fail-closed guarantee is what keeps us from picking deep-ITM
        strikes or sizing against stale mid-price estimates.
        """
        tick = self._latest_primary
        now_ns = self._clock.now_ns()
        if tick is None:
            raise SpotStaleError("no primary spot tick received yet")
        age_ms = (now_ns - tick.ts_ns) / 1_000_000
        if age_ms > max_age_ms:
            raise SpotStaleError(
                f"primary spot stale: age_ms={age_ms:.0f} > threshold_ms={max_age_ms}"
            )
        return tick.price_usd

    def get_primary_or_none(self, *, max_age_ms: int) -> float | None:
        """Non-raising variant for feature / snapshot paths.

        Returns None iff `get_primary` would raise. Callers that return
        None handle "no features yet" upstream (e.g. `FeedLoop._snapshot`)
        so a stale spot simply means we sit this tick out, which is
        equivalent to refusing to trade — the staleness contract is
        preserved.
        """
        try:
            return self.get_primary(max_age_ms=max_age_ms)
        except SpotStaleError:
            return None

    @property
    def latest_primary_tick(self) -> SpotTick | None:
        return self._latest_primary

    @property
    def latest_confirmation_tick(self) -> SpotTick | None:
        return self._latest_confirmation
