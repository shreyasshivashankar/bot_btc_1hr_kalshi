"""Persistent derivatives oracle: single source of truth for BTC OI / liq.

Sibling to `SpotOracle`. Lives at App scope, owns one or more
`DerivativesFeed` tasks, and exposes a fail-closed `get_open_interest`
accessor that raises `DerivativesStaleError` when the last sample is
older than the caller's `max_age_ms`. The non-raising
`get_open_interest_or_none` variant matches `SpotOracle.get_primary_or_none`
for snapshot/feature paths that handle "no data yet" upstream.

Why an oracle and not just an exposed feed:

  * Promotion path. The Coinglass HTTP poller this is replacing was
    polled on a 30s cadence with no staleness contract — `App.latest
    _open_interest` could silently age across a wedged poll without
    the snapshot consumer noticing. The oracle inherits the
    `SpotOracle` fail-closed posture so a future trap that gates on
    OI movement gets the same staleness guarantees as one that gates
    on spot price.
  * Multi-source aggregation. Hyperliquid (PR-A) is the bootstrap
    source. PR-B adds Bybit; PR-C may feed in liquidation events
    from the same venues. Centralizing them here lets consumers
    subscribe to the *latest of any source* without each one having
    to know which feeds are wired today.

Source preference today: any sample wins (most recent timestamp). Once
multiple sources are live we may want a primary/confirmation split
analogous to spot (Coinbase primary / Kraken confirmation) — that is
a behavioral change requiring its own slice + sign-off.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable

import structlog

from bot_btc_1hr_kalshi.market_data.feeds.derivatives import DerivativesFeed
from bot_btc_1hr_kalshi.market_data.types import OpenInterestSample
from bot_btc_1hr_kalshi.obs.clock import Clock

_log = structlog.get_logger("bot_btc_1hr_kalshi.derivatives_oracle")

OpenInterestCallback = Callable[[OpenInterestSample], None]


class DerivativesStaleError(RuntimeError):
    """Last-known derivatives sample is older than `max_age_ms` (or absent)."""


class DerivativesOracle:
    """Persistent, fail-closed accessor for BTC derivatives metrics.

    Constructed with a tuple of `DerivativesFeed` instances; the App
    spawns `run()` as a background task. Each feed's events stream
    into a shared `_latest_oi` slot, with subscribers notified on every
    new sample.

    Empty `feeds` is allowed — the oracle then exposes a no-op `run()`
    and `get_open_interest_or_none` always returns None. This keeps
    boot wiring simple in dev/test where no derivatives feed is
    configured.
    """

    __slots__ = (
        "_clock",
        "_feeds",
        "_latest_oi",
        "_oi_cbs",
    )

    def __init__(
        self,
        *,
        feeds: tuple[DerivativesFeed, ...] = (),
        clock: Clock,
    ) -> None:
        self._feeds = feeds
        self._clock = clock
        self._latest_oi: OpenInterestSample | None = None
        self._oi_cbs: list[OpenInterestCallback] = []

    async def run(self) -> None:
        """Consume all configured feeds for the lifetime of the App.

        If any feed coroutine raises unexpectedly, the gather propagates
        and the App crashes so Cloud Run restarts the container — same
        contract as `SpotOracle.run`. With zero feeds, returns
        immediately so the App startup path doesn't hang waiting on a
        no-op gather.
        """
        if not self._feeds:
            return
        await asyncio.gather(*(self._consume(f) for f in self._feeds))

    async def _consume(self, feed: DerivativesFeed) -> None:
        async for sample in feed.events():
            self._latest_oi = sample
            cbs = list(self._oi_cbs)
            for cb in cbs:
                try:
                    cb(sample)
                except Exception as exc:  # pragma: no cover — consumer bug
                    _log.warning(
                        "derivatives_oracle.callback_error",
                        source=sample.source,
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
