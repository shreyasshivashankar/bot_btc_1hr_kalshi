"""Whale Alert poller (Slice 11 P4 — shadow-mode only).

Polls the Whale Alert v1 `transactions` endpoint on a fixed cadence and
emits a compressed `WhaleAlertSample` via a caller-provided async
callback. The caller (typically `__main__`) stashes the latest sample on
`App.latest_whale_alert` and emits a structured log record so paper-
soak telemetry can inform a future microstructure-gated promotion
decision.

Signal hypothesis (observational today, trap-gated later — same
contract as Coinglass OI/heatmap):

    net inflow to exchanges  -> supply-to-sellers proxy (bearish prior)
    net outflow from exchanges -> removal from trading venues (bullish)

The summary we emit is the `(to_exchange - from_exchange)` USD balance
over the polling window. Neither direction is acted on until shadow-
soak data + risk-committee review. Hard rule #2.

Why compress? The v1 endpoint returns a list of transactions; at whale-
alert cadence and a permissive `min_value` filter the per-poll payload
can exceed a few hundred transactions. Emitting each one into the
decision journal would blow past the log budget and make no trap
computation faster. We distill the window to four stats and keep the
raw payload off-box.

Design mirrors `coinglass_heatmap.CoinglassHeatmapPoller`: shared httpx
client injected, fail-soft polling loop, pure-function parser unit-
testable via MockTransport + canned fixture.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

import httpx
import orjson
import structlog

from bot_btc_1hr_kalshi.market_data.types import WhaleAlertSample
from bot_btc_1hr_kalshi.obs.clock import Clock

log = structlog.get_logger(__name__)

WHALE_ALERT_DEFAULT_BASE_URL = "https://api.whale-alert.io"
WHALE_ALERT_DEFAULT_PATH = "/v1/transactions"
# Whale Alert accepts the key as a query-string param (not a header),
# which is unusual — documented here so the curious reviewer sees the
# choice is intentional and lines up with their v1 spec.
WHALE_ALERT_API_KEY_PARAM = "api_key"


class WhaleAlertParseError(ValueError):
    """Response shape did not match the documented v1 contract."""


def parse_whale_alert_response(
    raw: bytes | str,
    *,
    symbol: str,
    window_sec: float,
    clock: Clock,
) -> WhaleAlertSample:
    """Parse a Whale Alert `/v1/transactions` response into a summary.

    Documented envelope (v1):

        {
          "result": "success",
          "cursor": "...",
          "count": 42,
          "transactions": [
            {
              "blockchain": "bitcoin",
              "symbol": "BTC",
              "amount_usd": 12345678.0,
              "timestamp": 1712345678,
              "from": {"address": "...", "owner": "...",
                       "owner_type": "exchange"},
              "to":   {"address": "...", "owner": "...",
                       "owner_type": "unknown"}
            },
            ...
          ]
        }

    `owner_type` is the classification field Whale Alert uses — we
    treat `"exchange"` as exchange and anything else as non-exchange
    (unknown, personal wallet, other). Net flow = (to-exchange USD) -
    (from-exchange USD); positive means net whales *depositing* to
    exchanges over the window.
    """
    payload: Any = orjson.loads(raw)
    if not isinstance(payload, dict):
        raise WhaleAlertParseError(
            f"expected JSON object, got {type(payload).__name__}"
        )
    result = payload.get("result")
    if result != "success":
        raise WhaleAlertParseError(
            f"whale-alert result={result!r} error={payload.get('message')!r}"
        )
    transactions = payload.get("transactions")
    if not isinstance(transactions, list):
        raise WhaleAlertParseError(
            "response `transactions` is missing or not a list"
        )

    net_usd = 0.0
    largest_usd = 0.0
    count = 0
    for txn in transactions:
        if not isinstance(txn, dict):
            continue
        amount = _float_or_none(txn.get("amount_usd"))
        if amount is None or amount <= 0.0:
            continue
        from_type = _owner_type(txn.get("from"))
        to_type = _owner_type(txn.get("to"))
        # Internal-exchange transfers cancel on both sides — skip to
        # avoid double-counting the exchange-to-exchange hops Whale
        # Alert reports during cold-wallet rotations.
        if from_type == "exchange" and to_type == "exchange":
            continue
        if to_type == "exchange":
            net_usd += amount
        elif from_type == "exchange":
            net_usd -= amount
        else:
            # Neither side is a flagged exchange — does not move the
            # net-flow stat but still counts toward whale activity.
            pass
        if amount > largest_usd:
            largest_usd = amount
        count += 1

    return WhaleAlertSample(
        ts_ns=clock.now_ns(),
        symbol=symbol,
        net_exchange_flow_usd=net_usd,
        largest_txn_usd=largest_usd,
        txn_count=count,
        window_sec=window_sec,
    )


def _owner_type(entry: Any) -> str | None:
    if isinstance(entry, dict):
        ot = entry.get("owner_type")
        if isinstance(ot, str):
            return ot
    return None


def _float_or_none(v: Any) -> float | None:
    if isinstance(v, int | float):
        return float(v)
    return None


async def fetch_whale_alert(
    *,
    client: httpx.AsyncClient,
    api_key: str,
    start_ts_sec: int,
    base_url: str = WHALE_ALERT_DEFAULT_BASE_URL,
    path: str = WHALE_ALERT_DEFAULT_PATH,
    symbol: str = "btc",
    min_value_usd: int = 1_000_000,
    window_sec: float,
    clock: Clock,
) -> WhaleAlertSample:
    """Single fetch+parse. Raises on HTTP / decode errors so the caller
    decides whether to swallow (polling loop) or propagate (tests).

    `start_ts_sec` is a Unix-seconds lower bound — the poller advances
    this by `window_sec` on each successful poll to avoid re-counting
    transactions already observed. The Whale Alert v1 contract uses
    second precision.
    """
    params = {
        WHALE_ALERT_API_KEY_PARAM: api_key,
        "start": str(start_ts_sec),
        "currency": symbol,
        "min_value": str(min_value_usd),
    }
    resp = await client.get(f"{base_url}{path}", params=params)
    resp.raise_for_status()
    return parse_whale_alert_response(
        resp.content,
        symbol=symbol,
        window_sec=window_sec,
        clock=clock,
    )


class WhaleAlertPoller:
    """Background loop — fetch transactions on cadence, publish summary.

    A missing API key short-circuits the loop entirely (Whale Alert
    does not expose an unauthenticated tier). The boot wiring logs a
    warning and leaves the feed disabled in that case; the poller
    itself refuses to construct without a key to surface the problem at
    startup rather than in the silent-failure path.
    """

    __slots__ = (
        "_api_key",
        "_base_url",
        "_client",
        "_clock",
        "_last_poll_ts_sec",
        "_min_value_usd",
        "_on_sample",
        "_path",
        "_poll_interval_sec",
        "_symbol",
    )

    def __init__(
        self,
        *,
        client: httpx.AsyncClient,
        clock: Clock,
        on_sample: Callable[[WhaleAlertSample], Awaitable[None]],
        api_key: str,
        base_url: str = WHALE_ALERT_DEFAULT_BASE_URL,
        path: str = WHALE_ALERT_DEFAULT_PATH,
        symbol: str = "btc",
        min_value_usd: int = 1_000_000,
        poll_interval_sec: float = 60.0,
    ) -> None:
        if poll_interval_sec <= 0:
            raise ValueError("poll_interval_sec must be > 0")
        if not api_key:
            raise ValueError("whale alert requires an api key")
        self._client = client
        self._clock = clock
        self._on_sample = on_sample
        self._api_key = api_key
        self._base_url = base_url
        self._path = path
        self._symbol = symbol
        self._min_value_usd = min_value_usd
        self._poll_interval_sec = poll_interval_sec
        # `start` bound advances on each successful poll so we never
        # re-count a transaction. Cold start uses (now - window) so the
        # first poll captures the most recent window rather than
        # returning an empty result (Whale Alert rejects start=0).
        self._last_poll_ts_sec = (clock.now_ns() // 1_000_000_000) - int(
            poll_interval_sec
        )

    async def poll_once(self) -> WhaleAlertSample | None:
        """Single fetch+publish cycle. Returns the sample on success or
        None on failure. Used by tests; `run()` calls this in a loop."""
        start_ts_sec = self._last_poll_ts_sec
        try:
            sample = await fetch_whale_alert(
                client=self._client,
                api_key=self._api_key,
                start_ts_sec=start_ts_sec,
                base_url=self._base_url,
                path=self._path,
                symbol=self._symbol,
                min_value_usd=self._min_value_usd,
                window_sec=float(self._poll_interval_sec),
                clock=self._clock,
            )
        except (httpx.HTTPError, WhaleAlertParseError, ValueError) as exc:
            log.warning(
                "whale_alert.fetch_failed",
                url=f"{self._base_url}{self._path}",
                symbol=self._symbol,
                error=str(exc),
                exc_type=type(exc).__name__,
            )
            return None
        # Advance the `start` bound only on success; a transient
        # failure leaves the window overlap in place for retry.
        self._last_poll_ts_sec = self._clock.now_ns() // 1_000_000_000
        log.info(
            "whale_alert.sample",
            symbol=sample.symbol,
            net_exchange_flow_usd=sample.net_exchange_flow_usd,
            largest_txn_usd=sample.largest_txn_usd,
            txn_count=sample.txn_count,
            window_sec=sample.window_sec,
            ts_ns=sample.ts_ns,
        )
        await self._on_sample(sample)
        return sample

    async def run(self) -> None:
        while True:
            await self.poll_once()
            await asyncio.sleep(self._poll_interval_sec)
