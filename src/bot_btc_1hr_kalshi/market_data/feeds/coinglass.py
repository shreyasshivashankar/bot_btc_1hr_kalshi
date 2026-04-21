"""Coinglass open-interest poller (Slice 11 P2 — shadow-mode only).

Polls the Coinglass v4 aggregated-open-interest endpoint on a fixed
cadence and emits `OpenInterestSample` via a caller-provided async
callback. The caller (typically `__main__`) stashes the latest sample on
`App.latest_open_interest` for optional MarketSnapshot attachment, and
also emits a structured log record per sample so paper-soak telemetry
can inform a future microstructure-gated promotion decision.

Design notes
------------
* **Observational, not gating.** The signal layer's `MarketSnapshot.
  open_interest` field is opt-in and unused by traps today. This module
  is scaffolding for a future slice; wiring an entry gate to OI spikes
  would be a *behavioral change* requiring risk-committee sign-off per
  docs/RISK.md.
* **Fail-soft HTTP.** The polling loop swallows HTTP + parse errors,
  logs a warning, and retries next cycle. Upstream API instability
  must not crash the trading graph.
* **API-key optional.** Free-tier Coinglass endpoints accept unkeyed
  requests with stricter rate limits. The `CG-API-KEY` header is added
  only when `api_key` is non-empty so dev boots without credentials.
* **Stateless parse.** `parse_coinglass_response` is a pure function
  that turns response bytes into an `OpenInterestSample`. Easy to
  unit-test against MockTransport and to replay from captured fixtures.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

import httpx
import orjson
import structlog

from bot_btc_1hr_kalshi.market_data.types import OpenInterestSample
from bot_btc_1hr_kalshi.obs.clock import Clock

log = structlog.get_logger(__name__)

COINGLASS_DEFAULT_BASE_URL = "https://open-api-v4.coinglass.com"
COINGLASS_DEFAULT_OI_PATH = "/api/futures/open-interest/aggregated-history"
COINGLASS_API_KEY_HEADER = "CG-API-KEY"


class CoinglassParseError(ValueError):
    """Response shape did not match the documented v4 contract."""


def parse_coinglass_response(
    raw: bytes | str,
    *,
    symbol: str,
    clock: Clock,
) -> OpenInterestSample:
    """Parse a Coinglass aggregated-open-interest response into a single
    `OpenInterestSample`. Uses the most recent `data[-1]` entry — we poll
    faster than the API's candle granularity, so the latest sample is the
    one worth recording.

    Expected envelope (v4 public contract):

        {
          "code": "0",
          "msg": "success",
          "data": [
            {"time": 1713715200000, "aggregated_open_interest": 12345678.9, ...},
            ...
          ]
        }

    `ts_ns` falls back to `clock.now_ns()` if the response omits `time` —
    polling cadence is already fine-grained enough that server timestamp
    ~= observation time for logging purposes.
    """
    payload: Any = orjson.loads(raw)
    if not isinstance(payload, dict):
        raise CoinglassParseError(f"expected JSON object, got {type(payload).__name__}")
    # v4 signals success via `code == "0"`. Treat everything else as a
    # transport-layer failure so the outer loop retries next cycle.
    code = payload.get("code")
    if code not in ("0", 0):
        raise CoinglassParseError(f"coinglass code={code!r} msg={payload.get('msg')!r}")
    data = payload.get("data")
    if not isinstance(data, list) or not data:
        raise CoinglassParseError("response `data` is missing or empty")
    latest = data[-1]
    if not isinstance(latest, dict):
        raise CoinglassParseError("data[-1] is not an object")

    oi_usd = _extract_oi_usd(latest)
    ts_ns = _extract_ts_ns(latest, clock)
    exchanges = latest.get("exchange_count")
    if not isinstance(exchanges, int):
        exchanges = None
    return OpenInterestSample(
        ts_ns=ts_ns,
        symbol=symbol,
        total_oi_usd=oi_usd,
        exchanges_count=exchanges,
    )


def _extract_oi_usd(row: dict[str, Any]) -> float:
    # Coinglass v4 uses different field names across endpoints; accept
    # any of the documented variants to avoid per-endpoint parsers.
    for key in (
        "aggregated_open_interest_usd",
        "aggregated_open_interest",
        "open_interest_usd",
        "open_interest",
    ):
        val = row.get(key)
        if isinstance(val, int | float):
            return float(val)
    raise CoinglassParseError(f"no recognized open-interest field in {sorted(row.keys())}")


def _extract_ts_ns(row: dict[str, Any], clock: Clock) -> int:
    ts = row.get("time") or row.get("timestamp")
    if isinstance(ts, int | float):
        # Coinglass serializes in ms epoch — promote to ns.
        return int(ts) * 1_000_000
    return clock.now_ns()


async def fetch_coinglass_oi(
    *,
    client: httpx.AsyncClient,
    base_url: str = COINGLASS_DEFAULT_BASE_URL,
    path: str = COINGLASS_DEFAULT_OI_PATH,
    symbol: str = "BTC",
    interval: str = "5m",
    limit: int = 1,
    api_key: str | None = None,
    clock: Clock,
) -> OpenInterestSample:
    """Single fetch+parse. Raises on HTTP / decode errors so the caller
    can decide whether to swallow (polling loop) or propagate (tests)."""
    headers: dict[str, str] = {}
    if api_key:
        headers[COINGLASS_API_KEY_HEADER] = api_key
    params = {"symbol": symbol, "interval": interval, "limit": str(limit)}
    resp = await client.get(f"{base_url}{path}", params=params, headers=headers)
    resp.raise_for_status()
    return parse_coinglass_response(resp.content, symbol=symbol, clock=clock)


class CoinglassPoller:
    """Background loop — fetch OI on cadence, publish via callback.

    Parameters
    ----------
    client
        Shared httpx.AsyncClient (injected so tests can stub with
        MockTransport; production owns the client lifecycle).
    on_sample
        Async callback invoked on each successful fetch. Typically
        updates `App.latest_open_interest` and emits a structured log.
        NOT invoked on fetch failure — callers retain the prior sample.
    api_key
        Optional Coinglass API key. When empty, requests omit the
        `CG-API-KEY` header (free-tier fallback, stricter rate limits).
    poll_interval_sec
        Cadence between *completed* cycles. Slow endpoints can't
        stampede: the sleep starts only after the previous fetch
        resolves (success or failure).
    """

    __slots__ = (
        "_api_key",
        "_base_url",
        "_client",
        "_clock",
        "_interval",
        "_limit",
        "_oi_path",
        "_on_sample",
        "_poll_interval_sec",
        "_symbol",
    )

    def __init__(
        self,
        *,
        client: httpx.AsyncClient,
        clock: Clock,
        on_sample: Callable[[OpenInterestSample], Awaitable[None]],
        base_url: str = COINGLASS_DEFAULT_BASE_URL,
        oi_path: str = COINGLASS_DEFAULT_OI_PATH,
        symbol: str = "BTC",
        interval: str = "5m",
        limit: int = 1,
        api_key: str | None = None,
        poll_interval_sec: float = 30.0,
    ) -> None:
        if poll_interval_sec <= 0:
            raise ValueError("poll_interval_sec must be > 0")
        if limit <= 0:
            raise ValueError("limit must be > 0")
        self._client = client
        self._clock = clock
        self._on_sample = on_sample
        self._base_url = base_url
        self._oi_path = oi_path
        self._symbol = symbol
        self._interval = interval
        self._limit = limit
        self._api_key = api_key or None
        self._poll_interval_sec = poll_interval_sec

    async def poll_once(self) -> OpenInterestSample | None:
        """Single fetch+publish cycle. Returns the sample on success or
        None on failure. Used by tests; `run()` calls this in a loop."""
        try:
            sample = await fetch_coinglass_oi(
                client=self._client,
                base_url=self._base_url,
                path=self._oi_path,
                symbol=self._symbol,
                interval=self._interval,
                limit=self._limit,
                api_key=self._api_key,
                clock=self._clock,
            )
        except (httpx.HTTPError, CoinglassParseError, ValueError) as exc:
            log.warning(
                "coinglass.fetch_failed",
                url=f"{self._base_url}{self._oi_path}",
                symbol=self._symbol,
                error=str(exc),
                exc_type=type(exc).__name__,
            )
            return None
        log.info(
            "coinglass.oi_sample",
            symbol=sample.symbol,
            total_oi_usd=sample.total_oi_usd,
            exchanges=sample.exchanges_count,
            ts_ns=sample.ts_ns,
        )
        await self._on_sample(sample)
        return sample

    async def run(self) -> None:
        while True:
            await self.poll_once()
            await asyncio.sleep(self._poll_interval_sec)
