"""Coinglass liquidation-heatmap poller (Slice 11 P3 — shadow-mode only).

Polls the Coinglass v4 aggregated-liquidation-heatmap endpoint on a fixed
cadence and emits a compressed `LiquidationHeatmapSample` via a caller-
provided async callback. The caller (typically `__main__`) stashes the
latest sample on `App.latest_liquidation_heatmap` and emits a structured
log record so paper-soak telemetry can inform a future microstructure-
gated promotion decision.

Why compress? The raw payload is a (time, price) density grid; the full
matrix is dense enough to dominate the log budget if emitted unshaped.
We distill it to three stats per poll:

* `total_liquidation_usd` — sum over the grid (overall activity proxy).
* `peak_cluster_price_usd` — price bucket with the densest cluster.
* `peak_cluster_liquidation_usd` — the density of that bucket.

Same observational-only contract as `coinglass.py`: gating any trap
entry on these numbers is a *behavioral change* requiring risk-committee
sign-off per docs/RISK.md. The module exists today so a promotion
decision has real paper-soak data to lean on.

Design mirrors `market_data.feeds.coinglass.CoinglassPoller`: shared
httpx client injected, fail-soft polling loop, pure-function parser
unit-testable via MockTransport + canned fixture.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

import httpx
import orjson
import structlog

from bot_btc_1hr_kalshi.market_data.types import LiquidationHeatmapSample
from bot_btc_1hr_kalshi.obs.clock import Clock

log = structlog.get_logger(__name__)

COINGLASS_HEATMAP_DEFAULT_BASE_URL = "https://open-api-v4.coinglass.com"
COINGLASS_HEATMAP_DEFAULT_PATH = "/api/futures/liquidation/aggregated-heatmap"
COINGLASS_API_KEY_HEADER = "CG-API-KEY"


class CoinglassHeatmapParseError(ValueError):
    """Response shape did not match the documented v4 heatmap contract."""


def parse_coinglass_heatmap_response(
    raw: bytes | str,
    *,
    symbol: str,
    clock: Clock,
) -> LiquidationHeatmapSample:
    """Parse a Coinglass liquidation-heatmap response into a summary sample.

    The v4 endpoint returns a (time x price) density grid. The expected
    envelope (common fields across documented variants):

        {
          "code": "0",
          "msg": "success",
          "data": {
            "y":   [price_0, price_1, ...],        # price-axis labels
            "liq": [[time_idx, price_idx, usd], ...]   # density cells
            ...
          }
        }

    We accept either `liq` / `liquidation_data` / `heatmap` for the cell
    list and either `y` / `prices` for the price-axis labels — the v4
    endpoints have historically moved field names between minor versions
    and the parser prefers to keep working across them rather than ship
    a per-field rigid contract that breaks silently on a rename.
    """
    payload: Any = orjson.loads(raw)
    if not isinstance(payload, dict):
        raise CoinglassHeatmapParseError(
            f"expected JSON object, got {type(payload).__name__}"
        )
    code = payload.get("code")
    if code not in ("0", 0):
        raise CoinglassHeatmapParseError(
            f"coinglass code={code!r} msg={payload.get('msg')!r}"
        )
    data = payload.get("data")
    if not isinstance(data, dict):
        raise CoinglassHeatmapParseError("response `data` is missing or not an object")

    prices = _extract_price_axis(data)
    cells = _extract_cells(data)

    total_usd = 0.0
    peak_usd = -1.0
    peak_price_idx = -1
    for cell in cells:
        if not isinstance(cell, (list, tuple)) or len(cell) < 3:
            continue
        try:
            price_idx = int(cell[1])
            usd = float(cell[2])
        except (TypeError, ValueError):
            continue
        if usd <= 0:
            continue
        total_usd += usd
        if usd > peak_usd:
            peak_usd = usd
            peak_price_idx = price_idx

    if peak_price_idx < 0 or peak_price_idx >= len(prices):
        raise CoinglassHeatmapParseError(
            "no usable liquidation cells in heatmap response"
        )
    peak_price = float(prices[peak_price_idx])

    ts_ns = _extract_ts_ns(data, clock)
    return LiquidationHeatmapSample(
        ts_ns=ts_ns,
        symbol=symbol,
        total_liquidation_usd=total_usd,
        peak_cluster_price_usd=peak_price,
        peak_cluster_liquidation_usd=peak_usd,
    )


def _extract_price_axis(data: dict[str, Any]) -> list[float]:
    for key in ("y", "prices", "price"):
        val = data.get(key)
        if isinstance(val, list) and val and all(isinstance(p, int | float) for p in val):
            return [float(p) for p in val]
    raise CoinglassHeatmapParseError(
        f"no recognized price-axis field in {sorted(data.keys())}"
    )


def _extract_cells(data: dict[str, Any]) -> list[Any]:
    for key in ("liq", "liquidation_data", "heatmap", "data"):
        val = data.get(key)
        if isinstance(val, list):
            return val
    raise CoinglassHeatmapParseError(
        f"no recognized heatmap cell-list in {sorted(data.keys())}"
    )


def _extract_ts_ns(data: dict[str, Any], clock: Clock) -> int:
    # Some heatmap variants carry an `end_time` / `time` at the envelope
    # level marking when the grid's rightmost column closed; others leave
    # the caller to stamp observation time. Default to the caller clock
    # — poll cadence makes server-vs-observer skew immaterial for logs.
    ts = data.get("end_time") or data.get("time")
    if isinstance(ts, int | float):
        return int(ts) * 1_000_000  # Coinglass uses ms epoch.
    return clock.now_ns()


async def fetch_coinglass_heatmap(
    *,
    client: httpx.AsyncClient,
    base_url: str = COINGLASS_HEATMAP_DEFAULT_BASE_URL,
    path: str = COINGLASS_HEATMAP_DEFAULT_PATH,
    symbol: str = "BTC",
    interval: str = "1h",
    api_key: str | None = None,
    clock: Clock,
) -> LiquidationHeatmapSample:
    """Single fetch+parse. Raises on HTTP / decode errors so the caller
    decides whether to swallow (polling loop) or propagate (tests)."""
    headers: dict[str, str] = {}
    if api_key:
        headers[COINGLASS_API_KEY_HEADER] = api_key
    params = {"symbol": symbol, "interval": interval}
    resp = await client.get(f"{base_url}{path}", params=params, headers=headers)
    resp.raise_for_status()
    return parse_coinglass_heatmap_response(resp.content, symbol=symbol, clock=clock)


class CoinglassHeatmapPoller:
    """Background loop — fetch heatmap on cadence, publish via callback.

    Parameters mirror `coinglass.CoinglassPoller` so the startup wiring
    and shutdown path in `__main__` can treat both pollers identically.
    """

    __slots__ = (
        "_api_key",
        "_base_url",
        "_client",
        "_clock",
        "_heatmap_path",
        "_interval",
        "_on_sample",
        "_poll_interval_sec",
        "_symbol",
    )

    def __init__(
        self,
        *,
        client: httpx.AsyncClient,
        clock: Clock,
        on_sample: Callable[[LiquidationHeatmapSample], Awaitable[None]],
        base_url: str = COINGLASS_HEATMAP_DEFAULT_BASE_URL,
        heatmap_path: str = COINGLASS_HEATMAP_DEFAULT_PATH,
        symbol: str = "BTC",
        interval: str = "1h",
        api_key: str | None = None,
        poll_interval_sec: float = 60.0,
    ) -> None:
        if poll_interval_sec <= 0:
            raise ValueError("poll_interval_sec must be > 0")
        self._client = client
        self._clock = clock
        self._on_sample = on_sample
        self._base_url = base_url
        self._heatmap_path = heatmap_path
        self._symbol = symbol
        self._interval = interval
        self._api_key = api_key or None
        self._poll_interval_sec = poll_interval_sec

    async def poll_once(self) -> LiquidationHeatmapSample | None:
        """Single fetch+publish cycle. Returns the sample on success or
        None on failure. Used by tests; `run()` calls this in a loop."""
        try:
            sample = await fetch_coinglass_heatmap(
                client=self._client,
                base_url=self._base_url,
                path=self._heatmap_path,
                symbol=self._symbol,
                interval=self._interval,
                api_key=self._api_key,
                clock=self._clock,
            )
        except (httpx.HTTPError, CoinglassHeatmapParseError, ValueError) as exc:
            log.warning(
                "coinglass_heatmap.fetch_failed",
                url=f"{self._base_url}{self._heatmap_path}",
                symbol=self._symbol,
                error=str(exc),
                exc_type=type(exc).__name__,
            )
            return None
        log.info(
            "coinglass_heatmap.sample",
            symbol=sample.symbol,
            total_liquidation_usd=sample.total_liquidation_usd,
            peak_cluster_price_usd=sample.peak_cluster_price_usd,
            peak_cluster_liquidation_usd=sample.peak_cluster_liquidation_usd,
            ts_ns=sample.ts_ns,
        )
        await self._on_sample(sample)
        return sample

    async def run(self) -> None:
        while True:
            await self.poll_once()
            await asyncio.sleep(self._poll_interval_sec)
