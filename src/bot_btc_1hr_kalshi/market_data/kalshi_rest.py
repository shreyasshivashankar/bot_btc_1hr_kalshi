"""Kalshi REST client — market discovery.

Thin wrapper around Kalshi's public `/markets` endpoint. The bot needs to
auto-discover the currently-open BTC hourly market on every hour-roll
(so we don't hard-code tickers like `KXBTC-26APR1717-B60000` which would
drift out of date within the hour).

Public market metadata (listing, titles, strikes) is unauthenticated — we
use `httpx.AsyncClient` without a signer here. Signed endpoints
(`/portfolio/*`) live in `execution/broker/kalshi.py`.

The BTC hourly series ticker is `KXBTC` as of 2026. It is configurable via
`series_ticker` so the strategy can be retargeted without code changes.
"""

from __future__ import annotations

import asyncio
import datetime as dt
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from typing import Any

import httpx
import orjson
import structlog

from bot_btc_1hr_kalshi.risk.clock_drift import NtpProbe

_log = structlog.get_logger("bot_btc_1hr_kalshi.market_discovery")


@dataclass(frozen=True, slots=True)
class HourlyMarket:
    ticker: str
    strike_usd: float
    settlement_ts_ns: int
    status: str


class MarketDiscoveryError(RuntimeError):
    """Raised when no open market matches the discovery window."""


def _parse_iso_to_ns(s: str) -> int:
    # Kalshi uses RFC3339 (`...Z` or `+00:00`). datetime.fromisoformat handles
    # the `+00:00` form natively; normalize `Z` first.
    clean = s.replace("Z", "+00:00") if s.endswith("Z") else s
    t = dt.datetime.fromisoformat(clean)
    return int(t.timestamp() * 1_000_000_000)


def _extract_strike_usd(market: dict[str, Any]) -> float | None:
    """Pick the strike out of a market object.

    Kalshi hourly BTC markets are binary above/below markets; the strike
    lives in `floor_strike` (for "above X" / B-style) or `cap_strike`
    (for "below X" / T-style). Older ticker formats also encode the strike
    as the trailing digits after `-B` or `-T`, which we use as a fallback
    when the structured fields are absent.
    """
    for key in ("floor_strike", "cap_strike"):
        val = market.get(key)
        if val is not None:
            try:
                return float(val)
            except (TypeError, ValueError):
                pass
    ticker = str(market.get("ticker", ""))
    for sep in ("-B", "-T"):
        if sep in ticker:
            tail = ticker.rsplit(sep, 1)[-1]
            try:
                return float(tail)
            except ValueError:
                return None
    return None


class KalshiRestClient:
    """Public-endpoint REST client for market discovery."""

    def __init__(
        self,
        *,
        client: httpx.AsyncClient,
        api_base: str = "/trade-api/v2",
    ) -> None:
        self._client = client
        self._base = api_base.rstrip("/")

    async def list_open_markets(self, *, series_ticker: str = "KXBTC") -> list[dict[str, Any]]:
        """Return the raw `markets` array for an open-status listing.

        Paginates via `cursor` until exhausted. Hourly series usually
        return one page, but we page defensively in case Kalshi starts
        listing the full day up-front.
        """
        out: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            params = {"series_ticker": series_ticker, "status": "open", "limit": "100"}
            if cursor:
                params["cursor"] = cursor
            resp = await self._client.get(f"{self._base}/markets", params=params)
            if resp.status_code != 200:
                raise MarketDiscoveryError(
                    f"markets list: {resp.status_code} {resp.text[:200]}"
                )
            data = orjson.loads(resp.content)
            out.extend(data.get("markets", []) or [])
            cursor = data.get("cursor") or None
            if not cursor:
                break
            # Space out paginated requests so a multi-page listing can't burst
            # past Kalshi's retail read rate-limit (~10 req/s) on boot.
            await asyncio.sleep(0.1)
        return out

    async def list_btc_hourly_markets(
        self,
        *,
        now_ns: int,
        series_ticker: str = "KXBTC",
        max_horizon_sec: int = 3600,
        btc_spot_usd: float | None = None,
        max_markets: int = 5,
    ) -> list[HourlyMarket]:
        """Return up to `max_markets` open hourly markets of the next
        settlement, ranked by `|strike - btc_spot_usd|`.

        Multi-strike era (Slice: correlation cap + multi-book routing).
        All returned markets share the earliest future `settlement_ts_ns`
        — mixing settlements breaks the correlation cap's key identity
        (the cap counts per-hour same-side positions, and two strikes
        settling different hours are NOT structurally correlated).

        Ranking:
          1. Filter: future settlement within `max_horizon_sec`.
          2. Keep only candidates sharing the soonest `settlement_ts_ns`
             (drop later hours even if more strikes are listed).
          3. Secondary sort: smallest `|strike - btc_spot_usd|` when spot
             is provided (tiebreak alphabetical ticker for determinism).
          4. If `btc_spot_usd is None`, falls back to alphabetical tiebreak
             so legacy callers and unit tests still get deterministic
             output. Live callers MUST pass spot — the feedloop enforces
             this via the SpotOracle's fail-closed `get_primary`.

        First item is always the "primary" market (closest-to-spot) and
        is what trap evaluation drives off until the cross-sectional
        evaluator ships.

        Raises `MarketDiscoveryError` if no market matches — caller should
        backoff and retry rather than trading blind.
        """
        if max_markets < 1:
            raise ValueError("max_markets must be >= 1")
        markets = await self.list_open_markets(series_ticker=series_ticker)
        horizon_ns = now_ns + max_horizon_sec * 1_000_000_000
        candidates: list[HourlyMarket] = []
        for m in markets:
            exp_raw = m.get("expected_expiration_time") or m.get("close_time")
            if not exp_raw:
                continue
            try:
                settlement_ns = _parse_iso_to_ns(str(exp_raw))
            except ValueError:
                continue
            if settlement_ns <= now_ns or settlement_ns > horizon_ns:
                continue
            strike = _extract_strike_usd(m)
            if strike is None:
                continue
            candidates.append(HourlyMarket(
                ticker=str(m["ticker"]),
                strike_usd=strike,
                settlement_ts_ns=settlement_ns,
                status=str(m.get("status", "")),
            ))

        if not candidates:
            raise MarketDiscoveryError(
                f"no open {series_ticker} market with settlement in next "
                f"{max_horizon_sec}s (found {len(markets)} listings)"
            )

        # Keep only the soonest settlement — the correlation cap counts
        # same-hour same-side positions, so mixing hours would silently
        # break its semantics.
        soonest_ns = min(c.settlement_ts_ns for c in candidates)
        candidates = [c for c in candidates if c.settlement_ts_ns == soonest_ns]

        if btc_spot_usd is not None:
            candidates.sort(
                key=lambda c: (abs(c.strike_usd - btc_spot_usd), c.ticker)
            )
        else:
            candidates.sort(key=lambda c: c.ticker)
        chosen = candidates[:max_markets]
        primary = chosen[0]
        _log.info(
            "market_discovery.selected",
            ticker=primary.ticker,
            strike_usd=primary.strike_usd,
            settlement_ts_ns=primary.settlement_ts_ns,
            candidate_count=len(candidates),
            tracked_count=len(chosen),
            tracked_tickers=[m.ticker for m in chosen],
            btc_spot_usd=btc_spot_usd,
            strike_gap_usd=(
                abs(primary.strike_usd - btc_spot_usd)
                if btc_spot_usd is not None
                else None
            ),
        )
        return chosen

    async def current_btc_hourly_market(
        self,
        *,
        now_ns: int,
        series_ticker: str = "KXBTC",
        max_horizon_sec: int = 3600,
        btc_spot_usd: float | None = None,
    ) -> HourlyMarket:
        """Back-compat single-market discovery — returns the primary market
        from `list_btc_hourly_markets(max_markets=1)`. New callers should
        use `list_btc_hourly_markets` directly.
        """
        markets = await self.list_btc_hourly_markets(
            now_ns=now_ns,
            series_ticker=series_ticker,
            max_horizon_sec=max_horizon_sec,
            btc_spot_usd=btc_spot_usd,
            max_markets=1,
        )
        return markets[0]


_TRUNCATION_MIDPOINT_NS = 500_000_000  # 500ms — see docstring below


def kalshi_date_header_probe(
    client: httpx.AsyncClient,
    *,
    path: str = "/exchange/status",
) -> NtpProbe:
    """Clock-drift probe built on Kalshi's HTTP `Date` response header.

    `client.base_url` is expected to already include the `/trade-api/v2`
    prefix (that is how it is wired in `__main__._start_feed_loop_if_enabled`),
    so `path` defaults to the server-relative suffix. Kalshi's own server
    clock is our anchor for signed-request validity, not ground-truth UTC —
    what we halt on is disagreement with the server that validates the
    `KALSHI-ACCESS-TIMESTAMP` header.

    Resolution: RFC 7231 `Date` is 1-second truncated (floor). A raw parse
    would systematically under-report server time by a uniform 0-1000 ms,
    triggering a false-positive halt on any threshold below 1 s. We shift
    the return value by +500 ms so a perfectly-synced clock measures 0 drift
    on average, with residual noise uniformly ±500 ms. Callers should pair
    this probe with `clock_drift_halt_ms >= 1000` (config default). If
    Kalshi ever ships a millisecond `server_time` JSON field, drop the
    offset and tighten the threshold.
    """

    async def _probe() -> int:
        resp = await client.get(path)
        resp.raise_for_status()
        date_hdr = resp.headers.get("Date", "")
        if not date_hdr:
            raise RuntimeError("kalshi /exchange/status returned no Date header")
        parsed = parsedate_to_datetime(date_hdr)
        return int(parsed.timestamp() * 1_000_000_000) + _TRUNCATION_MIDPOINT_NS

    return _probe
