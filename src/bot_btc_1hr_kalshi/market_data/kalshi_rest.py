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

import datetime as dt
from dataclasses import dataclass
from typing import Any

import httpx
import orjson
import structlog

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
        return out

    async def current_btc_hourly_market(
        self,
        *,
        now_ns: int,
        series_ticker: str = "KXBTC",
        max_horizon_sec: int = 3600,
    ) -> HourlyMarket:
        """Find the hourly market whose settlement is the next one after now.

        We pick the market with the *soonest* future settlement within
        `max_horizon_sec` (default: one hour). Ties (shouldn't happen in
        practice) broken alphabetically on ticker for determinism.

        Raises `MarketDiscoveryError` if no market matches — caller should
        backoff and retry rather than trading blind.
        """
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

        # Soonest settlement wins; deterministic tiebreak on ticker.
        candidates.sort(key=lambda c: (c.settlement_ts_ns, c.ticker))
        chosen = candidates[0]
        _log.info(
            "market_discovery.selected",
            ticker=chosen.ticker,
            strike_usd=chosen.strike_usd,
            settlement_ts_ns=chosen.settlement_ts_ns,
            candidate_count=len(candidates),
        )
        return chosen
