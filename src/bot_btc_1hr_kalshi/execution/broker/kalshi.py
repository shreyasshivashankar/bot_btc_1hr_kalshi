"""Kalshi REST broker — implements the Broker protocol against the live API.

Endpoints (v2 trading API, documented at docs.kalshi.com):
    POST   /trade-api/v2/portfolio/orders          create
    DELETE /trade-api/v2/portfolio/orders/{id}     cancel
    GET    /trade-api/v2/portfolio/orders?status=resting
    GET    /trade-api/v2/portfolio/positions

Signing handled by KalshiSigner (RSA-PSS-SHA256, per DESIGN.md).

This module focuses on wire format + typed translation. All network calls
go through an injected `httpx.AsyncClient` so tests can use MockTransport.
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from typing import Any

import httpx
import orjson
import structlog

from bot_btc_1hr_kalshi.execution.broker.base import (
    BrokerPosition,
    Fill,
    OrderAck,
    OrderRequest,
    OrderStatus,
)
from bot_btc_1hr_kalshi.execution.broker.kalshi_signer import KalshiSigner
from bot_btc_1hr_kalshi.market_data.types import TradeEvent
from bot_btc_1hr_kalshi.obs.clock import Clock
from bot_btc_1hr_kalshi.obs.schemas import Side

_log = structlog.get_logger("bot_btc_1hr_kalshi.broker.kalshi")


class KalshiBrokerError(RuntimeError):
    """Raised on unrecoverable broker errors (HTTP 5xx, auth failure)."""


_TIF_FOR_TYPE = {"maker": "GTC", "ioc": "IOC"}


def _status_from_kalshi(k_status: str, remaining: int) -> OrderStatus:
    s = k_status.lower()
    if s in ("resting", "open", "working"):
        return "resting"
    if s in ("filled", "executed"):
        return "filled"
    if s in ("partially_filled", "partial_fill", "partial"):
        return "partially_filled" if remaining > 0 else "filled"
    if s in ("cancelled", "canceled"):
        return "cancelled"
    if s in ("rejected",):
        return "rejected"
    # Unknown → be conservative: if remaining=0 treat as filled, else resting.
    return "filled" if remaining == 0 else "resting"


def _fill_from_kalshi(raw: dict[str, Any], *, req: OrderRequest, order_id: str, fallback_ts_ns: int) -> Fill:
    price = int(raw.get("yes_price", req.limit_price_cents))
    contracts = int(raw.get("count", 0))
    ts_ms = int(raw.get("created_time_ms") or raw.get("ts") or 0)
    ts_ns = ts_ms * 1_000_000 if ts_ms > 0 else fallback_ts_ns
    fees_usd = float(raw.get("maker_fee", 0.0)) + float(raw.get("taker_fee", 0.0))
    return Fill(
        order_id=order_id,
        client_order_id=req.client_order_id,
        market_id=req.market_id,
        side=req.side,
        action=req.action,
        price_cents=price,
        contracts=contracts,
        ts_ns=ts_ns,
        fees_usd=fees_usd,
    )


class KalshiBroker:
    """Live Kalshi trading-api adapter. Network calls go through the injected client."""

    def __init__(
        self,
        *,
        client: httpx.AsyncClient,
        signer: KalshiSigner,
        clock: Clock,
        api_base: str = "/trade-api/v2",
        max_retries: int = 3,
        retry_initial_sec: float = 0.25,
        retry_max_sec: float = 8.0,
        sleep: Callable[[float], Awaitable[None]] | None = None,
        rng: Callable[[], float] | None = None,
    ) -> None:
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if retry_initial_sec <= 0:
            raise ValueError("retry_initial_sec must be > 0")
        if retry_max_sec < retry_initial_sec:
            raise ValueError("retry_max_sec must be >= retry_initial_sec")
        self._client = client
        self._signer = signer
        self._clock = clock
        self._base = api_base.rstrip("/")
        self._max_retries = max_retries
        self._retry_initial = retry_initial_sec
        self._retry_max = retry_max_sec
        self._sleep = sleep or asyncio.sleep
        self._rng = rng or random.random

    async def _request(
        self, method: str, path: str, *, body: bytes | None = None
    ) -> httpx.Response:
        full_path = f"{self._base}{path}"
        backoff = self._retry_initial
        # We re-sign every attempt because the timestamp is part of the
        # signature — a replay that crosses the server's ts-window is
        # auth-rejected (hard rule #5 spirit: clock is authoritative).
        for attempt in range(self._max_retries + 1):
            headers = self._signer.headers(method=method, path=full_path)
            if body is not None:
                headers["Content-Type"] = "application/json"
            resp = await self._client.request(
                method, full_path, headers=headers, content=body,
            )
            if resp.status_code == 429:
                if attempt >= self._max_retries:
                    # Retries exhausted. Surface as an error, don't return
                    # the 429 as if it were a routine rejection — the caller
                    # needs to halt/alert rather than silently retry at the
                    # OMS layer.
                    break
                retry_after = _parse_retry_after(resp)
                # Full jitter: sleep ∈ [0, backoff). Server Retry-After wins
                # if present, since the exchange is telling us exactly how
                # long to wait (and crossing it is grounds for a 1hr ban).
                sleep_for = (
                    retry_after
                    if retry_after is not None
                    else self._rng() * backoff
                )
                _log.warning(
                    "broker.kalshi.rate_limited",
                    method=method,
                    path=full_path,
                    attempt=attempt + 1,
                    sleep_sec=sleep_for,
                    retry_after_header=retry_after,
                )
                await self._sleep(sleep_for)
                backoff = min(self._retry_max, backoff * 2.0)
                continue
            if resp.status_code >= 500:
                raise KalshiBrokerError(
                    f"{method} {full_path} → {resp.status_code}: {resp.text}"
                )
            if resp.status_code == 401:
                raise KalshiBrokerError(f"auth_failed: {resp.text}")
            return resp
        # Retries exhausted on 429. A persistent rate-limit at this point
        # means the caller should surface the failure; do not silently
        # succeed with the last 429 response.
        raise KalshiBrokerError(
            f"rate_limited_exhausted: {method} {full_path} after {self._max_retries} retries"
        )

    async def submit(self, req: OrderRequest) -> OrderAck:
        body = orjson.dumps({
            "ticker": req.market_id,
            "client_order_id": req.client_order_id,
            "side": req.side.lower(),
            "action": req.action.lower(),
            "count": req.contracts,
            "type": "limit",
            "time_in_force": _TIF_FOR_TYPE[req.order_type],
            ("yes_price" if req.side == "YES" else "no_price"): req.limit_price_cents,
        })
        resp = await self._request("POST", "/portfolio/orders", body=body)
        if resp.status_code >= 400:
            reason = _reason_from_body(resp)
            _log.warning("broker.kalshi.submit_rejected", reason=reason, client_order_id=req.client_order_id)
            return OrderAck(
                order_id="",
                client_order_id=req.client_order_id,
                status="rejected",
                filled_contracts=0,
                remaining_contracts=req.contracts,
                fills=(),
                reason=reason,
            )
        data = orjson.loads(resp.content)
        order = data.get("order", data)
        return _ack_from_order(order, req=req, fallback_ts_ns=self._clock.now_ns())

    async def cancel(self, order_id: str) -> bool:
        if not order_id:
            return False
        resp = await self._request("DELETE", f"/portfolio/orders/{order_id}")
        if resp.status_code in (200, 202, 204):
            return True
        if resp.status_code == 404:
            return False
        _log.warning("broker.kalshi.cancel_unexpected", status=resp.status_code, body=resp.text)
        return False

    async def list_open_orders(self) -> tuple[OrderAck, ...]:
        resp = await self._request("GET", "/portfolio/orders?status=resting")
        if resp.status_code != 200:
            raise KalshiBrokerError(f"list_open_orders: {resp.status_code} {resp.text}")
        data = orjson.loads(resp.content)
        orders = data.get("orders", [])
        acks: list[OrderAck] = []
        for o in orders:
            remaining = int(o.get("remaining_count", 0))
            filled = int(o.get("count", 0)) - remaining
            acks.append(OrderAck(
                order_id=str(o["order_id"]),
                client_order_id=str(o.get("client_order_id", "")),
                status=_status_from_kalshi(o.get("status", "resting"), remaining),
                filled_contracts=max(0, filled),
                remaining_contracts=remaining,
                fills=(),
            ))
        return tuple(acks)

    async def match_trade(self, trade: TradeEvent) -> tuple[Fill, ...]:
        """Live broker doesn't synthesize fills from the public tape — fills
        come back via the order ack on submit, the (future) WS order channel,
        and the periodic reconciler. The Broker protocol mandates this method
        so the OMS smart-router exit path can call it uniformly across paper
        and live; here it's an explicit no-op."""
        return ()

    async def list_positions(self) -> tuple[BrokerPosition, ...]:
        resp = await self._request("GET", "/portfolio/positions")
        if resp.status_code != 200:
            raise KalshiBrokerError(f"list_positions: {resp.status_code} {resp.text}")
        data = orjson.loads(resp.content)
        positions = data.get("market_positions", [])
        out: list[BrokerPosition] = []
        for p in positions:
            contracts = int(p.get("position", 0))
            if contracts == 0:
                continue
            # Kalshi returns YES contracts as a signed quantity (positive = long YES,
            # negative = long NO). We normalize: NO holdings are flipped to Side=NO
            # with positive contract count.
            side: Side = "YES" if contracts > 0 else "NO"
            abs_contracts = abs(contracts)
            avg_entry = int(p.get("market_exposure", 0)) // abs_contracts if abs_contracts else 0
            out.append(BrokerPosition(
                market_id=str(p["ticker"]),
                side=side,
                contracts=abs_contracts,
                avg_entry_price_cents=avg_entry,
            ))
        return tuple(out)


def _parse_retry_after(resp: httpx.Response) -> float | None:
    """Parse Retry-After (RFC 7231). Kalshi sends integer seconds; we ignore
    HTTP-date forms for now since the exchange does not emit them. Returns
    None on any parse failure so the caller falls back to exponential
    backoff with jitter."""
    raw = resp.headers.get("Retry-After") or resp.headers.get("retry-after")
    if not raw:
        return None
    try:
        val = float(raw.strip())
    except ValueError:
        return None
    if val < 0:
        return None
    return val


def _reason_from_body(resp: httpx.Response) -> str:
    try:
        data = orjson.loads(resp.content)
    except orjson.JSONDecodeError:
        return f"http_{resp.status_code}"
    if isinstance(data, dict):
        err = data.get("error") or data.get("message") or data.get("detail")
        if isinstance(err, str) and err:
            return err
    return f"http_{resp.status_code}"


def _ack_from_order(order: dict[str, Any], *, req: OrderRequest, fallback_ts_ns: int) -> OrderAck:
    order_id = str(order.get("order_id", ""))
    remaining = int(order.get("remaining_count", 0))
    filled = int(order.get("count", req.contracts)) - remaining
    fills_raw = order.get("fills") or []
    fills = tuple(
        _fill_from_kalshi(f, req=req, order_id=order_id, fallback_ts_ns=fallback_ts_ns)
        for f in fills_raw
    )
    return OrderAck(
        order_id=order_id,
        client_order_id=req.client_order_id,
        status=_status_from_kalshi(str(order.get("status", "resting")), remaining),
        filled_contracts=max(0, filled),
        remaining_contracts=remaining,
        fills=fills,
    )
