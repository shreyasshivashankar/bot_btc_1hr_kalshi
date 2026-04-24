"""Hyperliquid public WebSocket feed — `metaAndAssetCtxs` for BTC OI.

Hyperliquid's public info-WS exposes asset-context updates that include
`openInterest` (BTC-denominated) and `markPx`. We subscribe once on
session start, then every push (initial + periodic) yields a fresh
`OpenInterestSample` (USD-denominated, source=hyperliquid).

Why Hyperliquid: it's the largest auth-less perp venue with a public
WS that pushes OI on a sub-minute cadence. The Coinglass HTTP poller
this replaces is a 30s polling loop with weaker rate-limit guarantees;
moving to a push-based source closes the staleness gap that motivated
the migration to a `DerivativesOracle` (mirror of `SpotOracle`).

Wire format (public docs, as of 2026):

    Client -> server (subscribe, sent once on session start):
        {"method":"subscribe","subscription":{"type":"metaAndAssetCtxs"}}

    Server -> client (subscription ack — ignored):
        {"channel":"subscriptionResponse","data":{...}}

    Server -> client (data — initial snapshot + periodic updates):
        {
          "channel":"metaAndAssetCtxs",
          "data":[
            {"universe":[{"name":"BTC",...},{"name":"ETH",...},...]},
            [
              {"funding":"0.0000125","openInterest":"23456.78","markPx":"67000.0",...},
              {...},  // ETH
              ...
            ]
          ]
        }

The `data` array's first element (universe) is parallel-indexed with the
second element (per-asset contexts). We look up BTC by name once per
frame — the index is not stable across protocol versions, so caching it
would invite a silent feed-mismatch bug.

Numeric fields are JSON strings. The parser converts at the boundary.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import orjson
import structlog

from bot_btc_1hr_kalshi.market_data.types import OpenInterestSample
from bot_btc_1hr_kalshi.obs.clock import Clock

_log = structlog.get_logger("bot_btc_1hr_kalshi.feed.hyperliquid")

HYPERLIQUID_WS_URL = "wss://api.hyperliquid.xyz/ws"
HYPERLIQUID_SOURCE = "hyperliquid"


class HyperliquidParseError(ValueError):
    """Frame did not match the documented `metaAndAssetCtxs` contract."""


def build_hyperliquid_subscribe() -> bytes:
    return orjson.dumps(
        {"method": "subscribe", "subscription": {"type": "metaAndAssetCtxs"}}
    )


def parse_hyperliquid_meta_and_asset_ctxs(
    raw: bytes | str,
    *,
    asset: str,
    recv_ts_ns: int,
) -> OpenInterestSample | None:
    """Parse one `metaAndAssetCtxs` frame.

    Returns None for non-data frames (subscription acks, heartbeats,
    other channels). Raises `HyperliquidParseError` only for *malformed*
    `metaAndAssetCtxs` payloads — wire-protocol drift the caller should
    log loudly, not for routine non-data frames.

    OI on Hyperliquid is denominated in the underlying (BTC for the
    BTC perp). We convert to USD using the same frame's `markPx` so
    the `total_oi_usd` field on `OpenInterestSample` stays apples-to-
    apples with the prior Coinglass-sourced values.
    """
    try:
        data = orjson.loads(raw)
    except orjson.JSONDecodeError as exc:
        raise HyperliquidParseError(f"invalid JSON: {exc}") from exc
    if not isinstance(data, dict):
        return None
    channel = data.get("channel")
    if channel != "metaAndAssetCtxs":
        return None
    payload = data.get("data")
    if not isinstance(payload, list) or len(payload) != 2:
        raise HyperliquidParseError(
            f"expected data=[universe, ctxs] pair; got {type(payload).__name__}"
        )
    meta, ctxs = payload
    if not isinstance(meta, dict) or not isinstance(ctxs, list):
        raise HyperliquidParseError("malformed [meta, ctxs] shape")
    universe = meta.get("universe")
    if not isinstance(universe, list):
        raise HyperliquidParseError("meta.universe is not a list")

    idx = _find_asset_index(universe, asset)
    if idx is None or idx >= len(ctxs):
        # Asset not in this account's universe (or universe truncated).
        # Not a wire error — return None so the caller skips this frame.
        return None
    ctx = ctxs[idx]
    if not isinstance(ctx, dict):
        raise HyperliquidParseError(f"ctx at index {idx} is not an object")

    try:
        oi_underlying = float(ctx["openInterest"])
        mark_px = float(ctx["markPx"])
    except (KeyError, ValueError, TypeError) as exc:
        raise HyperliquidParseError(f"missing openInterest/markPx: {exc}") from exc

    return OpenInterestSample(
        ts_ns=recv_ts_ns,
        symbol=asset,
        total_oi_usd=oi_underlying * mark_px,
        # Hyperliquid's OI is venue-internal (its own perp book), not an
        # aggregation across exchanges. Use 1 so log readers can distinguish
        # single-venue from multi-venue (Coinglass) provenance.
        exchanges_count=1,
        source=HYPERLIQUID_SOURCE,
    )


def _find_asset_index(universe: list[Any], asset: str) -> int | None:
    for i, entry in enumerate(universe):
        if isinstance(entry, dict) and entry.get("name") == asset:
            return i
    return None


def hyperliquid_parser(
    *, asset: str, clock: Clock
) -> Callable[[bytes | str], OpenInterestSample | None]:
    """Bind the parser to a clock + asset for use in `DerivativesFeed`."""

    def _p(raw: bytes | str) -> OpenInterestSample | None:
        return parse_hyperliquid_meta_and_asset_ctxs(
            raw, asset=asset, recv_ts_ns=clock.now_ns()
        )

    return _p
