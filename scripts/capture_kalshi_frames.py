"""Capture raw Kalshi WS frames to a JSONL fixture file.

Run against the live WS for long enough to observe every frame type we rely
on (snapshot, delta, trade, subscribed-ack, at minimum). The captured file
feeds `tests/unit/test_kalshi_parser.py` — pinning the parser against real
wire bytes so silent wire-format drift (like the `yes_dollars_fp` vs `yes`
mismatch we just hit) fails loudly in CI.

Usage:
  .venv/bin/python scripts/capture_kalshi_frames.py \\
      [--ticker KXBTC-26APR1801-B66250] \\
      [--duration 60] \\
      [--out tests/fixtures/kalshi_ws_frames.jsonl]

If --ticker is omitted, the script calls the Kalshi REST market-discovery
endpoint and picks the currently-open hourly BTC market closest to the
current price.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path

import httpx
import websockets

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

from bot_btc_1hr_kalshi.execution.broker.kalshi_signer import KalshiSigner  # noqa: E402
from bot_btc_1hr_kalshi.obs.clock import SystemClock  # noqa: E402

WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"
WS_SIGN_PATH = "/trade-api/ws/v2"
REST_BASE = "https://api.elections.kalshi.com/trade-api/v2"


def _fetch_open_btc_markets() -> list[dict]:
    resp = httpx.get(
        f"{REST_BASE}/markets",
        params={"status": "open", "series_ticker": "KXBTC", "limit": 200},
        timeout=10.0,
    )
    resp.raise_for_status()
    return list(resp.json().get("markets", []))


def _orderbook_depth(client: httpx.Client, ticker: str) -> tuple[int, int]:
    """Fetch /markets/{ticker}/orderbook and return (yes_depth, no_depth).

    Kalshi's orderbook endpoint returns `{"orderbook_fp": {"yes_dollars":
    [["0.42", "500.00"], ...], "no_dollars": [...]}}`. Prices and sizes are
    string-dollars / string-floats. Depth = sum of sizes on that side.
    """
    r = client.get(f"{REST_BASE}/markets/{ticker}/orderbook", timeout=5.0)
    r.raise_for_status()
    ob = r.json().get("orderbook_fp") or {}
    yes_d = sum(round(float(size)) for _, size in (ob.get("yes_dollars") or []))
    no_d = sum(round(float(size)) for _, size in (ob.get("no_dollars") or []))
    return yes_d, no_d


def _pick_active_markets(n: int = 1) -> list[str]:
    """Return up to `n` open KXBTC markets with the best two-sided depth.

    Subscribing to several ATM markets simultaneously multiplies our chance of
    catching orderbook_delta and trade frames in a given time window — the BTC
    hourly book can be very quiet per-market even when aggregate activity is fine.
    """
    markets = _fetch_open_btc_markets()
    if not markets:
        raise RuntimeError("no open KXBTC markets returned from REST")

    candidates = markets[:50]
    print(f"Probing orderbooks for {len(candidates)} open KXBTC markets…")

    scored: list[tuple[int, int, int, str]] = []
    with httpx.Client() as client:
        for m in candidates:
            t = str(m["ticker"])
            yes_d, no_d = _orderbook_depth(client, t)
            scored.append((min(yes_d, no_d), yes_d, no_d, t))

    scored.sort(reverse=True)
    two_sided = [s for s in scored if s[0] > 0] or scored
    picked = two_sided[:n]
    for score, yes_d, no_d, t in picked:
        print(f"  -> {t}  yes_depth={yes_d}  no_depth={no_d}")
    return [t for _, _, _, t in picked]


def _list_markets() -> None:
    """Print open BTC markets ranked by two-sided orderbook depth."""
    markets = _fetch_open_btc_markets()
    candidates = markets[:50]
    print(f"Probing orderbooks for {len(candidates)} markets…")

    rows: list[tuple[str, int, int, str]] = []
    with httpx.Client() as client:
        for m in candidates:
            t = str(m["ticker"])
            yes_d, no_d = _orderbook_depth(client, t)
            rows.append((t, yes_d, no_d, m.get("close_time", "?")))

    print(f"\n{'ticker':<40s}  {'yes_depth':>10s}  {'no_depth':>10s}  close_time")
    for t, yd, nd, ct in sorted(rows, key=lambda r: min(r[1], r[2]), reverse=True):
        print(f"{t:<40s}  {yd:>10}  {nd:>10}  {ct}")


async def _capture(tickers: list[str], duration_sec: float, out_path: Path) -> None:
    api_key = os.environ["BOT_BTC_1HR_KALSHI_API_KEY"]
    key_path = os.environ["BOT_BTC_1HR_KALSHI_PRIVATE_KEY_PATH"]
    signer = KalshiSigner(
        api_key_id=api_key,
        private_key_pem=Path(key_path).read_bytes(),
        clock=SystemClock(),
    )
    headers = signer.headers(method="GET", path=WS_SIGN_PATH)

    print(f"Connecting: {WS_URL}")
    print(f"Markets:    {len(tickers)} ({', '.join(tickers[:3])}{'…' if len(tickers) > 3 else ''})")
    print(f"Duration:   {duration_sec}s")
    print(f"Out:        {out_path}\n")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    counts: dict[str, int] = {}

    with out_path.open("w", encoding="utf-8") as f:
        async with websockets.connect(
            WS_URL,
            additional_headers=headers,
            max_size=8 * 1024 * 1024,
            ping_interval=20,
            open_timeout=10,
        ) as ws:
            # `orderbook_delta` + `trade` are the only channels our bot consumes.
            # `ticker_v2` and `market_lifecycle` were tried during discovery but
            # Kalshi rejected them as "Unknown channel name" — remove them so the
            # capture doesn't pollute the fixture with `error` frames.
            await ws.send(json.dumps({
                "id": 1,
                "cmd": "subscribe",
                "params": {
                    "channels": ["orderbook_delta", "trade"],
                    "market_tickers": tickers,
                },
            }))

            # Write a synthetic 'error' frame by attempting an invalid
            # subscribe AFTER we've captured some real data. That gives us
            # one real 'error' fixture for the parser test suite.
            try:
                async with asyncio.timeout(duration_sec):
                    async for raw in ws:
                        if isinstance(raw, bytes):
                            frame_str = raw.decode("utf-8")
                        else:
                            frame_str = raw
                        record = {
                            "recv_ts_ns": time.time_ns(),
                            "raw": frame_str,
                        }
                        f.write(json.dumps(record) + "\n")
                        try:
                            t = json.loads(frame_str).get("type", "?")
                        except json.JSONDecodeError:
                            t = "<non-json>"
                        counts[t] = counts.get(t, 0) + 1
            except TimeoutError:
                pass

            # Tail: provoke an error frame on an invalid channel for the
            # error-handling test case. Does not affect already-captured data.
            try:
                await ws.send(json.dumps({
                    "id": 99,
                    "cmd": "subscribe",
                    "params": {"channels": ["nonsense_channel_name"]},
                }))
                async with asyncio.timeout(3):
                    async for raw in ws:
                        frame_str = raw.decode("utf-8") if isinstance(raw, bytes) else raw
                        try:
                            parsed = json.loads(frame_str)
                            t = parsed.get("type", "?")
                        except json.JSONDecodeError:
                            t = "<non-json>"
                        f.write(json.dumps({"recv_ts_ns": time.time_ns(), "raw": frame_str}) + "\n")
                        counts[t] = counts.get(t, 0) + 1
                        if t == "error":
                            break
            except TimeoutError:
                pass

    print("--- Capture summary ---")
    total = sum(counts.values())
    for t, n in sorted(counts.items(), key=lambda kv: -kv[1]):
        print(f"  {t:>24s}: {n}")
    print(f"  {'TOTAL':>24s}: {total}")
    missing = [t for t in ("orderbook_snapshot", "orderbook_delta", "trade", "error") if t not in counts]
    if missing:
        print(f"\nNOTE: did not capture frame types: {missing}")
        print("      Re-run on a busier market or for longer if the parser test suite needs those.")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ticker", default=None, help="specific ticker (default: auto-pick ATM cluster)")
    ap.add_argument("--duration", type=float, default=300.0)
    ap.add_argument("--n-markets", type=int, default=8, help="how many ATM markets to subscribe to at once")
    ap.add_argument("--list", action="store_true", help="list open BTC markets and exit")
    ap.add_argument(
        "--out",
        default=str(REPO / "tests" / "fixtures" / "kalshi_ws_frames.jsonl"),
    )
    args = ap.parse_args()

    if args.list:
        _list_markets()
        return 0

    tickers = [args.ticker] if args.ticker else _pick_active_markets(args.n_markets)
    asyncio.run(_capture(tickers, args.duration, Path(args.out)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
