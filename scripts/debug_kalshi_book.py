"""Dump raw Kalshi WS frames for a given market ticker.

Usage:
  .venv/bin/python scripts/debug_kalshi_book.py [market_ticker]

Defaults to whatever market `make paper` selected last — pass the ticker on
argv to override. Connects, subscribes to orderbook_delta + trade, prints
every frame received for 15 seconds, then closes.

Purpose: if /readyz reports no_valid_books, this tells us whether Kalshi is
 (a) accepting the subscribe (we'd see a `subscribed` ack),
 (b) sending snapshot frames (we'd see an `orderbook_snapshot`),
 (c) sending any data at all (volume of frames in 15s).
If we see (a) but no (b), Kalshi is not sending the snapshot for this market
— that's the bug. If we see (c) no frames, the subscribe silently failed.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

import websockets

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

from bot_btc_1hr_kalshi.execution.broker.kalshi_signer import KalshiSigner  # noqa: E402
from bot_btc_1hr_kalshi.obs.clock import SystemClock  # noqa: E402

WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"
SIGN_PATH = "/trade-api/ws/v2"
DEFAULT_TICKER = "KXBTC-26APR1801-B66250"  # override via argv


async def _run(ticker: str) -> None:
    api_key = os.environ["BOT_BTC_1HR_KALSHI_API_KEY"]
    key_path = os.environ["BOT_BTC_1HR_KALSHI_PRIVATE_KEY_PATH"]
    signer = KalshiSigner(
        api_key_id=api_key,
        private_key_pem=Path(key_path).read_bytes(),
        clock=SystemClock(),
    )
    headers = signer.headers(method="GET", path=SIGN_PATH)

    print(f"Connecting to {WS_URL}")
    print(f"Subscribing: market_tickers=[{ticker}], channels=[orderbook_delta, trade]\n")

    async with websockets.connect(
        WS_URL,
        additional_headers=headers,
        max_size=8 * 1024 * 1024,
        ping_interval=20,
        open_timeout=10,
    ) as ws:
        await ws.send(json.dumps({
            "id": 1,
            "cmd": "subscribe",
            "params": {
                "channels": ["orderbook_delta", "trade"],
                "market_tickers": [ticker],
            },
        }))

        counts: dict[str, int] = {}
        try:
            async with asyncio.timeout(15):
                async for raw in ws:
                    msg = json.loads(raw) if isinstance(raw, str | bytes) else raw
                    t = msg.get("type", "?")
                    counts[t] = counts.get(t, 0) + 1
                    body = json.dumps(msg)
                    if len(body) > 300:
                        body = body[:300] + "… (truncated)"
                    print(f"<- [{t}] {body}")
        except TimeoutError:
            pass

    print("\n--- Summary (15s window) ---")
    for t, n in sorted(counts.items(), key=lambda kv: -kv[1]):
        print(f"  {t}: {n}")
    if not counts:
        print("  (no frames received — subscribe silently failed OR market has no activity)")


if __name__ == "__main__":
    ticker = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_TICKER
    asyncio.run(_run(ticker))
