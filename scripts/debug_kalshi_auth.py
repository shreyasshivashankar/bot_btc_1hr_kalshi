"""Isolate whether Kalshi 401s are a signer bug or WS-transport-specific.

Hits a SIGNED REST endpoint (`GET /trade-api/v2/portfolio/balance`) using the
exact same `KalshiSigner` the WS factory wires up. Outcomes:

  * 200 → signer works end-to-end; the WS 401 is WS-specific (header transmission,
    per-account WS entitlement, etc). Investigate `websockets.connect` headers.
  * 401 → signer is broken. Compare the three signed headers against Kalshi's
    published spec (docs.kalshi.com/api/authentication).

Runs standalone; reads the same env vars as `make paper`:
  BOT_BTC_1HR_KALSHI_API_KEY
  BOT_BTC_1HR_KALSHI_PRIVATE_KEY_PATH   (path to PEM file on disk)
  BOT_BTC_1HR_KALSHI_REST_URL           (defaults to prod elections host)
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import httpx
import websockets

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

from bot_btc_1hr_kalshi.execution.broker.kalshi_signer import KalshiSigner  # noqa: E402
from bot_btc_1hr_kalshi.obs.clock import SystemClock  # noqa: E402

HOSTS: list[tuple[str, str]] = [
    ("elections-prod", "https://api.elections.kalshi.com/trade-api/v2"),
    ("legacy-prod",    "https://trading-api.kalshi.com/trade-api/v2"),
    ("demo",           "https://demo-api.kalshi.co/trade-api/v2"),
]
SIGNED_PATH = "/portfolio/balance"  # auth-required per Kalshi API


def _probe(label: str, rest_base: str, signer: KalshiSigner) -> int:
    """Hit SIGNED_PATH against `rest_base`; return HTTP status (or -1 on transport err)."""
    base_path = urlparse(rest_base).path.rstrip("/")
    signed_path = base_path + SIGNED_PATH
    url = urlunparse(urlparse(rest_base)._replace(path=signed_path))
    headers = signer.headers(method="GET", path=signed_path)
    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(url, headers=headers)
    except httpx.HTTPError as e:
        print(f"[{label}] transport error: {e}")
        return -1
    body = resp.text
    if len(body) > 300:
        body = body[:300] + "… (truncated)"
    print(f"[{label}] {url}")
    print(f"[{label}]   HTTP {resp.status_code}  {body}")
    return resp.status_code


WS_HOSTS: dict[str, str] = {
    "elections-prod": "wss://api.elections.kalshi.com/trade-api/ws/v2",
    "demo":           "wss://demo-api.kalshi.co/trade-api/ws/v2",
}
WS_SIGN_PATH = "/trade-api/ws/v2"


async def _probe_ws(signer: KalshiSigner, host_label: str) -> None:
    """Attempt the authenticated WS handshake on the host where REST succeeded."""
    ws_url = WS_HOSTS.get(host_label)
    if ws_url is None:
        print(f"[ws] no WS host mapping for {host_label}")
        return

    headers = signer.headers(method="GET", path=WS_SIGN_PATH)
    print(f"[ws] Connecting {ws_url}")
    print(f"[ws]   signed path: {WS_SIGN_PATH}")
    print(f"[ws]   headers.keys: {sorted(headers)}")
    print(f"[ws]   timestamp:   {headers['KALSHI-ACCESS-TIMESTAMP']}")
    print(f"[ws]   signature:   {headers['KALSHI-ACCESS-SIGNATURE'][:32]}…")

    try:
        async with websockets.connect(
            ws_url,
            additional_headers=headers,
            max_size=8 * 1024 * 1024,
            ping_interval=20,
            open_timeout=10,
        ) as ws:
            print("[ws] CONNECTED — subscribing to ticker_v2 …")
            import json
            await ws.send(json.dumps({
                "id": 1,
                "cmd": "subscribe",
                "params": {"channels": ["ticker_v2"]},
            }))
            try:
                resp = await asyncio.wait_for(ws.recv(), timeout=5.0)
                print(f"[ws] first frame: {resp[:300] if isinstance(resp, str) else resp[:300]!r}")
            except asyncio.TimeoutError:
                print("[ws] connected but no frame in 5s (still a success — auth passed)")
    except websockets.InvalidStatus as e:
        resp = e.response
        print(f"[ws] HTTP {resp.status_code} rejection")
        print(f"[ws] response headers: {dict(resp.headers)}")
        body = bytes(resp.body).decode("utf-8", errors="replace") if resp.body else ""
        print(f"[ws] response body: {body[:500]}")
    except Exception as e:
        print(f"[ws] {type(e).__name__}: {e}")


def main() -> int:
    api_key = os.environ.get("BOT_BTC_1HR_KALSHI_API_KEY")
    key_path = os.environ.get("BOT_BTC_1HR_KALSHI_PRIVATE_KEY_PATH")
    if not api_key or not key_path:
        print("ERROR: set BOT_BTC_1HR_KALSHI_API_KEY and BOT_BTC_1HR_KALSHI_PRIVATE_KEY_PATH")
        return 2
    try:
        pem_bytes = Path(key_path).read_bytes()
    except OSError as exc:
        print(f"ERROR: could not read PEM at {key_path}: {exc}")
        return 2

    signer = KalshiSigner(
        api_key_id=api_key,
        private_key_pem=pem_bytes,
        clock=SystemClock(),
    )

    print(f"API key id: {api_key}")
    print()

    results: dict[str, int] = {}
    for label, base in HOSTS:
        results[label] = _probe(label, base, signer)
        print()

    okay = [l for l, s in results.items() if s == 200]
    if okay:
        print(f"SIGNER OK on: {okay}. Now probing WS handshake on same host…\n")
        asyncio.run(_probe_ws(signer, okay[0]))
        return 0

    not_found = [l for l, s in results.items() if s == 401]
    if not_found and all(results[l] in (401, -1) for l in results):
        print(
            "All hosts rejected the key with 401. 'NOT_FOUND' detail means Kalshi's "
            "server could not locate this API key id at the host it was registered "
            "against. Verify the key is active in the Kalshi dashboard, and that you "
            "copied it from the portal that matches the host (prod vs demo).",
        )
        return 1

    print(f"Mixed results: {results}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
