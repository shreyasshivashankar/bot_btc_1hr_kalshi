from __future__ import annotations

import httpx
import orjson
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from bot_btc_1hr_kalshi.execution.broker.base import OrderRequest
from bot_btc_1hr_kalshi.execution.broker.kalshi import KalshiBroker, KalshiBrokerError
from bot_btc_1hr_kalshi.execution.broker.kalshi_signer import KalshiSigner
from bot_btc_1hr_kalshi.obs.clock import ManualClock


def _signer(clock: ManualClock) -> KalshiSigner:
    priv = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = priv.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return KalshiSigner(api_key_id="TESTKEY", private_key_pem=pem, clock=clock)


def _client(handler: httpx.MockTransport) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=handler, base_url="https://api.test")


def _req(cid: str = "c1") -> OrderRequest:
    return OrderRequest(
        client_order_id=cid,
        market_id="KXBTCD-26APR17-60000",
        side="YES",
        action="BUY",
        limit_price_cents=40,
        contracts=5,
        order_type="maker",
    )


async def test_submit_sends_signed_request_and_parses_ack() -> None:
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["method"] = request.method
        seen["path"] = request.url.path
        seen["key_hdr"] = request.headers.get("KALSHI-ACCESS-KEY", "")
        seen["sig_hdr"] = request.headers.get("KALSHI-ACCESS-SIGNATURE", "")
        seen["ts_hdr"] = request.headers.get("KALSHI-ACCESS-TIMESTAMP", "")
        seen["body"] = orjson.loads(request.content)
        body = {
            "order": {
                "order_id": "ord-1",
                "client_order_id": "c1",
                "status": "resting",
                "count": 5,
                "remaining_count": 5,
                "fills": [],
            }
        }
        return httpx.Response(201, content=orjson.dumps(body))

    clock = ManualClock(1_700_000_000_000_000_000)
    broker = KalshiBroker(client=_client(httpx.MockTransport(handler)), signer=_signer(clock), clock=clock)
    ack = await broker.submit(_req())
    assert ack.order_id == "ord-1"
    assert ack.status == "resting"
    assert ack.remaining_contracts == 5
    assert seen["method"] == "POST"
    assert seen["path"] == "/trade-api/v2/portfolio/orders"
    assert seen["key_hdr"] == "TESTKEY"
    assert seen["sig_hdr"]  # present and non-empty
    body = seen["body"]
    assert isinstance(body, dict)
    assert body["ticker"] == "KXBTCD-26APR17-60000"
    assert body["side"] == "yes"
    assert body["action"] == "buy"
    assert body["time_in_force"] == "GTC"  # maker → GTC
    assert body["yes_price"] == 40


async def test_submit_ioc_uses_ioc_tif() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        body = orjson.loads(request.content)
        assert body["time_in_force"] == "IOC"
        return httpx.Response(200, content=orjson.dumps({
            "order": {
                "order_id": "ord-2", "client_order_id": "c1", "status": "filled",
                "count": 5, "remaining_count": 0,
                "fills": [{"yes_price": 40, "count": 5, "created_time_ms": 1700000000000}],
            }
        }))

    clock = ManualClock(1)
    broker = KalshiBroker(client=_client(httpx.MockTransport(handler)), signer=_signer(clock), clock=clock)
    req = OrderRequest(
        client_order_id="c1", market_id="M", side="YES", action="SELL",
        limit_price_cents=40, contracts=5, order_type="ioc",
    )
    ack = await broker.submit(req)
    assert ack.status == "filled"
    assert ack.filled_contracts == 5
    assert len(ack.fills) == 1


async def test_submit_400_returns_rejected_ack_with_reason() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, content=orjson.dumps({"error": "insufficient_funds"}))

    clock = ManualClock(1)
    broker = KalshiBroker(client=_client(httpx.MockTransport(handler)), signer=_signer(clock), clock=clock)
    ack = await broker.submit(_req())
    assert ack.status == "rejected"
    assert ack.reason == "insufficient_funds"
    assert ack.order_id == ""


async def test_submit_500_raises_broker_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, content=b"internal")

    clock = ManualClock(1)
    broker = KalshiBroker(client=_client(httpx.MockTransport(handler)), signer=_signer(clock), clock=clock)
    with pytest.raises(KalshiBrokerError):
        await broker.submit(_req())


async def test_submit_401_raises_auth_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, content=b"no auth")

    clock = ManualClock(1)
    broker = KalshiBroker(client=_client(httpx.MockTransport(handler)), signer=_signer(clock), clock=clock)
    with pytest.raises(KalshiBrokerError, match="auth_failed"):
        await broker.submit(_req())


async def test_cancel_returns_true_on_2xx_and_false_on_404() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.url.path.endswith("/ord-x"):
            return httpx.Response(204)
        return httpx.Response(404)

    clock = ManualClock(1)
    broker = KalshiBroker(client=_client(httpx.MockTransport(handler)), signer=_signer(clock), clock=clock)
    assert await broker.cancel("ord-x") is True
    assert await broker.cancel("ord-missing") is False
    assert await broker.cancel("") is False  # short-circuit, no network
    assert len(calls) == 2


async def test_list_open_orders_parses_response() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/trade-api/v2/portfolio/orders"
        assert request.url.query.decode() == "status=resting"
        return httpx.Response(200, content=orjson.dumps({
            "orders": [
                {"order_id": "a", "client_order_id": "c1", "status": "resting",
                 "count": 10, "remaining_count": 6},
                {"order_id": "b", "client_order_id": "c2", "status": "partially_filled",
                 "count": 5, "remaining_count": 0},
            ]
        }))

    clock = ManualClock(1)
    broker = KalshiBroker(client=_client(httpx.MockTransport(handler)), signer=_signer(clock), clock=clock)
    acks = await broker.list_open_orders()
    assert len(acks) == 2
    assert acks[0].order_id == "a"
    assert acks[0].status == "resting"
    assert acks[0].filled_contracts == 4
    assert acks[1].status == "filled"  # partial-with-zero-remaining normalizes to filled


async def test_list_positions_normalizes_signed_quantity() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=orjson.dumps({
            "market_positions": [
                {"ticker": "M1", "position": 5, "market_exposure": 200},  # 5 YES @ 40
                {"ticker": "M2", "position": -3, "market_exposure": 180}, # 3 NO @ 60
                {"ticker": "M3", "position": 0,  "market_exposure": 0},   # flat — skipped
            ]
        }))

    clock = ManualClock(1)
    broker = KalshiBroker(client=_client(httpx.MockTransport(handler)), signer=_signer(clock), clock=clock)
    positions = await broker.list_positions()
    assert len(positions) == 2
    p_yes = next(p for p in positions if p.side == "YES")
    assert p_yes.market_id == "M1" and p_yes.contracts == 5 and p_yes.avg_entry_price_cents == 40
    p_no = next(p for p in positions if p.side == "NO")
    assert p_no.market_id == "M2" and p_no.contracts == 3 and p_no.avg_entry_price_cents == 60


async def test_list_positions_raises_on_non_200() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, content=b"forbidden")

    clock = ManualClock(1)
    broker = KalshiBroker(client=_client(httpx.MockTransport(handler)), signer=_signer(clock), clock=clock)
    with pytest.raises(KalshiBrokerError):
        await broker.list_positions()
