"""Kalshi request signer.

Kalshi's trading-api authenticates each REST request with three headers:
    KALSHI-ACCESS-KEY:       <api_key_id>
    KALSHI-ACCESS-TIMESTAMP: <unix_ms>
    KALSHI-ACCESS-SIGNATURE: base64(RSA-PSS-SHA256(f"{ts}{METHOD}{path}"))

The signature covers the timestamp, HTTP method (uppercase) and path (URL
path only, without scheme/host/query).

Private keys are PEM-encoded RSA keys, loaded from Secret Manager via env
var `BOT_BTC_1HR_KALSHI_API_SECRET` (hard rule #4).
"""

from __future__ import annotations

import base64

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey

from bot_btc_1hr_kalshi.obs.clock import Clock


class KalshiSigner:
    __slots__ = ("_clock", "_key", "_key_id")

    def __init__(self, *, api_key_id: str, private_key_pem: bytes, clock: Clock) -> None:
        if not api_key_id:
            raise ValueError("api_key_id is required")
        key = serialization.load_pem_private_key(private_key_pem, password=None)
        if not isinstance(key, RSAPrivateKey):
            raise ValueError("Kalshi private key must be RSA")
        self._key_id = api_key_id
        self._key = key
        self._clock = clock

    @property
    def api_key_id(self) -> str:
        return self._key_id

    def headers(self, *, method: str, path: str) -> dict[str, str]:
        ts_ms = self._clock.now_ns() // 1_000_000
        to_sign = f"{ts_ms}{method.upper()}{path}".encode()
        sig = self._key.sign(
            to_sign,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256(),
        )
        return {
            "KALSHI-ACCESS-KEY": self._key_id,
            "KALSHI-ACCESS-TIMESTAMP": str(ts_ms),
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode("ascii"),
        }
