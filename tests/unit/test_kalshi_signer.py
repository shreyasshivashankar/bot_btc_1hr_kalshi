from __future__ import annotations

import base64

import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from bot_btc_1hr_kalshi.execution.broker.kalshi_signer import KalshiSigner
from bot_btc_1hr_kalshi.obs.clock import ManualClock


def _gen_pem() -> tuple[bytes, rsa.RSAPublicKey]:
    priv = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = priv.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return pem, priv.public_key()


def test_headers_contain_access_key_and_timestamp_ms() -> None:
    pem, _ = _gen_pem()
    clock = ManualClock(2_000_000_000_000_000)  # 2e15 ns = 2_000_000_000 ms
    signer = KalshiSigner(api_key_id="KEY123", private_key_pem=pem, clock=clock)
    h = signer.headers(method="POST", path="/trade-api/v2/portfolio/orders")
    assert h["KALSHI-ACCESS-KEY"] == "KEY123"
    assert h["KALSHI-ACCESS-TIMESTAMP"] == "2000000000"
    assert h["KALSHI-ACCESS-SIGNATURE"]
    base64.b64decode(h["KALSHI-ACCESS-SIGNATURE"])  # must be valid base64


def test_signature_verifies_with_public_key() -> None:
    pem, pub = _gen_pem()
    clock = ManualClock(1_000_000_000_000_000)  # ms=1_000_000_000
    signer = KalshiSigner(api_key_id="K", private_key_pem=pem, clock=clock)

    path = "/trade-api/v2/portfolio/orders"
    h = signer.headers(method="POST", path=path)
    ts = h["KALSHI-ACCESS-TIMESTAMP"]
    to_verify = f"{ts}POST{path}".encode()
    sig = base64.b64decode(h["KALSHI-ACCESS-SIGNATURE"])
    # Raises InvalidSignature on mismatch — passes silently on success.
    pub.verify(
        sig,
        to_verify,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256(),
    )


def test_rejects_non_rsa_key() -> None:
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    priv = Ed25519PrivateKey.generate()
    pem = priv.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    with pytest.raises(ValueError, match="must be RSA"):
        KalshiSigner(api_key_id="K", private_key_pem=pem, clock=ManualClock(0))


def test_empty_key_id_rejected() -> None:
    pem, _ = _gen_pem()
    with pytest.raises(ValueError, match="api_key_id"):
        KalshiSigner(api_key_id="", private_key_pem=pem, clock=ManualClock(0))


def test_method_is_uppercased_in_signature_string() -> None:
    pem, pub = _gen_pem()
    clock = ManualClock(1_000_000_000_000_000)
    signer = KalshiSigner(api_key_id="K", private_key_pem=pem, clock=clock)
    h = signer.headers(method="post", path="/x")
    ts = h["KALSHI-ACCESS-TIMESTAMP"]
    # Must verify against UPPERCASE method in to_sign
    to_verify = f"{ts}POST/x".encode()
    pub.verify(
        base64.b64decode(h["KALSHI-ACCESS-SIGNATURE"]),
        to_verify,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256(),
    )
