"""Bearer-token auth for admin endpoints.

Token is read from env var BOT_BTC_1HR_KALSHI_ADMIN_TOKEN at construction time.
Requests to /admin/* must include `Authorization: Bearer <token>`.
Health endpoints (/healthz, /readyz) do not require auth — they are used by
Cloud Run's probe and must respond without credentials.
"""

from __future__ import annotations

import hmac

from fastapi import Header, HTTPException, status


class AdminAuth:
    __slots__ = ("_token",)

    def __init__(self, token: str | None) -> None:
        if not token:
            raise ValueError(
                "admin token is required — set BOT_BTC_1HR_KALSHI_ADMIN_TOKEN"
            )
        self._token = token

    def verify(self, authorization: str | None = Header(default=None)) -> None:
        if authorization is None or not authorization.startswith("Bearer "):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="missing bearer token",
            )
        provided = authorization.removeprefix("Bearer ").strip()
        if not hmac.compare_digest(provided, self._token):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="invalid bearer token",
            )
