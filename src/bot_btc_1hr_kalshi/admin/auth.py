"""Admin-token auth for admin endpoints.

Admin endpoints require `X-Admin-Token: <token>` where the token comes from
env var BOT_BTC_1HR_KALSHI_ADMIN_TOKEN at construction time. We use a
dedicated header rather than `Authorization: Bearer ...` so the Authorization
slot stays free for Cloud Run IAM / gcloud identity tokens — scripts in
`scripts/` send both.

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

    def verify(
        self,
        x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    ) -> None:
        if x_admin_token is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="missing X-Admin-Token header",
            )
        if not hmac.compare_digest(x_admin_token, self._token):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="invalid admin token",
            )
