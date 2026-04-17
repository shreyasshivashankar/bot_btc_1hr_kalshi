"""FastAPI app factory for the admin/health HTTP surface."""

from __future__ import annotations

from fastapi import FastAPI

from bot_btc_1hr_kalshi.admin.auth import AdminAuth
from bot_btc_1hr_kalshi.admin.routes import build_router
from bot_btc_1hr_kalshi.app import App


def create_app(app_state: App, *, admin_token: str) -> FastAPI:
    auth = AdminAuth(admin_token)
    fastapi_app = FastAPI(
        title="bot_btc_1hr_kalshi",
        description="Admin + health endpoints for the Kalshi BTC-1h market maker.",
        version="0.1.0",
        docs_url=None,  # no swagger in prod
        redoc_url=None,
    )
    fastapi_app.include_router(build_router(app_state, auth))
    return fastapi_app
