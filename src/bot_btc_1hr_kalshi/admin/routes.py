"""Admin HTTP routes. Thin layer — all side effects go through `App`."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status

from bot_btc_1hr_kalshi.admin.auth import AdminAuth
from bot_btc_1hr_kalshi.app import App


def build_router(app: App, auth: AdminAuth) -> APIRouter:
    router = APIRouter()

    @router.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @router.get("/readyz")
    async def readyz() -> dict[str, Any]:
        ok, reason = app.ready()
        if not ok:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={"ready": False, "reason": reason},
            )
        return {"ready": True, "reason": reason}

    @router.get("/admin/status", dependencies=[Depends(auth.verify)])
    async def admin_status() -> dict[str, Any]:
        return app.status()

    @router.post("/admin/halt", dependencies=[Depends(auth.verify)])
    async def admin_halt() -> dict[str, Any]:
        app.halt()
        return {"trading_halted": app.trading_halted}

    @router.post("/admin/resume", dependencies=[Depends(auth.verify)])
    async def admin_resume() -> dict[str, Any]:
        try:
            app.resume()
        except RuntimeError as e:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=str(e),
            ) from e
        return {"trading_halted": app.trading_halted}

    @router.post("/admin/flatten", dependencies=[Depends(auth.verify)])
    async def admin_flatten() -> dict[str, Any]:
        outcomes = await app.flatten()
        return {
            "flattened_count": len(outcomes),
            "outcomes": [o.model_dump() for o in outcomes],
        }

    @router.post("/admin/tier1_override", dependencies=[Depends(auth.verify)])
    async def admin_tier1_override() -> dict[str, Any]:
        outcomes = await app.tier1_override()
        return {
            "tier1_override_active": True,
            "flattened_count": len(outcomes),
        }

    return router
