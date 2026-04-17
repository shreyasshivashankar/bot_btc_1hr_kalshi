"""Admin HTTP endpoints: /healthz, /readyz, /admin/{halt,resume,flatten,tier1_override,status}.

Two-layer auth: IAM roles/run.invoker + bearer token (BOT_BTC_1HR_KALSHI_ADMIN_TOKEN).
"""

from bot_btc_1hr_kalshi.admin.auth import AdminAuth
from bot_btc_1hr_kalshi.admin.routes import build_router
from bot_btc_1hr_kalshi.admin.server import create_app

__all__ = ["AdminAuth", "build_router", "create_app"]
