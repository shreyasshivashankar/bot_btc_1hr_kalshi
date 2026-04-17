"""Admin HTTP endpoints: /healthz, /readyz, /admin/{halt,resume,flatten,tier1_override,status}.

Two-layer auth: IAM roles/run.invoker + bearer token (BOT_BTC_1HR_KALSHI_ADMIN_TOKEN).
"""
