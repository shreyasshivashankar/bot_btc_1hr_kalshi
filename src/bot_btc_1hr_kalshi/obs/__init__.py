"""Observability: structured logs, bet-outcome telemetry, admin audit, metrics.

Every closed bet emits a pydantic-validated BetOutcome to
`bot_btc_1hr_kalshi.bet_outcomes` (hard rule #6). Schema drift breaks tuning queries.
"""
