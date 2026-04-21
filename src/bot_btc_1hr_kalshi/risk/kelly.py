"""Fractional-Kelly position sizing for binary YES contracts.

Model: we pay `price` (0..1) for a contract that pays $1 iff YES settles.
Given our estimated probability `q_true = price + edge`, the full-Kelly
fraction of bankroll to wager is (q - p) / (1 - p). We multiply by
`kelly_fraction` (<= 0.5 in this system — RISK.md §3) and clip at
`max_notional_usd`.

Slice 11 Phase 3.2 — inverted-risk clip: for entries at/above
`inverted_risk_threshold_cents`, the fractional Kelly allocation is
further multiplied by `inverted_risk_kelly_multiplier` (a value in (0,1]).
Rationale: at price >= 50¢ the per-contract dollar loss exceeds the
dollar win, and raw Kelly actually sizes UP because (1 - p) shrinks the
denominator. That is mathematically correct for uncorrelated bets on
unbounded horizons, but inappropriate for a binary-outcome, 1h-settled,
small-bankroll venue where tail drawdowns dominate psychology and
bankroll recovery. Defaults disable the clip (threshold=100, mult=1.0)
so existing tests and any caller that does not opt in keep their
previous behavior unchanged.
"""

from __future__ import annotations


def kelly_contracts(
    *,
    edge_cents: float,
    entry_price_cents: int,
    kelly_fraction: float,
    bankroll_usd: float,
    max_notional_usd: float,
    inverted_risk_threshold_cents: int = 100,
    inverted_risk_kelly_multiplier: float = 1.0,
) -> int:
    """Return the non-negative integer contract count to bet.

    Edge must be positive; otherwise we decline (size 0). Price must be
    strictly inside (0¢, 100¢) since a deterministic contract has no Kelly.
    """
    if edge_cents <= 0 or kelly_fraction <= 0 or bankroll_usd <= 0 or max_notional_usd <= 0:
        return 0
    if entry_price_cents <= 0 or entry_price_cents >= 100:
        return 0

    p_price = entry_price_cents / 100.0
    q_true = p_price + (edge_cents / 100.0)
    q_true = min(max(q_true, 0.001), 0.999)

    full_kelly_f = (q_true - p_price) / (1.0 - p_price)
    if full_kelly_f <= 0:
        return 0
    fractional_f = full_kelly_f * kelly_fraction

    if entry_price_cents >= inverted_risk_threshold_cents:
        fractional_f *= inverted_risk_kelly_multiplier

    bet_usd = min(fractional_f * bankroll_usd, max_notional_usd)
    if bet_usd <= 0:
        return 0

    contract_cost_usd = p_price
    return int(bet_usd / contract_cost_usd)
