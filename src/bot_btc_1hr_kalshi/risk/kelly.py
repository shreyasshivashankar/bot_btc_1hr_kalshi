"""Fractional-Kelly position sizing for binary YES contracts.

Model: we pay `price` (0..1) for a contract that pays $1 iff YES settles.
Given our estimated probability `q_true = price + edge`, the full-Kelly
fraction of bankroll to wager is (q - p) / (1 - p). We multiply by
`kelly_fraction` (<= 0.5 in this system — RISK.md §3) and clip at
`max_notional_usd`.
"""

from __future__ import annotations


def kelly_contracts(
    *,
    edge_cents: float,
    entry_price_cents: int,
    kelly_fraction: float,
    bankroll_usd: float,
    max_notional_usd: float,
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

    bet_usd = min(fractional_f * bankroll_usd, max_notional_usd)
    if bet_usd <= 0:
        return 0

    contract_cost_usd = p_price
    return int(bet_usd / contract_cost_usd)
