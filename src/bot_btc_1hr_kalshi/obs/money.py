"""NewType aliases for money and size quantities.

NewType is a runtime no-op — each alias is just `int` at runtime. Mypy
treats it as distinct so that mixing cents with contracts with micros
yields a type error, which catches a whole class of unit-confusion bugs
that would otherwise only surface in PnL reconciliation.

Semantics:
  * `Cents`      — integer cents in price space. Kalshi quotes in 1¢ ticks
                   (0..100), spot USD prices rendered at cent resolution.
  * `Micros`     — integer micro-dollars. Used by `Portfolio` for bankroll
                   and realized PnL accumulation to kill float drift
                   (commit c5a7023). Spot ticks store `price_micros` so the
                   feature engine never materializes a float from the wire.
  * `Contracts`  — integer contract counts. Kalshi contracts are atomic;
                   a fractional contract is always a bug.

Existing dataclass fields typed as plain `int` are still compatible —
NewType accepts any int at construction. Future migration will tighten
field annotations to use these aliases directly; until then, use them
explicitly in any *new* arithmetic path (hot path book walks, feature
engines, sizing logic) so the unit stays attached to the variable.

Helpers `usd_to_micros` / `micros_to_usd` quantize at the edge: every
conversion from float USD to integer micros rounds once, and that
rounded int is authoritative from then on. Never re-materialize a float
from the micros side only to quantize it again — that defeats the
purpose of the integer-accumulator representation.
"""

from __future__ import annotations

from typing import NewType

Cents = NewType("Cents", int)
Micros = NewType("Micros", int)
Contracts = NewType("Contracts", int)

MICROS_PER_USD = 1_000_000
MICROS_PER_CENT = 10_000


def usd_to_micros(usd: float) -> Micros:
    """Quantize a float USD amount to integer micros. Rounds half-away-
    from-zero via `round()` on positive+negative inputs identically (we
    use Python's banker's `round` here deliberately — accumulator error
    is unbiased over many credits/debits)."""
    return Micros(round(usd * MICROS_PER_USD))


def micros_to_usd(m: Micros) -> float:
    return m / MICROS_PER_USD


def cents_to_micros(c: Cents) -> Micros:
    """Exact conversion: 1¢ = 10_000 micros. No rounding — cents are
    integer and the factor is an integer."""
    return Micros(c * MICROS_PER_CENT)
