"""Primary/Confirmation integrity gate (docs/DESIGN.md §7.3a).

Coinbase is the PRIMARY spot venue — its prints feed FeatureEngine directly
and drive Bollinger / ATR / regime. Kraken is the CONFIRMATION venue: its
prints never touch features. They exist solely so that on entry we can ask
"Coinbase just moved N USD in the last second — did Kraken move the same
direction, or is Coinbase lying?"

Three design choices driven by institutional-trading intuition rather than
academic divergence thresholds:

1. Veto on ACTIVE DISAGREEMENT, not absolute price divergence.
   During a cascade both venues reprice, but the basis between them can
   legitimately blow out by $80-150. What matters is whether both feeds
   agree on *direction* over the last second, not whether their absolute
   levels are within $25.

2. Silence ≠ veto.
   Kraken is lower-liquidity than Coinbase. It is normal for the feed to
   have a ~500ms gap between trade prints on a quiet tape. Treating that
   as "confirmation is stale" would refuse entries the primary was
   willing to take. Only veto when Kraken has actually printed in the
   window AND those prints contradict Coinbase.

3. Fail-closed on long silence / disconnect.
   A feed that has produced nothing for `stale_halt_sec` (default 60s) is
   broken, not quiet. In that state we have no confirmation signal at
   all and must reject entries until it recovers. This is the one
   asymmetry vs. rule 2 — silence over seconds is fine, silence over
   minutes is not.

The gate is consulted by FeedLoop before approving any candidate entry
(see `feedloop._maybe_enter`). It never affects exits — hard rule #8
flattens flow independently through the calendar / kill-switch path.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class IntegrityDecision:
    approved: bool
    reason: str = ""


_APPROVED = IntegrityDecision(approved=True)


class IntegrityTracker:
    """Per-venue 1s velocity tracker. Not thread-safe; called only from the
    feed-loop task.

    Instances hold a small deque of (ts_ns, price_usd) per venue pruned to
    the velocity window. Memory is O(window_sec * tick_rate) — at Coinbase's
    ~10 ticks/sec burst rate this is ~10 entries, which is cheap."""

    __slots__ = (
        "_confirmation",
        "_confirmation_last_ns",
        "_floor_usd",
        "_primary",
        "_primary_last_ns",
        "_stale_ns",
        "_window_ns",
    )

    def __init__(
        self,
        *,
        velocity_window_sec: float,
        active_disagreement_floor_usd: float,
        stale_halt_sec: float,
    ) -> None:
        if velocity_window_sec <= 0:
            raise ValueError("velocity_window_sec must be > 0")
        if active_disagreement_floor_usd <= 0:
            raise ValueError("active_disagreement_floor_usd must be > 0")
        if stale_halt_sec <= 0:
            raise ValueError("stale_halt_sec must be > 0")
        self._window_ns = int(velocity_window_sec * 1_000_000_000)
        self._floor_usd = active_disagreement_floor_usd
        self._stale_ns = int(stale_halt_sec * 1_000_000_000)
        self._primary: deque[tuple[int, float]] = deque()
        self._confirmation: deque[tuple[int, float]] = deque()
        self._primary_last_ns: int | None = None
        self._confirmation_last_ns: int | None = None

    def record_primary(self, ts_ns: int, price_usd: float) -> None:
        self._primary.append((ts_ns, price_usd))
        self._primary_last_ns = ts_ns
        self._trim(self._primary, ts_ns)

    def record_confirmation(self, ts_ns: int, price_usd: float) -> None:
        self._confirmation.append((ts_ns, price_usd))
        self._confirmation_last_ns = ts_ns
        self._trim(self._confirmation, ts_ns)

    @property
    def primary_last_ns(self) -> int | None:
        return self._primary_last_ns

    @property
    def confirmation_last_ns(self) -> int | None:
        return self._confirmation_last_ns

    def check_entry(self, now_ns: int) -> IntegrityDecision:
        # Rule 3: fail-closed on a broken confirmation feed.
        if self._confirmation_last_ns is None:
            return IntegrityDecision(False, "confirmation_never_connected")
        age_ns = now_ns - self._confirmation_last_ns
        if age_ns > self._stale_ns:
            return IntegrityDecision(
                False, f"confirmation_stale:age_ms={age_ns // 1_000_000}",
            )

        primary_vel = self._velocity(self._primary, now_ns)
        # No primary motion in the window → nothing to confirm; approve.
        # This avoids blocking entries when the market is flat.
        if primary_vel is None or abs(primary_vel) <= self._floor_usd:
            return _APPROVED

        confirmation_vel = self._velocity(self._confirmation, now_ns)
        # Rule 2: silence ≠ veto. Confirmation has printed recently enough
        # to clear the stale check, but nothing in the 1s velocity window —
        # trust the primary.
        if confirmation_vel is None:
            return _APPROVED

        # Rule 1: active disagreement = opposite sign AND move exceeds noise.
        if primary_vel * confirmation_vel < 0 and abs(confirmation_vel) > self._floor_usd:
            return IntegrityDecision(
                False,
                f"active_disagreement:primary={primary_vel:+.1f}_confirmation={confirmation_vel:+.1f}",
            )
        return _APPROVED

    def _trim(self, d: deque[tuple[int, float]], now_ns: int) -> None:
        cutoff = now_ns - self._window_ns
        while d and d[0][0] < cutoff:
            d.popleft()

    def _velocity(
        self, d: deque[tuple[int, float]], now_ns: int,
    ) -> float | None:
        """Returns (newest_price - oldest_price) over the window, or None if
        fewer than 2 points are available."""
        self._trim(d, now_ns)
        if len(d) < 2:
            return None
        return d[-1][1] - d[0][1]


__all__ = ["IntegrityDecision", "IntegrityTracker"]
