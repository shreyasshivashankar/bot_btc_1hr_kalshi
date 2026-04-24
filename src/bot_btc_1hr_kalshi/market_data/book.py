"""L2 order book with sequence-gap detection.

Hard rule #9: on WS seq gap, the book is marked INVALID until a REST snapshot
rebuilds it. Any feature derived from `L2Book` (spread, depth, OFI) must check
`book.valid` before use — the signal layer treats INVALID as a pass.

The book is stored in YES-space (YES bids + YES asks). NO-side bids and asks
are derived by parity: NO_bid at price p ⇔ YES_ask at (100 - p). Use
`best_bid_for(side)` / `best_ask_for(side)` when you need side-specific prices.

Top-of-book is cached on every `apply()` so `best_bid` / `best_ask` are O(1).
The PositionMonitor and the cross-strike snapshot builder both read these on
every WS tick, and the previous `max(self._bids)` / `min(self._asks)` paths
were O(N) per call. N is small here (~20-100 levels) so the wall-clock cost
was tiny, but the property reads also showed up multiple times per tick path,
so caching trims redundant scans without changing observable behavior. Cache
is rebuilt from scratch on snapshot frames; on deltas it updates in place
unless the top level is depleted, in which case we rescan to find the next
best.
"""

from __future__ import annotations

from bot_btc_1hr_kalshi.market_data.types import BookLevel, BookUpdate
from bot_btc_1hr_kalshi.obs.schemas import Side


class L2Book:
    __slots__ = (
        "_asks",
        "_best_ask_price",
        "_best_ask_size",
        "_best_bid_price",
        "_best_bid_size",
        "_bids",
        "_invalidation_reason",
        "_last_seq",
        "_market_id",
        "_valid",
    )

    def __init__(self, market_id: str) -> None:
        self._market_id = market_id
        self._bids: dict[int, int] = {}
        self._asks: dict[int, int] = {}
        self._best_bid_price: int | None = None
        self._best_bid_size: int = 0
        self._best_ask_price: int | None = None
        self._best_ask_size: int = 0
        self._last_seq: int | None = None
        self._valid = False
        self._invalidation_reason: str | None = "awaiting_first_snapshot"

    @property
    def market_id(self) -> str:
        return self._market_id

    @property
    def valid(self) -> bool:
        return self._valid

    @property
    def invalidation_reason(self) -> str | None:
        return self._invalidation_reason

    @property
    def last_seq(self) -> int | None:
        return self._last_seq

    def invalidate(self, reason: str) -> None:
        """Mark the book INVALID (hard rule #9). Callers downstream of a WS
        reconnect, a snapshot-rebuild trigger, or any other event that breaks
        our view of resting liquidity must call this before the next feature
        evaluation. Book remains INVALID until the next snapshot replays the
        full state."""
        self._valid = False
        self._invalidation_reason = reason
        self._last_seq = None
        self._bids.clear()
        self._asks.clear()
        self._best_bid_price = None
        self._best_bid_size = 0
        self._best_ask_price = None
        self._best_ask_size = 0

    def apply(self, update: BookUpdate) -> None:
        """Apply a book update. Detects seq gaps and invalidates on mismatch."""
        if update.market_id != self._market_id:
            raise ValueError(f"market_id mismatch: {update.market_id} vs {self._market_id}")

        if (
            self._last_seq is not None
            and not update.is_snapshot
            and update.seq != self._last_seq + 1
        ):
            self._valid = False
            self._invalidation_reason = f"seq_gap:{self._last_seq}->{update.seq}"
            self._last_seq = update.seq
            return

        if update.is_snapshot:
            self._bids = {lvl.price_cents: lvl.size for lvl in update.bids if lvl.size > 0}
            self._asks = {lvl.price_cents: lvl.size for lvl in update.asks if lvl.size > 0}
            self._valid = True
            self._invalidation_reason = None
            self._rebuild_top_of_book()
        else:
            # Deltas are signed quantity changes; accumulate onto the existing
            # level. A running total of ≤0 means the level is fully cleared.
            # (See BookUpdate docstring — masking negatives would erase every
            # resting quote on partial fills.)
            for lvl in update.bids:
                new_size = self._bids.get(lvl.price_cents, 0) + lvl.size
                if new_size <= 0:
                    self._bids.pop(lvl.price_cents, None)
                    # Cleared the cached best — rescan for the next one.
                    if lvl.price_cents == self._best_bid_price:
                        self._rescan_best_bid()
                else:
                    self._bids[lvl.price_cents] = new_size
                    self._maybe_promote_bid(lvl.price_cents, new_size)
            for lvl in update.asks:
                new_size = self._asks.get(lvl.price_cents, 0) + lvl.size
                if new_size <= 0:
                    self._asks.pop(lvl.price_cents, None)
                    if lvl.price_cents == self._best_ask_price:
                        self._rescan_best_ask()
                else:
                    self._asks[lvl.price_cents] = new_size
                    self._maybe_promote_ask(lvl.price_cents, new_size)

        self._last_seq = update.seq

    def _rebuild_top_of_book(self) -> None:
        """Recompute cached top-of-book from scratch. Used after a snapshot
        frame, where every prior level is replaced wholesale."""
        if self._bids:
            p = max(self._bids)
            self._best_bid_price = p
            self._best_bid_size = self._bids[p]
        else:
            self._best_bid_price = None
            self._best_bid_size = 0
        if self._asks:
            p = min(self._asks)
            self._best_ask_price = p
            self._best_ask_size = self._asks[p]
        else:
            self._best_ask_price = None
            self._best_ask_size = 0

    def _rescan_best_bid(self) -> None:
        if self._bids:
            p = max(self._bids)
            self._best_bid_price = p
            self._best_bid_size = self._bids[p]
        else:
            self._best_bid_price = None
            self._best_bid_size = 0

    def _rescan_best_ask(self) -> None:
        if self._asks:
            p = min(self._asks)
            self._best_ask_price = p
            self._best_ask_size = self._asks[p]
        else:
            self._best_ask_price = None
            self._best_ask_size = 0

    def _maybe_promote_bid(self, price: int, size: int) -> None:
        # Three cases keep the cache consistent without a full scan:
        #   1) no current best       → this level is the best
        #   2) better than cur best  → replace
        #   3) same price as cur best → size changed, refresh cache size
        if self._best_bid_price is None or price > self._best_bid_price:
            self._best_bid_price = price
            self._best_bid_size = size
        elif price == self._best_bid_price:
            self._best_bid_size = size

    def _maybe_promote_ask(self, price: int, size: int) -> None:
        if self._best_ask_price is None or price < self._best_ask_price:
            self._best_ask_price = price
            self._best_ask_size = size
        elif price == self._best_ask_price:
            self._best_ask_size = size

    @property
    def best_bid(self) -> BookLevel | None:
        if self._best_bid_price is None:
            return None
        return BookLevel(price_cents=self._best_bid_price, size=self._best_bid_size)

    @property
    def best_ask(self) -> BookLevel | None:
        if self._best_ask_price is None:
            return None
        return BookLevel(price_cents=self._best_ask_price, size=self._best_ask_size)

    @property
    def mid_cents(self) -> float | None:
        if self._best_bid_price is None or self._best_ask_price is None:
            return None
        return (self._best_bid_price + self._best_ask_price) / 2.0

    @property
    def spread_cents(self) -> int | None:
        if self._best_bid_price is None or self._best_ask_price is None:
            return None
        return self._best_ask_price - self._best_bid_price

    def best_bid_for(self, side: Side) -> BookLevel | None:
        """Best bid on the given side (YES uses stored bids; NO flips from YES asks)."""
        if side == "YES":
            return self.best_bid
        ask = self.best_ask
        if ask is None:
            return None
        return BookLevel(price_cents=100 - ask.price_cents, size=ask.size)

    def best_ask_for(self, side: Side) -> BookLevel | None:
        """Best ask on the given side (YES uses stored asks; NO flips from YES bids)."""
        if side == "YES":
            return self.best_ask
        bid = self.best_bid
        if bid is None:
            return None
        return BookLevel(price_cents=100 - bid.price_cents, size=bid.size)

    def book_depth(self, *, levels: int = 5) -> float:
        """Sum of sizes across the top `levels` price tiers on each side."""
        if not self._bids and not self._asks:
            return 0.0
        top_bids = sorted(self._bids.items(), reverse=True)[:levels]
        top_asks = sorted(self._asks.items())[:levels]
        return float(sum(s for _, s in top_bids) + sum(s for _, s in top_asks))

    def snapshot_levels(self) -> tuple[list[BookLevel], list[BookLevel]]:
        """Bids (desc) / asks (asc). Useful for debugging and snapshots."""
        bids = [BookLevel(p, s) for p, s in sorted(self._bids.items(), reverse=True)]
        asks = [BookLevel(p, s) for p, s in sorted(self._asks.items())]
        return bids, asks

    def snapshot_levels_for(self, side: Side) -> tuple[list[BookLevel], list[BookLevel]]:
        """Side-specific (bids, asks). For NO, flips each YES level via 100-p."""
        bids, asks = self.snapshot_levels()
        if side == "YES":
            return bids, asks
        # NO-space: NO bids = YES asks reflected (desc by NO-price), NO asks = YES bids reflected.
        no_bids = sorted(
            (BookLevel(price_cents=100 - lvl.price_cents, size=lvl.size) for lvl in asks),
            key=lambda x: -x.price_cents,
        )
        no_asks = sorted(
            (BookLevel(price_cents=100 - lvl.price_cents, size=lvl.size) for lvl in bids),
            key=lambda x: x.price_cents,
        )
        return no_bids, no_asks
