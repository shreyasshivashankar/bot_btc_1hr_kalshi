"""L2 order book with sequence-gap detection.

Hard rule #9: on WS seq gap, the book is marked INVALID until a REST snapshot
rebuilds it. Any feature derived from `L2Book` (spread, depth, OFI) must check
`book.valid` before use — the signal layer treats INVALID as a pass.
"""

from __future__ import annotations

from bot_btc_1hr_kalshi.market_data.types import BookLevel, BookUpdate


class L2Book:
    __slots__ = ("_asks", "_bids", "_invalidation_reason", "_last_seq", "_market_id", "_valid")

    def __init__(self, market_id: str) -> None:
        self._market_id = market_id
        self._bids: dict[int, int] = {}
        self._asks: dict[int, int] = {}
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
        else:
            for lvl in update.bids:
                if lvl.size == 0:
                    self._bids.pop(lvl.price_cents, None)
                else:
                    self._bids[lvl.price_cents] = lvl.size
            for lvl in update.asks:
                if lvl.size == 0:
                    self._asks.pop(lvl.price_cents, None)
                else:
                    self._asks[lvl.price_cents] = lvl.size

        self._last_seq = update.seq

    @property
    def best_bid(self) -> BookLevel | None:
        if not self._bids:
            return None
        p = max(self._bids)
        return BookLevel(price_cents=p, size=self._bids[p])

    @property
    def best_ask(self) -> BookLevel | None:
        if not self._asks:
            return None
        p = min(self._asks)
        return BookLevel(price_cents=p, size=self._asks[p])

    @property
    def mid_cents(self) -> float | None:
        bb, ba = self.best_bid, self.best_ask
        if bb is None or ba is None:
            return None
        return (bb.price_cents + ba.price_cents) / 2.0

    @property
    def spread_cents(self) -> int | None:
        bb, ba = self.best_bid, self.best_ask
        if bb is None or ba is None:
            return None
        return ba.price_cents - bb.price_cents

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
