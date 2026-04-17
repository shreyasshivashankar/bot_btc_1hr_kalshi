"""Market data: Kalshi WS + Coinbase/Binance spot feeds, L2 book, RTI, seq-gap detector.

On WS sequence gap, book-derived features are marked INVALID until a REST snapshot
rebuilds the book (hard rule #9). Staleness >2s on the primary feed halts trading.
"""
