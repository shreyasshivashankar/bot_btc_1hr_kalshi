"""Tick archive: serialize live FeedEvents to JSONL and read them back.

See `archive/format.py` for the wire schema, `archive/writer.py` for the
hour-rotated writer that the live feed loop calls, and `archive/reader.py`
for the streaming reader consumed by `research/backtest_cli.py`.
"""
