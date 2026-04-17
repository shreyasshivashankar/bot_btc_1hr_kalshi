"""Placeholder for the tick-replay integration harness.

Target shape (DESIGN.md §9): feed a captured Parquet session through the full
event loop with an injected clock and assert (a) deterministic PnL, (b) every
closed bet emitted a schema-valid BetOutcome, (c) no seq-gap handler was
bypassed. Filled in when market_data + execution land.
"""

from __future__ import annotations

import pytest


@pytest.mark.integration
def test_replay_harness_stub() -> None:
    pytest.skip("Replay harness not implemented yet — see DESIGN.md §9")
