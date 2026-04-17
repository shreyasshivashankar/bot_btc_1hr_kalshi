from __future__ import annotations

import json
import logging

from bot_btc_1hr_kalshi.obs import bind_context, clear_context, configure, get_logger


def test_configure_is_idempotent() -> None:
    configure(level="INFO")
    configure(level="DEBUG")  # second call must not raise


def test_get_logger_returns_usable_logger(capsys: object) -> None:
    configure(level="INFO")
    log = get_logger("test.logger")
    log.info("hello", foo=1)

    from _pytest.capture import CaptureFixture

    assert isinstance(capsys, CaptureFixture)
    out = capsys.readouterr().out.strip().splitlines()
    assert out, "expected at least one log line"
    parsed = json.loads(out[-1])
    assert parsed["event"] == "hello"
    assert parsed["foo"] == 1
    assert parsed["level"] == "info"


def test_bind_context_attaches_fields(capsys: object) -> None:
    configure(level="INFO")
    clear_context()
    bind_context(trace_id="abc-123")
    get_logger("test.ctx").info("bound")
    clear_context()

    from _pytest.capture import CaptureFixture

    assert isinstance(capsys, CaptureFixture)
    lines = capsys.readouterr().out.strip().splitlines()
    parsed = json.loads(lines[-1])
    assert parsed["trace_id"] == "abc-123"


def test_log_level_filter_drops_below_threshold(capsys: object) -> None:
    configure(level="WARNING")
    get_logger("test.lvl").info("dropped")
    get_logger("test.lvl").warning("kept")

    from _pytest.capture import CaptureFixture

    assert isinstance(capsys, CaptureFixture)
    lines = [line for line in capsys.readouterr().out.strip().splitlines() if line]
    parsed_events = [json.loads(line)["event"] for line in lines]
    assert "dropped" not in parsed_events
    assert "kept" in parsed_events

    # reset to default for other tests
    configure(level="INFO")
    assert logging.getLevelNamesMapping()  # sanity: stdlib still callable
