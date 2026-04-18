"""Self-test for scripts/check_clock_usage.py (hard rule #5 backstop).

Verifies that:
  * The check catches time.time() / time.monotonic_ns() / datetime.now().
  * The allowlist (obs/clock.py) is respected.
  * Real src/ is clean — any future regression surfaces here as well as
    via `make clock-lint`, so the CI test run is the stricter gate.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
CHECK_SCRIPT = REPO_ROOT / "scripts" / "check_clock_usage.py"


def _load_module():  # type: ignore[no-untyped-def]
    spec = importlib.util.spec_from_file_location("check_clock_usage", CHECK_SCRIPT)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_detects_time_time(tmp_path: Path) -> None:
    mod = _load_module()
    f = tmp_path / "bad.py"
    f.write_text("import time\ndef f():\n    return time.time()\n")
    violations = mod.check_file(f)
    assert len(violations) == 1
    _line, _col, msg = violations[0]
    assert "time.time()" in msg


def test_detects_time_ns_and_monotonic(tmp_path: Path) -> None:
    mod = _load_module()
    f = tmp_path / "bad.py"
    f.write_text(
        "import time\n"
        "def f():\n"
        "    a = time.time_ns()\n"
        "    b = time.monotonic()\n"
        "    c = time.monotonic_ns()\n",
    )
    violations = mod.check_file(f)
    assert len(violations) == 3
    attrs = {msg for _, _, msg in violations}
    assert any("time.time_ns" in m for m in attrs)
    assert any("time.monotonic()" in m for m in attrs)
    assert any("time.monotonic_ns" in m for m in attrs)


def test_detects_datetime_now_and_utcnow(tmp_path: Path) -> None:
    mod = _load_module()
    f = tmp_path / "bad.py"
    f.write_text(
        "import datetime\n"
        "def f():\n"
        "    a = datetime.now()\n"
        "    b = datetime.utcnow()\n",
    )
    violations = mod.check_file(f)
    assert len(violations) == 2


def test_ignores_prose_in_docstrings(tmp_path: Path) -> None:
    """A docstring that *mentions* the banned names must not trigger."""
    mod = _load_module()
    f = tmp_path / "ok.py"
    f.write_text(
        '"""Docstring mentions time.time() and datetime.now() in prose.\n'
        'We wrap time.time_ns() in SystemClock.\n'
        '"""\n'
        "def f():\n"
        "    return 1\n",
    )
    assert mod.check_file(f) == []


def test_does_not_trip_on_non_call_attribute(tmp_path: Path) -> None:
    """`time.time` (no parens — a function reference, not a call) is allowed;
    the concern is invocation, not import or aliasing."""
    mod = _load_module()
    f = tmp_path / "ok.py"
    f.write_text("import time\nfn = time.time  # reference is fine\n")
    assert mod.check_file(f) == []


def test_real_src_tree_is_clean() -> None:
    """The live codebase must pass clock-lint. If this fails, either the
    allowlist needs widening (rare) or real trading code just acquired a
    forbidden wall-clock call (fix it)."""
    mod = _load_module()
    src_dir = REPO_ROOT / "src"
    allowlist = mod.ALLOWLIST
    violations: list[tuple[Path, int, int, str]] = []
    for p in sorted(src_dir.rglob("*.py")):
        rel = p.relative_to(REPO_ROOT)
        if rel in allowlist:
            continue
        for line, col, msg in mod.check_file(p):
            violations.append((rel, line, col, msg))
    assert violations == [], f"clock-lint violations: {violations}"
