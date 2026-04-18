#!/usr/bin/env python3
"""Hard rule #5 enforcement: trading code must use the injected Clock.

Walks every src/*.py (except obs/clock.py — the single exception, since
it is the SystemClock implementation) and uses ast to detect:
  * time.time(), time.time_ns(), time.monotonic(), time.monotonic_ns()
  * datetime.now(...), datetime.utcnow()

Ruff's DTZ rule already handles the datetime variants with no tz, but
`time.time*()` / `time.monotonic*()` slip past it. AST walks avoid the
false positives we'd get from grepping docstrings.

Exit code: 1 on any violation, 0 otherwise. Also prints file:line:col
pointers so the violation is click-through in a terminal or CI log.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

BANNED_TIME = {"time", "time_ns", "monotonic", "monotonic_ns"}
BANNED_DATETIME = {"now", "utcnow"}

ALLOWLIST = {
    # The single file permitted to call time.time_ns — it IS the clock.
    Path("src/bot_btc_1hr_kalshi/obs/clock.py"),
}


def _is_banned_call(node: ast.Call) -> tuple[str, str] | None:
    """Return (module, attr) if this Call is a banned wall-clock call."""
    func = node.func
    if not isinstance(func, ast.Attribute):
        return None
    value = func.value
    if not isinstance(value, ast.Name):
        return None
    if value.id == "time" and func.attr in BANNED_TIME:
        return ("time", func.attr)
    if value.id == "datetime" and func.attr in BANNED_DATETIME:
        return ("datetime", func.attr)
    return None


def check_file(path: Path) -> list[tuple[int, int, str]]:
    """Return a list of (line, col, message) violations for `path`."""
    try:
        src = path.read_text(encoding="utf-8")
    except OSError as exc:
        print(f"clock-lint: cannot read {path}: {exc}", file=sys.stderr)
        return []
    try:
        tree = ast.parse(src, filename=str(path))
    except SyntaxError as exc:
        print(f"clock-lint: syntax error in {path}: {exc}", file=sys.stderr)
        return []

    violations: list[tuple[int, int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            banned = _is_banned_call(node)
            if banned is not None:
                mod, attr = banned
                violations.append(
                    (node.lineno, node.col_offset, f"{mod}.{attr}(): hard rule #5 forbids wall-clock in trading code — use the injected Clock"),
                )
    return violations


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    src_dir = root / "src"
    if not src_dir.is_dir():
        print(f"clock-lint: {src_dir} not found", file=sys.stderr)
        return 1

    any_violation = False
    for path in sorted(src_dir.rglob("*.py")):
        rel = path.relative_to(root)
        if rel in ALLOWLIST:
            continue
        for line, col, msg in check_file(path):
            print(f"{rel}:{line}:{col}: {msg}")
            any_violation = True

    return 1 if any_violation else 0


if __name__ == "__main__":
    raise SystemExit(main())
