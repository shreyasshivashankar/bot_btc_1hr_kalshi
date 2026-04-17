"""Persistence for circuit-breaker state so it survives process restarts.

Hard rule #3: a 15% single-trade loss triggers a 60-minute API lockout with
no override. An in-memory freeze is lost on crash/restart, and a cycled
container would re-open trading immediately. The store keeps the freeze
deadline on stable storage so the lockout is honored across restarts.

Default impl is JSON on a local path (suitable for a GCS FUSE mount under
Cloud Run or any writable volume). Production can swap in a GCS-backed
impl later — keep the surface area tiny so that's a drop-in change.
"""

from __future__ import annotations

import contextlib
import json
import os
import tempfile
from pathlib import Path
from typing import Protocol, runtime_checkable


@runtime_checkable
class BreakerStore(Protocol):
    def load(self) -> dict[str, int | None]: ...
    def save(self, state: dict[str, int | None]) -> None: ...


class NullBreakerStore:
    """No-op store. Used in tests and when persistence is explicitly opted out."""

    __slots__ = ()

    def load(self) -> dict[str, int | None]:
        return {}

    def save(self, state: dict[str, int | None]) -> None:
        return None


class JsonFileBreakerStore:
    """JSON file at `path`. Writes atomically (tmp + rename)."""

    __slots__ = ("_path",)

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self._path = Path(path)

    def load(self) -> dict[str, int | None]:
        if not self._path.exists():
            return {}
        try:
            raw = self._path.read_text(encoding="utf-8")
        except OSError:
            return {}
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        if not isinstance(data, dict):
            return {}
        out: dict[str, int | None] = {}
        for k, v in data.items():
            if v is None or isinstance(v, int):
                out[str(k)] = v
        return out

    def save(self, state: dict[str, int | None]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(
            dir=str(self._path.parent), prefix=f".{self._path.name}.", suffix=".tmp"
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(state, f)
            os.replace(tmp, self._path)
        except Exception:
            with contextlib.suppress(OSError):
                os.unlink(tmp)
            raise
