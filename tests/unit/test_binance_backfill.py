"""Unit tests for the Binance public-data backfill CLI.

The CLI itself is thin glue around `httpx.Client.get` + `zipfile`. We
exercise the URL-construction, on-disk-layout, idempotency, and
manifest-emission contracts so future date-range or layout drift
fails loudly rather than silently producing empty backfills.
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import io
import sys
import zipfile
from pathlib import Path

import httpx
import orjson


def _load_module() -> object:
    """Load the script as a module by file path (it lives in /scripts/,
    not on the package import path).

    The script defines a `@dataclass`, which during class construction
    looks up its own module in `sys.modules` (to detect KW_ONLY). The
    lookup happens *during* `exec_module`, so we must register the
    module under its name *before* executing the body — otherwise the
    decorator raises `AttributeError: 'NoneType' object has no
    attribute '__dict__'`.
    """
    repo = Path(__file__).resolve().parents[2]
    path = repo / "scripts" / "binance_public_data_backfill.py"
    spec = importlib.util.spec_from_file_location("binance_backfill", str(path))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["binance_backfill"] = module
    spec.loader.exec_module(module)
    return module


backfill = _load_module()  # type: ignore[assignment]


def _zip_bytes(filename: str, content: bytes) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(filename, content)
    return buf.getvalue()


def test_target_url_matches_documented_layout() -> None:
    target = backfill.BackfillTarget(  # type: ignore[attr-defined]
        base_url="https://data.binance.vision",
        symbol="BTCUSDT",
        dataset="metrics",
        day=dt.date(2026, 4, 1),
    )
    assert target.zip_url == (
        "https://data.binance.vision/data/futures/um/daily/metrics/"
        "BTCUSDT/BTCUSDT-metrics-2026-04-01.zip"
    )
    assert target.filename == "BTCUSDT-metrics-2026-04-01.csv"


def test_daterange_is_inclusive_on_both_ends() -> None:
    days = list(backfill.daterange(dt.date(2026, 4, 1), dt.date(2026, 4, 3)))  # type: ignore[attr-defined]
    assert days == [
        dt.date(2026, 4, 1),
        dt.date(2026, 4, 2),
        dt.date(2026, 4, 3),
    ]


def test_dry_run_prints_urls_without_network(
    tmp_path: Path,
    capsys: object,
) -> None:
    rc = backfill.main(  # type: ignore[attr-defined]
        [
            "--start", "2026-04-01",
            "--end", "2026-04-02",
            "--symbol", "BTCUSDT",
            "--dataset", "metrics",
            "--out", str(tmp_path),
            "--dry-run",
        ]
    )
    assert rc == 0
    out = capsys.readouterr().out  # type: ignore[attr-defined]
    assert "BTCUSDT-metrics-2026-04-01.zip" in out
    assert "BTCUSDT-metrics-2026-04-02.zip" in out


def test_fetch_extracts_csv_and_writes_manifest(
    tmp_path: Path, monkeypatch: object,
) -> None:
    """Stub `httpx.Client` with a MockTransport that returns a zipped CSV
    for any request, then run the CLI for a single day and assert the
    extracted CSV + manifest line are on disk."""
    csv_payload = b"create_time,symbol,sum_open_interest\n1234,BTCUSDT,100\n"

    def handler(request: httpx.Request) -> httpx.Response:
        assert "BTCUSDT-metrics-2026-04-01.zip" in str(request.url)
        return httpx.Response(200, content=_zip_bytes("BTCUSDT-metrics-2026-04-01.csv", csv_payload))

    transport = httpx.MockTransport(handler)
    real_client = httpx.Client

    class _StubbedClient(real_client):  # type: ignore[misc, valid-type]
        def __init__(self, *args: object, **kwargs: object) -> None:
            kwargs["transport"] = transport
            super().__init__(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(backfill.httpx, "Client", _StubbedClient)  # type: ignore[attr-defined]

    rc = backfill.main(  # type: ignore[attr-defined]
        [
            "--start", "2026-04-01",
            "--end", "2026-04-01",
            "--symbol", "BTCUSDT",
            "--dataset", "metrics",
            "--out", str(tmp_path),
        ]
    )
    assert rc == 0
    csv_path = tmp_path / "metrics" / "BTCUSDT" / "BTCUSDT-metrics-2026-04-01.csv"
    assert csv_path.read_bytes() == csv_payload

    manifest = tmp_path / "_manifest.jsonl"
    assert manifest.exists()
    record = orjson.loads(manifest.read_bytes().splitlines()[0])
    assert record["symbol"] == "BTCUSDT"
    assert record["dataset"] == "metrics"
    assert record["day"] == "2026-04-01"
    assert record["size_bytes"] == len(csv_payload)
    assert len(record["sha256"]) == 64


def test_existing_csv_is_skipped_unless_force(
    tmp_path: Path, monkeypatch: object,
) -> None:
    csv_path = tmp_path / "metrics" / "BTCUSDT" / "BTCUSDT-metrics-2026-04-01.csv"
    csv_path.parent.mkdir(parents=True)
    csv_path.write_bytes(b"existing,csv\n")
    request_count = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        request_count["n"] += 1
        return httpx.Response(200, content=_zip_bytes("file.csv", b"new,data\n"))

    transport = httpx.MockTransport(handler)
    real_client = httpx.Client

    class _StubbedClient(real_client):  # type: ignore[misc, valid-type]
        def __init__(self, *args: object, **kwargs: object) -> None:
            kwargs["transport"] = transport
            super().__init__(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(backfill.httpx, "Client", _StubbedClient)  # type: ignore[attr-defined]

    rc = backfill.main(  # type: ignore[attr-defined]
        [
            "--start", "2026-04-01",
            "--end", "2026-04-01",
            "--out", str(tmp_path),
        ]
    )
    assert rc == 0
    assert request_count["n"] == 0  # no fetch
    assert csv_path.read_bytes() == b"existing,csv\n"  # untouched

    rc = backfill.main(  # type: ignore[attr-defined]
        [
            "--start", "2026-04-01",
            "--end", "2026-04-01",
            "--out", str(tmp_path),
            "--force",
        ]
    )
    assert rc == 0
    assert request_count["n"] == 1
    assert csv_path.read_bytes() == b"new,data\n"


def test_404_is_non_fatal(
    tmp_path: Path, monkeypatch: object, capsys: object,
) -> None:
    """Binance does not publish today's archive until the next UTC day —
    a 404 mid-range must be reported but not abort the run."""
    def handler(request: httpx.Request) -> httpx.Response:
        if "2026-04-02" in str(request.url):
            return httpx.Response(404)
        return httpx.Response(200, content=_zip_bytes("f.csv", b"ok\n"))

    transport = httpx.MockTransport(handler)
    real_client = httpx.Client

    class _StubbedClient(real_client):  # type: ignore[misc, valid-type]
        def __init__(self, *args: object, **kwargs: object) -> None:
            kwargs["transport"] = transport
            super().__init__(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(backfill.httpx, "Client", _StubbedClient)  # type: ignore[attr-defined]

    rc = backfill.main(  # type: ignore[attr-defined]
        [
            "--start", "2026-04-01",
            "--end", "2026-04-02",
            "--out", str(tmp_path),
        ]
    )
    assert rc == 1  # non-zero because some failures occurred
    out = capsys.readouterr()  # type: ignore[attr-defined]
    assert "FAILED" in out.err
    # Day 1 CSV present; day 2 missing
    day1 = tmp_path / "metrics" / "BTCUSDT" / "BTCUSDT-metrics-2026-04-01.csv"
    day2 = tmp_path / "metrics" / "BTCUSDT" / "BTCUSDT-metrics-2026-04-02.csv"
    assert day1.exists()
    assert not day2.exists()


def test_invalid_dates_return_exit_code_2(capsys: object) -> None:
    rc = backfill.main(["--start", "not-a-date", "--end", "2026-04-01"])  # type: ignore[attr-defined]
    assert rc == 2
    err = capsys.readouterr().err  # type: ignore[attr-defined]
    assert "invalid date" in err


def test_end_before_start_returns_exit_code_2(capsys: object) -> None:
    rc = backfill.main(["--start", "2026-04-05", "--end", "2026-04-01"])  # type: ignore[attr-defined]
    assert rc == 2
    err = capsys.readouterr().err  # type: ignore[attr-defined]
    assert "must be on or after --start" in err


def teardown_module(_module: object) -> None:
    sys.modules.pop("binance_backfill", None)
