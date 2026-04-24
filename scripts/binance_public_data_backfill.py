"""Download Binance USD-M futures public-data daily archives.

The risk-committee sign-off path needs replay against historical real
ticks, not just live capture. Binance ships daily ZIP archives of futures
data via their `data.binance.vision` S3 bucket — open public access, no
auth, gigabytes of CSV.

This CLI downloads + unzips the requested (symbol, dataset, day) tuples
into a local directory. Downstream the analytics layer (DuckDB) reads
the CSVs natively (`read_csv_auto`); we deliberately do not Parquet-
convert in this script so the only dependency surface is `httpx`,
`orjson`, and stdlib.

Datasets used today:
  * `metrics`             — daily OI + ratios (one row per 5-minute bucket)
  * `liquidationSnapshot` — every venue liquidation print

Layout on disk:
  <output_dir>/<dataset>/<symbol>/<symbol>-<dataset>-<YYYY-MM-DD>.csv

Usage:
  .venv/bin/python scripts/binance_public_data_backfill.py \\
      --start 2026-04-01 --end 2026-04-22 \\
      --symbol BTCUSDT --dataset metrics \\
      --out data/binance_public

Re-runs are idempotent: existing CSVs are skipped unless `--force` is set.
A `_manifest.jsonl` file in the output directory captures the URL and
checksum of every fetched file so a future ingest pipeline can verify.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import io
import sys
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import httpx
import orjson

DEFAULT_BASE_URL = "https://data.binance.vision"

# data.binance.vision is fronted by CloudFront. A 30-day history download
# typically completes well under 5 minutes per dataset on a residential
# link; the per-request timeout exists to fail fast on a wedged session
# rather than to enforce any throughput SLA.
HTTP_TIMEOUT_SEC = 60.0


@dataclass(frozen=True)
class BackfillTarget:
    base_url: str
    symbol: str
    dataset: str
    day: dt.date

    @property
    def filename(self) -> str:
        return f"{self.symbol}-{self.dataset}-{self.day.isoformat()}.csv"

    @property
    def zip_url(self) -> str:
        # `futures/um/daily/<dataset>/<symbol>/<symbol>-<dataset>-YYYY-MM-DD.zip`
        return (
            f"{self.base_url}/data/futures/um/daily/{self.dataset}/"
            f"{self.symbol}/{self.symbol}-{self.dataset}-{self.day.isoformat()}.zip"
        )


def daterange(start: dt.date, end_inclusive: dt.date) -> Iterator[dt.date]:
    current = start
    while current <= end_inclusive:
        yield current
        current += dt.timedelta(days=1)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Binance USD-M futures public-data backfill")
    p.add_argument("--start", required=True, help="Start date YYYY-MM-DD (inclusive)")
    p.add_argument("--end", required=True, help="End date YYYY-MM-DD (inclusive)")
    p.add_argument("--symbol", default="BTCUSDT", help="Linear-perp symbol (default BTCUSDT)")
    p.add_argument(
        "--dataset",
        default="metrics",
        choices=("metrics", "liquidationSnapshot", "aggTrades", "klines"),
        help="Binance dataset directory under futures/um/daily/<dataset>/",
    )
    p.add_argument("--out", default="data/binance_public", help="Output directory root")
    p.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Override base URL (test only)")
    p.add_argument("--force", action="store_true", help="Re-download files that already exist")
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the URLs that would be fetched without downloading",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        start = dt.date.fromisoformat(args.start)
        end = dt.date.fromisoformat(args.end)
    except ValueError as exc:
        print(f"error: invalid date: {exc}", file=sys.stderr)
        return 2
    if end < start:
        print("error: --end must be on or after --start", file=sys.stderr)
        return 2

    out_root = Path(args.out)
    dataset_dir = out_root / args.dataset / args.symbol
    dataset_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = out_root / "_manifest.jsonl"

    targets = [
        BackfillTarget(
            base_url=args.base_url, symbol=args.symbol, dataset=args.dataset, day=day
        )
        for day in daterange(start, end)
    ]

    if args.dry_run:
        for t in targets:
            print(t.zip_url)
        return 0

    fetched = 0
    skipped = 0
    failed = 0
    with httpx.Client(timeout=HTTP_TIMEOUT_SEC) as client:
        for target in targets:
            csv_path = dataset_dir / target.filename
            if csv_path.exists() and not args.force:
                skipped += 1
                continue
            try:
                _fetch_and_extract(client, target, csv_path)
                _append_manifest(manifest_path, target, csv_path)
                fetched += 1
                print(f"fetched {target.zip_url}")
            except FetchError as exc:
                failed += 1
                print(f"FAILED {target.zip_url}: {exc}", file=sys.stderr)

    print(f"done: fetched={fetched} skipped={skipped} failed={failed}")
    return 0 if failed == 0 else 1


class FetchError(RuntimeError):
    pass


def _fetch_and_extract(
    client: httpx.Client, target: BackfillTarget, csv_path: Path
) -> None:
    try:
        resp = client.get(target.zip_url)
    except httpx.HTTPError as exc:
        raise FetchError(f"http_error:{exc}") from exc
    if resp.status_code == 404:
        # Binance does not publish today's archive until the next UTC day.
        # 404 is non-fatal: skip the date and move on.
        raise FetchError("not_yet_published (404)")
    if resp.status_code != 200:
        raise FetchError(f"http_status:{resp.status_code}")

    try:
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            inner_names = zf.namelist()
            if not inner_names:
                raise FetchError("empty_zip")
            # Each daily archive holds exactly one CSV with a known name.
            with zf.open(inner_names[0]) as src, csv_path.open("wb") as dst:
                dst.write(src.read())
    except zipfile.BadZipFile as exc:
        raise FetchError(f"bad_zip:{exc}") from exc


def _append_manifest(
    manifest_path: Path, target: BackfillTarget, csv_path: Path
) -> None:
    sha = hashlib.sha256(csv_path.read_bytes()).hexdigest()
    record = {
        "symbol": target.symbol,
        "dataset": target.dataset,
        "day": target.day.isoformat(),
        "url": target.zip_url,
        "csv_path": str(csv_path),
        "sha256": sha,
        "size_bytes": csv_path.stat().st_size,
    }
    with manifest_path.open("ab") as f:
        f.write(orjson.dumps(record))
        f.write(b"\n")


if __name__ == "__main__":
    raise SystemExit(main())
