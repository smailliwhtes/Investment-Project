from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class FixtureSpec:
    symbols: list[str]
    rows: int
    start_date: str


def _symbol_offset(symbol: str) -> float:
    return sum(ord(ch) for ch in symbol) % 17 * 0.15


def build_fixture_frame(symbol: str, *, rows: int, start_date: str) -> pd.DataFrame:
    dates = pd.bdate_range(start=start_date, periods=rows)
    base = 50.0 + _symbol_offset(symbol)
    idx = pd.Series(range(rows), dtype="float64")
    trend = base + idx * 0.08
    season = (idx % 10) * 0.03
    open_price = trend + season
    close_price = open_price + ((idx % 7) - 3) * 0.02
    high_price = pd.concat([open_price, close_price], axis=1).max(axis=1) + 0.15
    low_price = pd.concat([open_price, close_price], axis=1).min(axis=1) - 0.15
    volume = 1_000_000 + (idx * 250).astype("int64") + int(_symbol_offset(symbol) * 1000)

    frame = pd.DataFrame(
        {
            "Date": dates.date,
            "Open": open_price.round(2),
            "High": high_price.round(2),
            "Low": low_price.round(2),
            "Close": close_price.round(2),
            "Volume": volume,
        }
    )
    return frame


def write_fixture_ohlcv(
    output_dir: Path,
    *,
    spec: FixtureSpec,
) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for symbol in spec.symbols:
        frame = build_fixture_frame(symbol, rows=spec.rows, start_date=spec.start_date)
        path = output_dir / f"{symbol}.csv"
        frame.to_csv(path, index=False)
        written.append(path)
    return written


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate deterministic OHLCV fixtures.")
    parser.add_argument(
        "--outdir",
        type=Path,
        default=Path("tests/fixtures/ohlcv"),
        help="Output directory for fixture CSVs.",
    )
    parser.add_argument("--rows", type=int, default=300, help="Number of trading days per symbol.")
    parser.add_argument(
        "--start-date",
        default="2022-01-03",
        help="Start date (YYYY-MM-DD) for business-day sequence.",
    )
    parser.add_argument(
        "--symbols",
        default="AAA,BBB,SPY",
        help="Comma-separated list of symbols to generate.",
    )
    args = parser.parse_args()
    symbols = [symbol.strip() for symbol in args.symbols.split(",") if symbol.strip()]
    spec = FixtureSpec(symbols=symbols, rows=args.rows, start_date=args.start_date)
    written = write_fixture_ohlcv(args.outdir, spec=spec)
    print(f"Wrote {len(written)} fixture files to {args.outdir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
