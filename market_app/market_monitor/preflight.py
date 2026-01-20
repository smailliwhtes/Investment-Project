from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from market_monitor.providers.base import HistoryProvider, ProviderError


@dataclass(frozen=True)
class PreflightSymbolReport:
    symbol: str
    status: str
    file_path: str
    rows: int
    start_date: str | None
    end_date: str | None
    missing_ohlcv_pct: float | None
    zero_volume_pct: float | None
    volume_available: bool
    adjusted_close_available: bool


@dataclass(frozen=True)
class PreflightReport:
    symbols: list[PreflightSymbolReport]

    @property
    def found_symbols(self) -> list[str]:
        return [s.symbol for s in self.symbols if s.status == "FOUND"]


def run_preflight(
    universe_df: pd.DataFrame,
    provider: HistoryProvider,
    outputs_dir: Path,
    *,
    run_id: str,
    run_timestamp: str,
    logger,
) -> PreflightReport:
    symbols: list[PreflightSymbolReport] = []
    for _, row in universe_df.iterrows():
        symbol = row["symbol"]
        try:
            df, file_path = _load_symbol(provider, symbol)
        except ProviderError as exc:
            symbols.append(
                PreflightSymbolReport(
                    symbol=symbol,
                    status="MISSING",
                    file_path="",
                    rows=0,
                    start_date=None,
                    end_date=None,
                    missing_ohlcv_pct=None,
                    zero_volume_pct=None,
                    volume_available=False,
                    adjusted_close_available=False,
                )
            )
            logger.warning(f"[preflight] {symbol}: {exc}")
            continue

        stats = _compute_symbol_stats(df)
        symbols.append(
            PreflightSymbolReport(
                symbol=symbol,
                status="FOUND",
                file_path=str(file_path),
                rows=stats["rows"],
                start_date=stats["start_date"],
                end_date=stats["end_date"],
                missing_ohlcv_pct=stats["missing_ohlcv_pct"],
                zero_volume_pct=stats["zero_volume_pct"],
                volume_available=stats["volume_available"],
                adjusted_close_available=stats["adjusted_close_available"],
            )
        )

    report = PreflightReport(symbols=symbols)
    _write_preflight_reports(report, outputs_dir, run_id=run_id, run_timestamp=run_timestamp)
    return report


def _load_symbol(provider: HistoryProvider, symbol: str) -> tuple[pd.DataFrame, Path]:
    if not hasattr(provider, "load_symbol_data"):
        raise ProviderError("Preflight requires a file-based provider.")
    df, file_path = provider.load_symbol_data(symbol)
    return df, file_path


def _compute_symbol_stats(df: pd.DataFrame) -> dict[str, Any]:
    rows = len(df)
    start_date = None
    end_date = None
    if rows and "Date" in df.columns:
        dates = pd.to_datetime(df["Date"], errors="coerce")
        if not dates.isna().all():
            start_date = dates.min().strftime("%Y-%m-%d")
            end_date = dates.max().strftime("%Y-%m-%d")

    missing_ohlcv_pct = None
    if rows:
        cols = ["Open", "High", "Low", "Close"]
        missing_any = df[cols].isna().any(axis=1) if all(c in df.columns for c in cols) else None
        if missing_any is not None:
            missing_ohlcv_pct = float(np.mean(missing_any) * 100.0)

    volume_available = "Volume" in df.columns and df["Volume"].notna().any()
    zero_volume_pct = None
    if rows and volume_available:
        zero_volume_pct = float((df["Volume"] == 0).mean() * 100.0)

    adjusted_close_available = "Adjusted_Close" in df.columns and df["Adjusted_Close"].notna().any()

    return {
        "rows": int(rows),
        "start_date": start_date,
        "end_date": end_date,
        "missing_ohlcv_pct": missing_ohlcv_pct,
        "zero_volume_pct": zero_volume_pct,
        "volume_available": bool(volume_available),
        "adjusted_close_available": bool(adjusted_close_available),
    }


def _write_preflight_reports(
    report: PreflightReport,
    outputs_dir: Path,
    *,
    run_id: str,
    run_timestamp: str,
) -> None:
    outputs_dir.mkdir(parents=True, exist_ok=True)
    csv_path = outputs_dir / "preflight_report.csv"
    md_path = outputs_dir / "preflight_report.md"

    rows = [
        {
            "symbol": symbol.symbol,
            "status": symbol.status,
            "file_path": symbol.file_path,
            "rows": symbol.rows,
            "start_date": symbol.start_date,
            "end_date": symbol.end_date,
            "missing_ohlcv_pct": symbol.missing_ohlcv_pct,
            "zero_volume_pct": symbol.zero_volume_pct,
            "volume_available": symbol.volume_available,
            "adjusted_close_available": symbol.adjusted_close_available,
        }
        for symbol in report.symbols
    ]
    df = pd.DataFrame(rows)
    df.to_csv(csv_path, index=False)

    total = len(report.symbols)
    found = len(report.found_symbols)
    missing = total - found
    lines = [
        "# Preflight Report",
        "",
        f"- Run ID: {run_id}",
        f"- Generated: {datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')}",
        f"- Run timestamp: {run_timestamp}",
        "",
        "## Coverage Summary",
        "",
        f"- Symbols requested: {total}",
        f"- Symbols found: {found}",
        f"- Symbols missing: {missing}",
        "",
        "## Per-Symbol Snapshot",
        "",
        "| Symbol | Status | Rows | Date Range | Missing OHLCV % | Zero Volume % | Volume Available | Adjusted Close |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in report.symbols:
        date_range = "-"
        if row.start_date and row.end_date:
            date_range = f"{row.start_date} → {row.end_date}"
        missing_pct = f"{row.missing_ohlcv_pct:.2f}" if row.missing_ohlcv_pct is not None else "NA"
        zero_pct = f"{row.zero_volume_pct:.2f}" if row.zero_volume_pct is not None else "NA"
        lines.append(
            "| "
            + " | ".join(
                [
                    row.symbol,
                    row.status,
                    str(row.rows),
                    date_range,
                    missing_pct,
                    zero_pct,
                    "yes" if row.volume_available else "no",
                    "yes" if row.adjusted_close_available else "no",
                ]
            )
            + " |"
        )

    md_path.write_text("\n".join(lines), encoding="utf-8")
