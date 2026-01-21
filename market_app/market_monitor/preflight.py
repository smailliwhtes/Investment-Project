from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from market_monitor.corpus.pipeline import (
    discover_corpus_sources,
    validate_corpus_sources,
)
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
    datasets: list[dict[str, Any]]
    corpus: list[dict[str, Any]]

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
    corpus_dir: Path | None = None,
    raw_events_dir: Path | None = None,
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

    datasets = _check_datasets(provider)
    corpus = _check_corpus(corpus_dir, raw_events_dir)
    report = PreflightReport(symbols=symbols, datasets=datasets, corpus=corpus)
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


def _check_datasets(provider: HistoryProvider) -> list[dict[str, Any]]:
    datasets = []
    source = getattr(provider, "source", None)
    if source and getattr(source, "directory", None):
        dir_path = source.directory
        status = "FOUND" if dir_path.exists() else "MISSING"
        detail = "NASDAQ daily directory available" if status == "FOUND" else "NASDAQ daily directory missing"
        datasets.append(
            {
                "name": "nasdaq_daily",
                "status": status,
                "path": str(dir_path),
                "detail": detail,
            }
        )
    return datasets


def _check_corpus(corpus_dir: Path | None, raw_events_dir: Path | None) -> list[dict[str, Any]]:
    if corpus_dir is None and raw_events_dir is None:
        return []
    sources = discover_corpus_sources(corpus_dir, raw_events_dir)
    if not sources:
        return [
            {
                "name": "gdelt_conflict",
                "status": "EMPTY",
                "path": str(corpus_dir or raw_events_dir),
                "detail": "No CSV or raw ZIP files discovered.",
            }
        ]
    report = validate_corpus_sources(
        sources,
        rootcode_top_n=1,
        country_top_k=1,
        chunk_size=50_000,
    )
    required = {"SQLDATE", "EventCode", "EventRootCode", "QuadClass"}
    available = {col for info in report.sources for col in info.columns}
    missing = sorted(required - {col for col in available})
    detail = (
        f"{len(sources)} files parsed; missing columns: {', '.join(missing) if missing else 'none'}"
    )
    return [
        {
            "name": "gdelt_conflict",
            "status": "FOUND" if report.min_date else "EMPTY",
            "path": str(corpus_dir or raw_events_dir),
            "detail": detail,
            "rows": int(sum(info.rows for info in report.sources)),
            "start_date": report.min_date,
            "end_date": report.max_date,
        }
    ]


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

    rows = []
    for symbol in report.symbols:
        rows.append(
            {
                "section": "symbol",
                "name": symbol.symbol,
                "status": symbol.status,
                "file_path": symbol.file_path,
                "rows": symbol.rows,
                "start_date": symbol.start_date,
                "end_date": symbol.end_date,
                "missing_ohlcv_pct": symbol.missing_ohlcv_pct,
                "zero_volume_pct": symbol.zero_volume_pct,
                "volume_available": symbol.volume_available,
                "adjusted_close_available": symbol.adjusted_close_available,
                "detail": None,
            }
        )
    for dataset in report.datasets:
        rows.append(
            {
                "section": "dataset",
                "name": dataset.get("name"),
                "status": dataset.get("status"),
                "file_path": dataset.get("path"),
                "rows": dataset.get("rows"),
                "start_date": dataset.get("start_date"),
                "end_date": dataset.get("end_date"),
                "missing_ohlcv_pct": None,
                "zero_volume_pct": None,
                "volume_available": None,
                "adjusted_close_available": None,
                "detail": dataset.get("detail"),
            }
        )
    for corpus in report.corpus:
        rows.append(
            {
                "section": "corpus",
                "name": corpus.get("name"),
                "status": corpus.get("status"),
                "file_path": corpus.get("path"),
                "rows": corpus.get("rows"),
                "start_date": corpus.get("start_date"),
                "end_date": corpus.get("end_date"),
                "missing_ohlcv_pct": None,
                "zero_volume_pct": None,
                "volume_available": None,
                "adjusted_close_available": None,
                "detail": corpus.get("detail"),
            }
        )

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
        "## Dataset Checks",
        "",
    ]
    if report.datasets:
        for dataset in report.datasets:
            lines.append(f"- {dataset.get('name')}: {dataset.get('status')} ({dataset.get('detail')})")
    else:
        lines.append("- No dataset checks recorded.")
    lines.extend(["", "## Corpus Checks", ""])
    if report.corpus:
        for corpus in report.corpus:
            lines.append(f"- {corpus.get('name')}: {corpus.get('status')} ({corpus.get('detail')})")
    else:
        lines.append("- Corpus not configured.")

    lines.extend(
        [
            "",
            "## Per-Symbol Snapshot",
            "",
            "| Symbol | Status | Rows | Date Range | Missing OHLCV % | Zero Volume % | Volume Available | Adjusted Close |",
            "| --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
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
