from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from market_monitor.corpus.pipeline import (
    discover_corpus_sources,
    validate_corpus_sources,
)
from market_monitor.gates import apply_gates
from market_monitor.providers.base import HistoryProvider, ProviderError
from market_monitor.timebase import parse_as_of_date, parse_now_utc, utcnow


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
    chosen_as_of_date: str | None
    stage1_micro_days: int | None
    stage2_short_days: int | None
    stage3_deep_days: int | None
    stage1_min_history_days: int | None
    stage2_min_history_days: int | None
    stage3_min_history_days: int | None
    stage1_as_of_date: str | None
    stage2_as_of_date: str | None
    stage3_as_of_date: str | None
    stage1_history_days: int | None
    stage2_history_days: int | None
    stage3_history_days: int | None
    stage1_gate_fail_codes: str | None
    stage2_gate_fail_codes: str | None
    stage3_gate_fail_codes: str | None


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
    as_of_date: str | None = None,
    now_utc: str | None = None,
    staging_cfg: dict[str, Any] | None = None,
    gates_cfg: dict[str, Any] | None = None,
    corpus_dir: Path | None = None,
    raw_events_dir: Path | None = None,
) -> PreflightReport:
    symbols: list[PreflightSymbolReport] = []
    anchor_date = _resolve_anchor_date(as_of_date, now_utc)
    stage1_days = staging_cfg.get("stage1_micro_days") if staging_cfg else None
    stage2_days = staging_cfg.get("stage2_short_days") if staging_cfg else None
    stage3_days = staging_cfg.get("stage3_deep_days") if staging_cfg else None
    stage_min_history = staging_cfg.get("history_min_days") if staging_cfg else None
    price_min = gates_cfg.get("price_min") if gates_cfg else None
    price_max = gates_cfg.get("price_max") if gates_cfg else None
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
                    chosen_as_of_date=None,
                    stage1_micro_days=stage1_days,
                    stage2_short_days=stage2_days,
                    stage3_deep_days=stage3_days,
                    stage1_min_history_days=None,
                    stage2_min_history_days=stage_min_history,
                    stage3_min_history_days=stage_min_history,
                    stage1_as_of_date=None,
                    stage2_as_of_date=None,
                    stage3_as_of_date=None,
                    stage1_history_days=0,
                    stage2_history_days=0,
                    stage3_history_days=0,
                    stage1_gate_fail_codes="NO_HISTORY",
                    stage2_gate_fail_codes="NO_HISTORY",
                    stage3_gate_fail_codes="NO_HISTORY",
                )
            )
            logger.warning(f"[preflight] {symbol}: {exc}")
            continue

        stats = _compute_symbol_stats(df, anchor_date=anchor_date)
        stage1 = _stage_snapshot(
            df,
            stage1_days,
            anchor_date=anchor_date,
            min_history=None,
            price_min=price_min,
            price_max=price_max,
        )
        stage2 = _stage_snapshot(
            df,
            stage2_days,
            anchor_date=anchor_date,
            min_history=stage_min_history,
            price_min=price_min,
            price_max=price_max,
        )
        stage3 = _stage_snapshot(
            df,
            stage3_days,
            anchor_date=anchor_date,
            min_history=stage_min_history,
            price_min=price_min,
            price_max=price_max,
        )
        logger.info(
            "[preflight] %s path=%s rows=%s min_date=%s max_date=%s chosen_as_of_date=%s lookback=(%s/%s/%s)",
            symbol,
            file_path,
            stats["rows"],
            stats["start_date"],
            stats["end_date"],
            stats["chosen_as_of_date"],
            stage1_days,
            stage2_days,
            stage3_days,
        )
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
                chosen_as_of_date=stats["chosen_as_of_date"],
                stage1_micro_days=stage1_days,
                stage2_short_days=stage2_days,
                stage3_deep_days=stage3_days,
                stage1_min_history_days=None,
                stage2_min_history_days=stage_min_history,
                stage3_min_history_days=stage_min_history,
                stage1_as_of_date=stage1["as_of_date"],
                stage2_as_of_date=stage2["as_of_date"],
                stage3_as_of_date=stage3["as_of_date"],
                stage1_history_days=stage1["history_days"],
                stage2_history_days=stage2["history_days"],
                stage3_history_days=stage3["history_days"],
                stage1_gate_fail_codes=stage1["gate_fail_codes"],
                stage2_gate_fail_codes=stage2["gate_fail_codes"],
                stage3_gate_fail_codes=stage3["gate_fail_codes"],
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


def _compute_symbol_stats(df: pd.DataFrame, *, anchor_date=None) -> dict[str, Any]:
    rows = len(df)
    start_date = None
    end_date = None
    chosen_as_of_date = None
    if rows and "Date" in df.columns:
        dates = pd.to_datetime(df["Date"], errors="coerce")
        if not dates.isna().all():
            start_date = dates.min().strftime("%Y-%m-%d")
            end_date = dates.max().strftime("%Y-%m-%d")
            if anchor_date is not None:
                filtered = dates[dates <= pd.to_datetime(anchor_date)]
                if not filtered.empty:
                    chosen_as_of_date = filtered.max().strftime("%Y-%m-%d")
            else:
                chosen_as_of_date = end_date

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
        "chosen_as_of_date": chosen_as_of_date,
    }


def _stage_snapshot(
    df: pd.DataFrame,
    days: int | None,
    *,
    anchor_date,
    min_history: int | None,
    price_min: float | None,
    price_max: float | None,
) -> dict[str, Any]:
    frame = df.copy()
    if "Date" in frame.columns:
        frame["Date"] = pd.to_datetime(frame["Date"], errors="coerce")
        frame = frame.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)
    if anchor_date is not None and "Date" in frame.columns:
        frame = frame[frame["Date"] <= pd.to_datetime(anchor_date)].reset_index(drop=True)
    if days is not None and days > 0:
        frame = frame.tail(days).reset_index(drop=True)
    history_days = int(len(frame))
    as_of_date = None
    if not frame.empty and "Date" in frame.columns:
        as_of_date = pd.to_datetime(frame["Date"].iloc[-1], errors="coerce").strftime("%Y-%m-%d")
    gate_fail = []
    if history_days <= 0:
        gate_fail.append("NO_HISTORY")
    if min_history is not None and history_days < min_history:
        gate_fail.append(f"HISTORY<{min_history}")
    if not frame.empty and "Close" in frame.columns:
        last_price = float(frame["Close"].iloc[-1])
        _, price_fail = apply_gates({"last_price": last_price}, price_min, price_max)
        gate_fail.extend(price_fail)
    return {
        "as_of_date": as_of_date,
        "history_days": history_days,
        "gate_fail_codes": ";".join(code for code in gate_fail if code),
    }


def _resolve_anchor_date(as_of_date: str | None, now_utc: str | None):
    if as_of_date:
        return parse_as_of_date(as_of_date)
    if now_utc:
        return parse_now_utc(now_utc).date()
    return None


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

    columns = [
        "section",
        "name",
        "status",
        "file_path",
        "csv_path",
        "row_count",
        "rows",
        "min_date",
        "max_date",
        "start_date",
        "end_date",
        "chosen_as_of_date",
        "stage1_micro_days",
        "stage2_short_days",
        "stage3_deep_days",
        "stage1_min_history_days",
        "stage2_min_history_days",
        "stage3_min_history_days",
        "stage1_as_of_date",
        "stage2_as_of_date",
        "stage3_as_of_date",
        "stage1_history_days",
        "stage2_history_days",
        "stage3_history_days",
        "stage1_gate_fail_codes",
        "stage2_gate_fail_codes",
        "stage3_gate_fail_codes",
        "missing_ohlcv_pct",
        "zero_volume_pct",
        "volume_available",
        "adjusted_close_available",
        "detail",
    ]

    rows = []
    for symbol in report.symbols:
        rows.append(
            {
                "section": "symbol",
                "name": symbol.symbol,
                "status": symbol.status,
                "file_path": symbol.file_path,
                "csv_path": symbol.file_path,
                "row_count": symbol.rows,
                "rows": symbol.rows,
                "min_date": symbol.start_date,
                "max_date": symbol.end_date,
                "start_date": symbol.start_date,
                "end_date": symbol.end_date,
                "chosen_as_of_date": symbol.chosen_as_of_date,
                "stage1_micro_days": symbol.stage1_micro_days,
                "stage2_short_days": symbol.stage2_short_days,
                "stage3_deep_days": symbol.stage3_deep_days,
                "stage1_min_history_days": symbol.stage1_min_history_days,
                "stage2_min_history_days": symbol.stage2_min_history_days,
                "stage3_min_history_days": symbol.stage3_min_history_days,
                "stage1_as_of_date": symbol.stage1_as_of_date,
                "stage2_as_of_date": symbol.stage2_as_of_date,
                "stage3_as_of_date": symbol.stage3_as_of_date,
                "stage1_history_days": symbol.stage1_history_days,
                "stage2_history_days": symbol.stage2_history_days,
                "stage3_history_days": symbol.stage3_history_days,
                "stage1_gate_fail_codes": symbol.stage1_gate_fail_codes,
                "stage2_gate_fail_codes": symbol.stage2_gate_fail_codes,
                "stage3_gate_fail_codes": symbol.stage3_gate_fail_codes,
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
                "csv_path": dataset.get("path"),
                "row_count": dataset.get("rows"),
                "rows": dataset.get("rows"),
                "min_date": dataset.get("start_date"),
                "max_date": dataset.get("end_date"),
                "start_date": dataset.get("start_date"),
                "end_date": dataset.get("end_date"),
                "chosen_as_of_date": None,
                "stage1_micro_days": None,
                "stage2_short_days": None,
                "stage3_deep_days": None,
                "stage1_min_history_days": None,
                "stage2_min_history_days": None,
                "stage3_min_history_days": None,
                "stage1_as_of_date": None,
                "stage2_as_of_date": None,
                "stage3_as_of_date": None,
                "stage1_history_days": None,
                "stage2_history_days": None,
                "stage3_history_days": None,
                "stage1_gate_fail_codes": None,
                "stage2_gate_fail_codes": None,
                "stage3_gate_fail_codes": None,
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
                "csv_path": corpus.get("path"),
                "row_count": corpus.get("rows"),
                "rows": corpus.get("rows"),
                "min_date": corpus.get("start_date"),
                "max_date": corpus.get("end_date"),
                "start_date": corpus.get("start_date"),
                "end_date": corpus.get("end_date"),
                "chosen_as_of_date": None,
                "stage1_micro_days": None,
                "stage2_short_days": None,
                "stage3_deep_days": None,
                "stage1_min_history_days": None,
                "stage2_min_history_days": None,
                "stage3_min_history_days": None,
                "stage1_as_of_date": None,
                "stage2_as_of_date": None,
                "stage3_as_of_date": None,
                "stage1_history_days": None,
                "stage2_history_days": None,
                "stage3_history_days": None,
                "stage1_gate_fail_codes": None,
                "stage2_gate_fail_codes": None,
                "stage3_gate_fail_codes": None,
                "missing_ohlcv_pct": None,
                "zero_volume_pct": None,
                "volume_available": None,
                "adjusted_close_available": None,
                "detail": corpus.get("detail"),
            }
        )

    df = pd.DataFrame(rows, columns=columns)
    df.to_csv(csv_path, index=False)

    total = len(report.symbols)
    found = len(report.found_symbols)
    missing = total - found
    lines = [
        "# Preflight Report",
        "",
        f"- Run ID: {run_id}",
        f"- Generated: {utcnow().isoformat().replace('+00:00', 'Z')}",
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
