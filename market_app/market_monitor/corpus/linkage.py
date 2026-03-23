from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

from market_monitor.corpus.pipeline import CorpusRun, run_corpus_pipeline
from market_monitor.features.io import read_ohlcv, resolve_ohlcv_path
from market_monitor.features.join_exogenous import build_joined_features
from market_monitor.hash_utils import hash_file, hash_manifest
from market_monitor.timebase import utcnow


@dataclass(frozen=True)
class LinkageResult:
    output_dir: Path
    manifest_path: Path
    summary_path: Path
    joined_manifest_path: Path
    market_rows: int
    gdelt_rows: int
    joined_rows: int
    event_impact_rows: int


def _dedupe_symbols(values: Iterable[str]) -> list[str]:
    symbols: list[str] = []
    seen: set[str] = set()
    for value in values:
        symbol = str(value).strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        symbols.append(symbol)
    return symbols


def _pick_column(columns: Iterable[str], candidates: Iterable[str]) -> str | None:
    lowered = {str(col).lower(): str(col) for col in columns}
    for candidate in candidates:
        if candidate.lower() in lowered:
            return lowered[candidate.lower()]
    return None


def _build_market_daily_frame(*, ohlcv_daily_dir: Path, symbols: list[str]) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for symbol in symbols:
        file_path = resolve_ohlcv_path(symbol, ohlcv_daily_dir)
        if file_path is None or not file_path.exists():
            continue
        df = read_ohlcv(file_path)
        if df.empty:
            continue
        close_col = _pick_column(df.columns, ["close"])
        if close_col is None:
            continue
        open_col = _pick_column(df.columns, ["open"])
        high_col = _pick_column(df.columns, ["high"])
        low_col = _pick_column(df.columns, ["low"])
        volume_col = _pick_column(df.columns, ["volume", "vol"])

        frame = pd.DataFrame(
            {
                "day": pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d"),
                "symbol": symbol,
                "open": pd.to_numeric(df[open_col], errors="coerce") if open_col else np.nan,
                "high": pd.to_numeric(df[high_col], errors="coerce") if high_col else np.nan,
                "low": pd.to_numeric(df[low_col], errors="coerce") if low_col else np.nan,
                "close": pd.to_numeric(df[close_col], errors="coerce"),
                "volume": pd.to_numeric(df[volume_col], errors="coerce") if volume_col else np.nan,
            }
        )
        frame = frame.dropna(subset=["day", "close"]).copy()
        frame["return_1d"] = frame["close"].pct_change()
        frame["return_5d"] = frame["close"].pct_change(5)
        frame["dollar_volume"] = frame["close"] * frame["volume"]
        rows.append(frame)

    if not rows:
        raise ValueError(
            f"No OHLCV rows found for requested symbols under {ohlcv_daily_dir}."
        )

    combined = pd.concat(rows, ignore_index=True)
    combined = combined.sort_values(["day", "symbol"]).reset_index(drop=True)
    return combined


def _prepare_gdelt_daily_frame(corpus_run: CorpusRun) -> pd.DataFrame:
    if corpus_run.daily_features is None or corpus_run.daily_features.empty:
        raise ValueError("Corpus pipeline produced no daily features.")
    daily = corpus_run.daily_features.copy()
    day_col = "Date" if "Date" in daily.columns else ("day" if "day" in daily.columns else None)
    if day_col is None:
        raise ValueError("Corpus daily features missing Date/day column.")
    daily["day"] = pd.to_datetime(daily[day_col], errors="coerce").dt.strftime("%Y-%m-%d")
    daily = daily.dropna(subset=["day"]).copy()
    if day_col != "day":
        daily = daily.drop(columns=[day_col], errors="ignore")
    columns = ["day"] + [col for col in daily.columns if col != "day"]
    return daily[columns].sort_values("day").reset_index(drop=True)


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, float) and np.isnan(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _build_return_stats(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty or "forward_days" not in df.columns or "forward_return" not in df.columns:
        return []
    grouped = (
        df.groupby(["symbol", "forward_days"], dropna=False)["forward_return"]
        .agg(["count", "mean", "median", "min", "max"])
        .reset_index()
        .sort_values(["symbol", "forward_days"])
    )
    rows: list[dict[str, Any]] = []
    for row in grouped.to_dict("records"):
        rows.append(
            {
                "symbol": str(row["symbol"]),
                "forward_days": int(row["forward_days"]),
                "count": int(row["count"]),
                "mean_forward_return": _to_float(row["mean"]),
                "median_forward_return": _to_float(row["median"]),
                "min_forward_return": _to_float(row["min"]),
                "max_forward_return": _to_float(row["max"]),
            }
        )
    return rows


def _build_top_context_days(daily_features: pd.DataFrame, *, top_n: int = 5) -> list[dict[str, Any]]:
    if daily_features.empty:
        return []
    for metric in ("conflict_event_count_total", "total_event_count"):
        if metric in daily_features.columns:
            ranked = (
                daily_features[["day", metric]]
                .dropna(subset=["day"])
                .sort_values([metric, "day"], ascending=[False, True])
                .head(top_n)
            )
            rows: list[dict[str, Any]] = []
            for row in ranked.to_dict("records"):
                rows.append({"day": str(row["day"]), "metric": metric, "value": _to_float(row[metric])})
            return rows
    return []


def _build_cause_effect_summary(
    *,
    corpus_run: CorpusRun,
    market_symbols: list[str],
    watchlist_symbols: list[str],
    market_rows: int,
    gdelt_rows: int,
    joined_rows: int,
) -> dict[str, Any]:
    event_impact_df = corpus_run.event_impact if corpus_run.event_impact is not None else pd.DataFrame()
    analog_outcomes_df = (
        pd.DataFrame(corpus_run.analog_outcomes) if corpus_run.analog_outcomes else pd.DataFrame()
    )
    daily_features = _prepare_gdelt_daily_frame(corpus_run)

    return {
        "generated_at_utc": utcnow().isoformat(),
        "market_symbols": market_symbols,
        "watchlist_symbols": watchlist_symbols,
        "counts": {
            "market_rows": int(market_rows),
            "gdelt_rows": int(gdelt_rows),
            "joined_rows": int(joined_rows),
            "event_impact_rows": int(len(event_impact_df)),
            "analog_outcomes_rows": int(len(analog_outcomes_df)),
        },
        "top_context_days": _build_top_context_days(daily_features, top_n=5),
        "event_impact_return_stats": _build_return_stats(event_impact_df),
        "analog_outcome_return_stats": _build_return_stats(analog_outcomes_df),
    }


def build_market_gdelt_linkage(
    *,
    config: dict[str, Any],
    base_dir: Path,
    provider,
    watchlist_symbols: list[str],
    output_dir: Path,
    lags: list[int],
    rolling_window: int | None,
    rolling_mean: bool,
    rolling_sum: bool,
    rolling_min_periods: int | None,
    include_raw_gdelt: bool,
    output_format: str,
    logger,
) -> LinkageResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    corpus_paths = config.get("corpus", {})
    pipeline_cfg = config.get("pipeline", {})
    benchmarks = [str(symbol).upper() for symbol in pipeline_cfg.get("benchmarks") or []]
    market_symbols = _dedupe_symbols(watchlist_symbols + benchmarks)

    corpus_root_value = corpus_paths.get("gdelt_conflict_dir") or corpus_paths.get("root_dir")
    if not corpus_root_value:
        raise ValueError("Missing corpus.gdelt_conflict_dir/root_dir in config.")
    corpus_root = Path(corpus_root_value).expanduser()
    if not corpus_root.is_absolute():
        corpus_root = (base_dir / corpus_root).resolve()

    raw_events = corpus_paths.get("gdelt_events_raw_dir")
    raw_events_dir = Path(raw_events).expanduser() if raw_events else None
    if raw_events_dir and not raw_events_dir.is_absolute():
        raw_events_dir = (base_dir / raw_events_dir).resolve()

    corpus_out_dir = output_dir / "corpus"
    corpus_run = run_corpus_pipeline(
        corpus_dir=corpus_root,
        raw_events_dir=raw_events_dir,
        outputs_dir=corpus_out_dir,
        config=config,
        provider=provider,
        watchlist=watchlist_symbols,
        logger=logger,
    )

    gdelt_daily_df = _prepare_gdelt_daily_frame(corpus_run)
    gdelt_daily_path = output_dir / "gdelt_daily_features.csv"
    gdelt_daily_df.to_csv(gdelt_daily_path, index=False, lineterminator="\n")

    ohlcv_daily_setting = config.get("paths", {}).get("ohlcv_daily_dir", "data/ohlcv_daily")
    ohlcv_daily_dir = Path(ohlcv_daily_setting).expanduser()
    if not ohlcv_daily_dir.is_absolute():
        ohlcv_daily_dir = (base_dir / ohlcv_daily_dir).resolve()
    market_daily_df = _build_market_daily_frame(
        ohlcv_daily_dir=ohlcv_daily_dir,
        symbols=market_symbols,
    )
    market_daily_path = output_dir / "market_daily.csv"
    market_daily_df.to_csv(market_daily_path, index=False, lineterminator="\n")

    linked_out_dir = output_dir / "linked_market_gdelt"
    joined_result = build_joined_features(
        market_path=market_daily_path,
        gdelt_path=gdelt_daily_path,
        out_dir=linked_out_dir,
        lags=lags,
        rolling_window=rolling_window,
        rolling_mean=rolling_mean,
        rolling_sum=rolling_sum,
        rolling_min_periods=rolling_min_periods,
        output_format=output_format,
        include_raw_gdelt=include_raw_gdelt,
    )

    event_impact_rows = 0
    event_impact_path = output_dir / "event_impact_library.csv"
    if corpus_run.event_impact is not None and not corpus_run.event_impact.empty:
        corpus_run.event_impact.sort_values(["event_date", "symbol", "forward_days"]).to_csv(
            event_impact_path,
            index=False,
            lineterminator="\n",
        )
        event_impact_rows = int(len(corpus_run.event_impact))

    analog_outcomes_path = output_dir / "analog_outcomes.csv"
    if corpus_run.analog_outcomes:
        pd.DataFrame(corpus_run.analog_outcomes).sort_values(
            ["analog_date", "symbol", "forward_days"]
        ).to_csv(analog_outcomes_path, index=False, lineterminator="\n")

    summary_payload = _build_cause_effect_summary(
        corpus_run=corpus_run,
        market_symbols=market_symbols,
        watchlist_symbols=watchlist_symbols,
        market_rows=len(market_daily_df),
        gdelt_rows=len(gdelt_daily_df),
        joined_rows=joined_result.rows,
    )
    summary_path = output_dir / "cause_effect_summary.json"
    summary_path.write_text(
        json.dumps(summary_payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    artifact_paths: list[Path] = [
        market_daily_path,
        gdelt_daily_path,
        summary_path,
        joined_result.manifest_path,
        output_dir / "corpus" / "corpus_manifest.json",
    ]
    if event_impact_path.exists():
        artifact_paths.append(event_impact_path)
    if analog_outcomes_path.exists():
        artifact_paths.append(analog_outcomes_path)
    if (output_dir / "corpus" / "analogs_report.md").exists():
        artifact_paths.append(output_dir / "corpus" / "analogs_report.md")

    manifest_payload = {
        "schema_version": 1,
        "generated_at_utc": utcnow().isoformat(),
        "market_symbols": market_symbols,
        "watchlist_symbols": watchlist_symbols,
        "config": {
            "lags": lags,
            "rolling_window": rolling_window,
            "rolling_mean": rolling_mean,
            "rolling_sum": rolling_sum,
            "rolling_min_periods": rolling_min_periods,
            "include_raw_gdelt": include_raw_gdelt,
            "output_format": output_format,
        },
        "counts": {
            "market_rows": int(len(market_daily_df)),
            "gdelt_rows": int(len(gdelt_daily_df)),
            "joined_rows": int(joined_result.rows),
            "event_impact_rows": int(event_impact_rows),
        },
        "artifacts": [
            {
                "path": path.relative_to(output_dir).as_posix(),
                "sha256": hash_file(path),
                "bytes": int(path.stat().st_size),
            }
            for path in artifact_paths
            if path.exists()
        ],
    }
    manifest_payload["content_hash"] = hash_manifest(
        {
            "config": manifest_payload["config"],
            "counts": manifest_payload["counts"],
            "artifacts": manifest_payload["artifacts"],
        }
    )

    manifest_path = output_dir / "cause_effect_manifest.json"
    manifest_path.write_text(
        json.dumps(manifest_payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    return LinkageResult(
        output_dir=output_dir,
        manifest_path=manifest_path,
        summary_path=summary_path,
        joined_manifest_path=joined_result.manifest_path,
        market_rows=len(market_daily_df),
        gdelt_rows=len(gdelt_daily_df),
        joined_rows=joined_result.rows,
        event_impact_rows=event_impact_rows,
    )
