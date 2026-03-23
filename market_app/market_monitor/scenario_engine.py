from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd
import yaml

from market_monitor.etf_holdings import load_etf_holdings
from market_monitor.features.io import iter_ohlcv_paths, symbol_from_ohlcv_path
from market_monitor.fred import load_fred_cache
from market_monitor.manifest import resolve_git_commit
from market_monitor.paths import resolve_path
from market_monitor.policy_event_schema import PolicyEvent, PolicyScenarioTemplate
from market_monitor.policy_events import load_policy_events, load_policy_source_catalog
from market_monitor.policy_explain import write_policy_report
from market_monitor.policy_exposure import resolve_policy_exposure
from market_monitor.policy_features import build_policy_event_frame
from market_monitor.policy_manifest import build_policy_manifest, write_policy_manifest
from market_monitor.policy_regimes import classify_policy_regime
from market_monitor.policy_score import rank_policy_impacts
from market_monitor.tabular_io import read_tabular
from market_monitor.timebase import utcnow

ProgressCallback = Callable[[str, str, str, float | None], None]


class PolicyScenarioError(RuntimeError):
    pass


class PolicyMissingInputError(PolicyScenarioError):
    pass


@dataclass(frozen=True)
class PolicyPaths:
    events_file: Path
    fred_cache_dir: Path
    gdelt_daily_file: Path
    etf_holdings_file: Path
    sources_config: Path
    scenarios_config: Path


@dataclass(frozen=True)
class PolicyScenarioRun:
    run_id: str
    scenario_name: str
    output_dir: Path
    report_path: Path
    manifest_path: Path
    summary_path: Path
    rankings_path: Path
    event_study_path: Path
    analogs_path: Path


def resolve_policy_paths(config: dict[str, Any], base_dir: Path) -> PolicyPaths:
    policy_cfg = config.get("policy", {})
    paths_cfg = policy_cfg.get("paths", {})
    events_file = resolve_path(base_dir, paths_cfg.get("events_file") or "data/policy_events.jsonl")
    fred_cache_dir = resolve_path(base_dir, paths_cfg.get("fred_cache_dir") or "data/fred_cache")
    gdelt_daily_file = resolve_path(
        base_dir,
        paths_cfg.get("gdelt_daily_file") or "data/policy_gdelt_daily_features.csv",
    )
    etf_holdings_file = resolve_path(
        base_dir,
        paths_cfg.get("etf_holdings_file") or "data/etf_holdings.csv",
    )
    sources_config = resolve_path(
        base_dir,
        paths_cfg.get("sources_config") or "policy_sources.yaml",
    )
    scenarios_config = resolve_path(
        base_dir,
        paths_cfg.get("scenarios_config") or "policy_scenarios.yaml",
    )
    if not all(
        path is not None
        for path in (
            events_file,
            fred_cache_dir,
            gdelt_daily_file,
            etf_holdings_file,
            sources_config,
            scenarios_config,
        )
    ):
        raise PolicyMissingInputError("Policy paths could not be resolved from config.")
    return PolicyPaths(
        events_file=events_file,
        fred_cache_dir=fred_cache_dir,
        gdelt_daily_file=gdelt_daily_file,
        etf_holdings_file=etf_holdings_file,
        sources_config=sources_config,
        scenarios_config=scenarios_config,
    )


def load_policy_scenarios(path: Path) -> dict[str, PolicyScenarioTemplate]:
    if not path.exists():
        raise PolicyMissingInputError(f"Policy scenarios config not found: {path}")
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    raw_scenarios = payload.get("scenarios", payload)
    if not isinstance(raw_scenarios, dict):
        raise PolicyScenarioError(f"Scenario catalog must be a mapping: {path}")
    return {
        str(name): PolicyScenarioTemplate.from_mapping(str(name), scenario_payload or {})
        for name, scenario_payload in raw_scenarios.items()
    }


def _load_available_symbols(provider) -> set[str]:
    directory = getattr(getattr(provider, "provider", provider), "source", None)
    base_dir = getattr(directory, "directory", None)
    if base_dir is None:
        return set()
    return {symbol_from_ohlcv_path(path) for path in iter_ohlcv_paths(Path(base_dir))}


def _ensure_gdelt_daily_features(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise PolicyMissingInputError(f"GDELT daily feature cache not found: {path}")
    frame = read_tabular(path)
    if "Date" not in frame.columns and "date" in frame.columns:
        frame = frame.rename(columns={"date": "Date"})
    if "Date" not in frame.columns:
        raise PolicyScenarioError(f"GDELT daily feature cache is missing Date column: {path}")
    return frame


def _align_start_date(index_dates: list[pd.Timestamp], event_date: str) -> int | None:
    event_ts = pd.to_datetime(event_date, errors="coerce")
    if pd.isna(event_ts):
        return None
    for idx, candidate in enumerate(index_dates):
        if candidate >= event_ts:
            return idx
    return None


def _window_return(close: pd.Series, event_date: str, horizon_days: int) -> float | None:
    dates = list(pd.to_datetime(close.index, errors="coerce"))
    start_idx = _align_start_date(dates, event_date)
    if start_idx is None:
        return None
    end_idx = start_idx + int(horizon_days)
    if end_idx >= len(close):
        return None
    start_price = float(close.iloc[start_idx])
    end_price = float(close.iloc[end_idx])
    if start_price <= 0:
        return None
    return end_price / start_price - 1.0


def _pre_event_volatility(close: pd.Series, event_date: str, lookback_days: int) -> float:
    dates = list(pd.to_datetime(close.index, errors="coerce"))
    start_idx = _align_start_date(dates, event_date)
    if start_idx is None or start_idx <= 1:
        return 0.0
    window_start = max(0, start_idx - lookback_days)
    returns = close.iloc[window_start:start_idx].pct_change().dropna()
    if returns.empty:
        return 0.0
    return float(returns.std(ddof=0) or 0.0)


def _event_volume_ratio(frame: pd.DataFrame, event_date: str) -> float | None:
    if "Volume" not in frame.columns:
        return None
    dates = list(pd.to_datetime(frame["Date"], errors="coerce"))
    start_idx = _align_start_date(dates, event_date)
    if start_idx is None:
        return None
    day_volume = pd.to_numeric(frame["Volume"], errors="coerce").iloc[start_idx]
    baseline = pd.to_numeric(frame["Volume"], errors="coerce").iloc[max(0, start_idx - 20):start_idx].dropna()
    if pd.isna(day_volume) or baseline.empty or float(baseline.mean()) <= 0:
        return None
    return float(day_volume / float(baseline.mean()))


def compute_policy_event_study(
    events: list[PolicyEvent],
    *,
    symbols: list[str],
    provider,
    benchmark_symbol: str,
    horizons: list[int],
    estimation_lookback_days: int,
) -> pd.DataFrame:
    if not events or not symbols:
        return pd.DataFrame()

    benchmark_frame, _ = provider.load_symbol_data(benchmark_symbol)
    benchmark_close = benchmark_frame.set_index("Date")["Close"]

    rows: list[dict[str, Any]] = []
    for symbol in symbols:
        try:
            asset_frame, _ = provider.load_symbol_data(symbol)
        except Exception:
            continue
        asset_close = asset_frame.set_index("Date")["Close"]
        volume_ratio = {
            event.event_id: _event_volume_ratio(asset_frame, event.event_date)
            for event in events
        }
        for event in events:
            window_volatility = _pre_event_volatility(
                asset_close,
                event.event_date,
                estimation_lookback_days,
            )
            for horizon_days in horizons:
                asset_return = _window_return(asset_close, event.event_date, horizon_days)
                benchmark_return = _window_return(benchmark_close, event.event_date, horizon_days)
                if asset_return is None or benchmark_return is None:
                    continue
                abnormal = asset_return - benchmark_return
                denom = max(window_volatility * np.sqrt(max(horizon_days, 1)), 1e-6)
                rows.append(
                    {
                        "event_id": event.event_id,
                        "event_type": event.event_type,
                        "event_date": event.event_date,
                        "symbol": symbol,
                        "horizon_days": int(horizon_days),
                        "asset_return": float(asset_return),
                        "benchmark_return": float(benchmark_return),
                        "cumulative_abnormal_return": float(abnormal),
                        "window_volatility": float(window_volatility),
                        "abnormal_zscore": float(abnormal / denom),
                        "event_volume_ratio": volume_ratio[event.event_id],
                    }
                )
    return pd.DataFrame(rows).sort_values(
        ["event_date", "event_id", "symbol", "horizon_days"]
    ).reset_index(drop=True)


def _jaccard_distance(left: set[str], right: set[str]) -> float:
    if not left and not right:
        return 0.0
    union = left | right
    if not union:
        return 0.0
    return 1.0 - (len(left & right) / len(union))


def _split_pipe(value: object) -> set[str]:
    text = str(value or "")
    return {part.strip().lower() for part in text.split("|") if part.strip()}


def retrieve_policy_analogs(
    event_frame: pd.DataFrame,
    *,
    template: PolicyScenarioTemplate,
    regime_snapshot: dict[str, Any],
    top_n: int,
) -> pd.DataFrame:
    if event_frame.empty:
        return pd.DataFrame()

    same_type = event_frame[event_frame["event_type"] == template.event_type]
    candidate_frame = same_type if not same_type.empty else event_frame

    distances: list[dict[str, Any]] = []
    template_sectors = set(template.sectors)
    template_countries = {country.lower() for country in template.countries}
    for _, row in candidate_frame.iterrows():
        distance = 0.0
        if row.get("event_type") != template.event_type:
            distance += 1.0
        distance += abs(float(row.get("severity", 0.5)) - template.severity)
        distance += _jaccard_distance(_split_pipe(row.get("sectors")), template_sectors)
        distance += 0.5 * _jaccard_distance(_split_pipe(row.get("countries")), template_countries)
        if row.get("macro_regime") != regime_snapshot.get("macro_regime"):
            distance += 0.35
        if row.get("policy_stance") != regime_snapshot.get("policy_stance"):
            distance += 0.2
        if row.get("conflict_regime") != regime_snapshot.get("conflict_regime"):
            distance += 0.2
        similarity = 1.0 / (1.0 + distance)
        distances.append(
            {
                "event_id": row.get("event_id"),
                "event_date": row.get("event_date"),
                "event_type": row.get("event_type"),
                "title": row.get("title"),
                "severity": row.get("severity"),
                "distance": distance,
                "similarity": similarity,
            }
        )

    analogs = pd.DataFrame(distances).sort_values(
        ["distance", "event_date", "event_id"],
        ascending=[True, True, True],
    ).head(top_n).reset_index(drop=True)
    if analogs.empty:
        return analogs
    analogs["rank"] = range(1, len(analogs) + 1)
    return analogs


def _summarize_simulations(
    event_study: pd.DataFrame,
    analogs: pd.DataFrame,
    *,
    symbols: list[str],
    horizons: list[int],
    seed: int,
    path_count: int,
    severity: float,
    direction_bias: int,
) -> pd.DataFrame:
    analog_ids = set(analogs["event_id"].astype(str))
    rng = np.random.default_rng(seed)
    rows: list[dict[str, Any]] = []
    for symbol in symbols:
        summary: dict[str, Any] = {"symbol": symbol}
        symbol_rows = event_study[
            (event_study["symbol"].astype(str).str.upper() == symbol.upper())
            & (event_study["event_id"].astype(str).isin(analog_ids))
        ]
        symbol_analog_count = int(symbol_rows["event_id"].astype(str).nunique())
        summary["analog_count"] = symbol_analog_count
        summary["simulation_basis"] = "empirical" if symbol_analog_count > 0 else "synthetic_fallback"
        for horizon_days in horizons:
            horizon_rows = symbol_rows[symbol_rows["horizon_days"] == int(horizon_days)]
            base = horizon_rows["cumulative_abnormal_return"].dropna().to_numpy(dtype=float)
            effective_sample_count = int(base.size)
            summary[f"effective_sample_count_{horizon_days}d"] = effective_sample_count
            if effective_sample_count == 0:
                base = np.array([0.0], dtype=float)
            scale = max(float(base.std(ddof=0) or 0.0), 0.005)
            draws = rng.choice(base, size=max(path_count, 1), replace=True)
            noise = rng.standard_t(df=5, size=max(path_count, 1)) * scale * 0.35
            jump_bias = float(direction_bias) * float(severity) * min(0.04, scale + 0.01)
            simulated = draws + noise + jump_bias
            summary[f"median_return_{horizon_days}d"] = float(np.median(simulated))
            summary[f"q10_return_{horizon_days}d"] = float(np.quantile(simulated, 0.10))
            summary[f"q25_return_{horizon_days}d"] = float(np.quantile(simulated, 0.25))
            summary[f"q75_return_{horizon_days}d"] = float(np.quantile(simulated, 0.75))
            summary[f"q90_return_{horizon_days}d"] = float(np.quantile(simulated, 0.90))
            summary[f"expected_volatility_{horizon_days}d"] = float(np.std(simulated, ddof=0))
        rows.append(summary)
    return pd.DataFrame(rows)


def run_policy_scenario(
    config: dict[str, Any],
    *,
    config_path: Path,
    config_hash: str,
    base_dir: Path,
    output_dir: Path,
    scenario_name: str,
    provider,
    as_of_date: str,
    seed: int | None = None,
    progress: ProgressCallback | None = None,
) -> PolicyScenarioRun:
    output_dir.mkdir(parents=True, exist_ok=True)
    policy_paths = resolve_policy_paths(config, base_dir)
    if progress is not None:
        progress("stage_start", "policy_inputs", "Loading policy datasets", 0.05)

    source_catalog = load_policy_source_catalog(policy_paths.sources_config)
    _ = source_catalog
    events = load_policy_events(policy_paths.events_file)
    macro_frame = load_fred_cache(policy_paths.fred_cache_dir)
    gdelt_frame = _ensure_gdelt_daily_features(policy_paths.gdelt_daily_file)
    holdings = load_etf_holdings(policy_paths.etf_holdings_file)
    scenarios = load_policy_scenarios(policy_paths.scenarios_config)

    if scenario_name not in scenarios:
        available = ", ".join(sorted(scenarios))
        raise PolicyScenarioError(
            f"Unknown policy scenario '{scenario_name}'. Available scenarios: {available}"
        )
    template = scenarios[scenario_name]

    available_symbols = _load_available_symbols(provider)
    exposure = resolve_policy_exposure(template, holdings, available_symbols=available_symbols)
    symbols = exposure["symbols"]
    if not symbols:
        raise PolicyScenarioError(
            f"Scenario '{scenario_name}' resolved no symbols present in the local OHLCV store."
        )

    regime_snapshot = classify_policy_regime(as_of_date, macro_frame, gdelt_frame)
    historical_events = [
        event
        for event in events
        if pd.to_datetime(event.event_date, errors="coerce") <= pd.to_datetime(as_of_date, errors="coerce")
    ]
    if not historical_events:
        raise PolicyScenarioError("No historical policy events are available on/before the selected as-of date.")

    if progress is not None:
        progress("stage_progress", "policy_inputs", "Policy datasets loaded", 0.18)
        progress("stage_start", "policy_event_study", "Computing policy event-study lane", 0.22)

    simulation_cfg = config.get("policy", {}).get("simulation", {})
    horizons = [int(value) for value in simulation_cfg.get("horizons", [1, 5, 20, 60])]
    estimation_lookback_days = int(simulation_cfg.get("estimation_lookback_days", 60))
    top_n_analogs = int(simulation_cfg.get("top_n_analogs", 5))
    path_count = int(simulation_cfg.get("path_count", 256))
    seed_value = int(seed if seed is not None else simulation_cfg.get("seed", 20260321))
    benchmark_symbol = template.benchmark_symbol or simulation_cfg.get("benchmark_symbol", "SPY")
    average_dollar_volume_floor = float(
        simulation_cfg.get("average_dollar_volume_floor", 1_000_000.0)
    )

    event_study = compute_policy_event_study(
        historical_events,
        symbols=symbols,
        provider=provider,
        benchmark_symbol=benchmark_symbol,
        horizons=horizons,
        estimation_lookback_days=estimation_lookback_days,
    )
    if event_study.empty:
        raise PolicyScenarioError("Policy event-study produced no usable rows for the resolved symbol set.")

    if progress is not None:
        progress("stage_end", "policy_event_study", "Policy event-study complete", 0.45)
        progress("stage_start", "policy_analogs", "Retrieving historical analogs", 0.5)

    event_frame = build_policy_event_frame(
        historical_events,
        macro_frame=macro_frame,
        gdelt_frame=gdelt_frame,
    )
    analogs = retrieve_policy_analogs(
        event_frame,
        template=template,
        regime_snapshot=regime_snapshot,
        top_n=top_n_analogs,
    )
    if analogs.empty:
        raise PolicyScenarioError("No policy analogs were found for the selected scenario.")

    if progress is not None:
        progress("stage_end", "policy_analogs", "Historical analogs retrieved", 0.64)
        progress("stage_start", "policy_simulation", "Running synthetic policy simulations", 0.68)

    simulation_summary = _summarize_simulations(
        event_study,
        analogs,
        symbols=symbols,
        horizons=horizons,
        seed=seed_value,
        path_count=path_count,
        severity=template.severity,
        direction_bias=template.direction_bias,
    )
    rankings = rank_policy_impacts(
        simulation_summary,
        event_study,
        provider=provider,
        as_of_date=as_of_date,
        average_dollar_volume_floor=average_dollar_volume_floor,
        analog_count=len(analogs),
        top_n_analogs=top_n_analogs,
    )

    if progress is not None:
        progress("stage_end", "policy_simulation", "Synthetic policy simulations complete", 0.84)
        progress("stage_start", "policy_outputs", "Writing policy artifacts", 0.88)

    run_id = f"policy_{scenario_name}_{utcnow().strftime('%Y%m%d_%H%M%S')}"
    event_study_path = output_dir / "policy_event_study.csv"
    analogs_path = output_dir / "policy_analogs.csv"
    rankings_path = output_dir / "policy_scenario_rankings.csv"
    report_path = output_dir / "policy_report.md"
    manifest_path = output_dir / "policy_manifest.json"
    summary_path = output_dir / "policy_summary.json"

    event_study.to_csv(event_study_path, index=False)
    analogs.to_csv(analogs_path, index=False)
    rankings.to_csv(rankings_path, index=False)
    write_policy_report(
        report_path,
        scenario_name=scenario_name,
        scenario_description=template.description,
        regime_snapshot=regime_snapshot,
        rankings=rankings,
        analogs=analogs,
        event_study=event_study,
    )

    top_rankings = rankings.head(10).to_dict(orient="records")
    summary_fields = [
        {"name": "as_of_date", "value": as_of_date},
        {"name": "benchmark_symbol", "value": benchmark_symbol},
        {"name": "symbol_count", "value": str(len(symbols))},
        {"name": "analog_count", "value": str(len(analogs))},
        {"name": "ranked_symbols", "value": str(len(rankings))},
    ]
    summary_payload = {
        "run_id": run_id,
        "scenario": scenario_name,
        "scenario_name": scenario_name,
        "status": "complete",
        "summary": "Policy scenario simulation complete.",
        "description": template.description,
        "as_of_date": as_of_date,
        "seed": seed_value,
        "benchmark_symbol": benchmark_symbol,
        "regime": regime_snapshot,
        "symbols": symbols,
        "output_dir": str(output_dir),
        "fields": summary_fields,
        "top_rankings": top_rankings,
        "report_path": str(report_path),
        "manifest_path": str(manifest_path),
        "event_study_path": str(event_study_path),
        "analogs_path": str(analogs_path),
        "rankings_path": str(rankings_path),
    }
    summary_path.write_text(json.dumps(summary_payload, indent=2, sort_keys=True), encoding="utf-8")

    manifest = build_policy_manifest(
        run_id=run_id,
        scenario_name=scenario_name,
        created_at=utcnow().isoformat(),
        as_of_date=as_of_date,
        seed=seed_value,
        config_path=config_path,
        config_hash=config_hash,
        git_commit=resolve_git_commit(base_dir),
        regime_snapshot=regime_snapshot,
        input_paths={
            "policy_events": policy_paths.events_file,
            "fred_cache": policy_paths.fred_cache_dir,
            "gdelt_daily_features": policy_paths.gdelt_daily_file,
            "etf_holdings": policy_paths.etf_holdings_file,
            "policy_sources": policy_paths.sources_config,
            "policy_scenarios": policy_paths.scenarios_config,
        },
        counts={
            "historical_events": len(historical_events),
            "analog_events": len(analogs),
            "symbols_ranked": len(rankings),
        },
        artifact_paths=[event_study_path, analogs_path, rankings_path, report_path, summary_path],
    )
    write_policy_manifest(manifest_path, manifest)

    if progress is not None:
        progress("stage_end", "policy_outputs", "Policy artifacts written", 1.0)

    return PolicyScenarioRun(
        run_id=run_id,
        scenario_name=scenario_name,
        output_dir=output_dir,
        report_path=report_path,
        manifest_path=manifest_path,
        summary_path=summary_path,
        rankings_path=rankings_path,
        event_study_path=event_study_path,
        analogs_path=analogs_path,
    )
