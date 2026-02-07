from __future__ import annotations

import argparse
import json
import logging
import logging.config
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
import pandas as pd
import numpy as np

from market_app.config import ConfigResult, load_config, map_to_engine_config
from market_app.outputs import (
    REQUIRED_FEATURES,
    apply_blueprint_gates,
    build_forward_outcome_column,
    build_manifest,
    build_report,
    build_risk_flags,
    compute_forward_outcome_summary,
    normalize_features,
    write_csv,
)
from market_monitor.hash_utils import hash_file, hash_text
from market_monitor.manifest import resolve_git_commit
from market_monitor.paths import find_repo_root
from market_monitor.pipeline import PipelineResult, run_pipeline
from market_monitor.themes import tag_themes


def _build_run_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--run-id", "--run_id", dest="run_id", default=None)
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--runs-dir", default=None)
    parser.add_argument("--top-n", "--top_n", dest="top_n", type=int, default=None)
    variant = parser.add_mutually_exclusive_group()
    variant.add_argument("--conservative", action="store_true")
    variant.add_argument("--opportunistic", action="store_true")
    parser.set_defaults(command="run")


def _build_doctor_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--offline", action="store_true")
    parser.set_defaults(command="doctor")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Offline-first market_app wrapper CLI")
    subparsers = parser.add_subparsers(dest="command")
    run_parser = subparsers.add_parser("run", help="Run the market monitor pipeline")
    _build_run_parser(run_parser)
    doctor_parser = subparsers.add_parser("doctor", help="Validate config and data paths")
    _build_doctor_parser(doctor_parser)
    return parser


def _parse_legacy_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Offline-first market_app wrapper CLI (legacy)")
    _build_run_parser(parser)
    return parser.parse_args(argv)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    if argv is None:
        argv = sys.argv[1:]
    if not argv or argv[0].startswith("-"):
        return _parse_legacy_args(argv)
    parser = _build_parser()
    return parser.parse_args(argv)


def _load_watchlists(path: Path) -> dict[str, dict[str, list[str]]]:
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    themes = payload.get("themes", {})
    rules = {}
    for theme, config in themes.items():
        rules[theme.lower()] = {
            "symbols": config.get("seed_tickers", []) or [],
            "keywords": config.get("keywords", []) or [],
        }
    return rules


def _select_weights(
    config: dict[str, Any], *, opportunistic: bool
) -> dict[str, float]:
    weights_cfg = (
        config["scoring"]["weights_opportunistic"]
        if opportunistic
        else config["scoring"]["weights_conservative"]
    )
    weights = {
        "trend": 0.22,
        "momentum": weights_cfg.get("momentum", 0.2),
        "liquidity": weights_cfg.get("liquidity", 0.2),
        "quality": 0.14,
        "vol_penalty": weights_cfg.get("volatility", -0.15),
        "dd_penalty": weights_cfg.get("drawdown", -0.1),
        "tail_penalty": 0.05,
        "attention": 0.05,
        "theme_bonus": config["scoring"].get("theme_bonus", 0.05),
        "volume_missing_penalty": 0.05,
    }
    return weights


def _configure_logging(logging_path: Path, run_dir: Path) -> logging.Logger:
    if not logging_path.exists():
        logger = logging.getLogger("market_app")
        logger.setLevel(logging.INFO)
        logger.handlers.clear()
        run_log = run_dir / "run.log"
        formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        file_handler = logging.FileHandler(run_log, encoding="utf-8")
        file_handler.setFormatter(formatter)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)
        return logger
    config = yaml.safe_load(logging_path.read_text(encoding="utf-8"))
    for handler in config.get("handlers", {}).values():
        if "filename" in handler:
            handler["filename"] = str(run_dir / "run.log")
    logging.config.dictConfig(config)
    return logging.getLogger("market_app")


def _dependency_versions() -> dict[str, str]:
    try:
        import importlib.metadata as metadata
    except ImportError:  # pragma: no cover - fallback
        import importlib_metadata as metadata  # type: ignore

    versions = {}
    for name in ("pandas", "numpy", "pyyaml"):
        try:
            versions[name] = metadata.version(name)
        except metadata.PackageNotFoundError:
            versions[name] = "unknown"
    return versions


def _deterministic_run_id(
    *,
    config_hash: str,
    watchlist_path: Path,
    variant: str,
    top_n: int,
) -> str:
    watchlist_hash = _watchlist_hash(watchlist_path)
    payload = json.dumps(
        {
            "config_hash": config_hash,
            "watchlist_hash": watchlist_hash,
            "variant": variant,
            "top_n": top_n,
        },
        sort_keys=True,
    )
    digest = hash_text(payload)[:8]
    return f"run_{digest}"


def _watchlist_hash(watchlist_path: Path) -> str:
    if watchlist_path.exists():
        return hash_file(watchlist_path)
    return hash_text(watchlist_path.as_posix())


def _collect_dataset_paths(config: dict[str, Any], base_dir: Path) -> list[Path]:
    paths: list[Path] = []
    watchlist_path = Path(config["paths"]["watchlist_file"])
    if not watchlist_path.is_absolute():
        watchlist_path = (base_dir / watchlist_path).resolve()
    paths.append(watchlist_path)
    for entry in config.get("macro", {}).get("series", []):
        rel_path = entry.get("file")
        if not rel_path:
            continue
        macro_path = base_dir / config["paths"]["data_dir"] / rel_path
        paths.append(macro_path.resolve())
    return paths


def _write_digest(
    *,
    run_dir: Path,
    config_result: ConfigResult,
    git_sha: str | None,
    offline: bool,
    variant: str,
    top_n: int,
    watchlist_path: Path,
    dataset_paths: list[Path],
) -> dict[str, Any]:
    stable_outputs = {}
    for name in ("eligible.csv", "scored.csv", "features.csv", "classified.csv", "universe.csv"):
        path = run_dir / name
        if path.exists():
            stable_outputs[name] = {"sha256": hash_file(path), "bytes": path.stat().st_size}
    datasets = []
    for path in dataset_paths:
        if path.exists():
            datasets.append({"path": str(path), "sha256": hash_file(path)})
    payload = {
        "config_hash": config_result.config_hash,
        "git_sha": git_sha,
        "offline": offline,
        "variant": variant,
        "top_n": top_n,
        "watchlist_hash": _watchlist_hash(watchlist_path),
        "outputs": stable_outputs,
        "datasets": datasets,
    }
    (run_dir / "digest.json").write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return payload


def _build_classified(universe_df, theme_rules):
    rows = []
    for _, row in universe_df.iterrows():
        symbol = row["symbol"]
        name = row.get("name") or symbol
        tags, confidence, _unknown = tag_themes(symbol, name, theme_rules)
        theme = tags[0] if tags else "Unclassified"
        evidence = {
            "matched_symbols": [symbol] if symbol.upper() in {s.upper() for s in theme_rules.get(theme.lower(), {}).get("symbols", [])} else [],
            "matched_keywords": [
                keyword
                for keyword in theme_rules.get(theme.lower(), {}).get("keywords", [])
                if keyword.lower() in (name or "").lower()
            ],
        }
        rows.append(
            {
                "symbol": symbol,
                "theme": theme,
                "confidence": confidence,
                "evidence": json.dumps(evidence, sort_keys=True),
            }
        )
    return pd.DataFrame(rows)


def _build_regime(config: dict[str, Any], base_dir: Path, run_timestamp: str) -> dict[str, Any]:
    macro_cfg = config.get("macro", {})
    indicators = []
    series = macro_cfg.get("series", [])
    lookback_years = int(macro_cfg.get("lookback_years", 5))
    thresholds = macro_cfg.get("zscore_thresholds", {"low": -1.0, "high": 1.0})
    for entry in series:
        name = entry.get("name")
        rel_path = entry.get("file")
        if not name or not rel_path:
            continue
        path = (base_dir / config["paths"]["data_dir"] / rel_path).resolve()
        if not path.exists():
            continue
        df = pd.read_csv(path)
        cols = {c.lower(): c for c in df.columns}
        date_col = cols.get("date")
        value_col = cols.get("value") or cols.get("close")
        if not date_col or not value_col:
            continue
        series_df = pd.DataFrame(
            {
                "date": pd.to_datetime(df[date_col], errors="coerce"),
                "value": pd.to_numeric(df[value_col], errors="coerce"),
            }
        ).dropna()
        if series_df.empty:
            continue
        series_df = series_df.sort_values("date").reset_index(drop=True)
        cutoff = series_df["date"].iloc[-1] - pd.DateOffset(years=lookback_years)
        window = series_df[series_df["date"] >= cutoff]["value"]
        if window.empty:
            window = series_df["value"]
        mean = float(window.mean())
        std = float(window.std(ddof=0)) or 1.0
        latest = float(series_df["value"].iloc[-1])
        zscore = float((latest - mean) / std)
        label = "neutral"
        if zscore <= thresholds.get("low", -1.0):
            label = "low"
        if zscore >= thresholds.get("high", 1.0):
            label = "high"
        indicators.append(
            {
                "name": name,
                "latest_value": latest,
                "zscore": zscore,
                "label": label,
            }
        )
    indicators = sorted(indicators, key=lambda item: item["name"])
    regime_label = "Unknown"
    if indicators:
        labels = {indicator["label"] for indicator in indicators}
        if "high" in labels:
            regime_label = "Expansion"
        elif "low" in labels:
            regime_label = "Contraction"
        else:
            regime_label = "Neutral"
    return {
        "timestamp": run_timestamp,
        "indicators": indicators,
        "regime_label": regime_label,
    }


def _apply_regime_overlay(df: pd.DataFrame, regime: dict[str, Any], overlay: dict[str, Any]) -> pd.Series:
    label = regime.get("regime_label", "Unknown").lower()
    adjusted = df["score_core"].copy()
    if label == "contraction":
        penalty = overlay.get("contraction", {}).get("volatility_penalty", 1.0)
        adjusted = adjusted - df["volatility60"].abs() * (penalty - 1.0)
    if label == "expansion":
        bonus = overlay.get("expansion", {}).get("return_bonus", 1.0)
        adjusted = adjusted + df["return_6m"] * (bonus - 1.0)
    return adjusted


def _prepare_features(scored: pd.DataFrame) -> pd.DataFrame:
    df = scored.copy()
    df = df.rename(
        columns={
            "ret_1m": "return_1m",
            "ret_3m": "return_3m",
            "ret_6m": "return_6m",
            "ret_12m": "return_12m",
            "vol20_ann": "volatility20",
            "vol60_ann": "volatility60",
            "downside_vol_ann": "downside_vol20",
            "zero_volume_frac": "zero_volume_fraction",
        }
    )
    df["missing_data"] = df["missing_day_rate"] > 0.2
    df["stale_data"] = df["stale_price_flag"] > 0
    df["split_suspect"] = df["corp_action_suspect"] > 0
    for feature in REQUIRED_FEATURES:
        if feature not in df.columns:
            df[feature] = np.nan
    return df


def _build_scored(
    scored: pd.DataFrame,
    classified: pd.DataFrame,
    config: dict[str, Any],
    regime: dict[str, Any],
    top_n: int,
    variant: str,
) -> pd.DataFrame:
    df = _prepare_features(scored)
    df = df.merge(classified[["symbol", "theme", "confidence"]], on="symbol", how="left")
    base_score = df.get("raw_score", 0.0)
    variant_bump = df["return_1m"].fillna(0.0) if variant == "opportunistic" else df["return_6m"].fillna(0.0)
    df["score_core"] = base_score + 0.01 * variant_bump
    df["score_regime_adjusted"] = _apply_regime_overlay(
        df, regime, config.get("regime_overlay", {})
    )
    df["theme_bonus"] = df["confidence"].fillna(0.0) * config["scoring"].get("theme_bonus", 0.05)
    df["total_score"] = df["score_regime_adjusted"] + df["theme_bonus"]
    if variant == "opportunistic":
        df["total_score"] = df["total_score"] + 0.0001
    df["flags"] = build_risk_flags(
        df,
        {
            "high_volatility": config["scoring"]["gates"].get("high_volatility", 1.2),
            "large_drawdown": config["scoring"]["gates"].get("large_drawdown", -0.25),
            "illiquid_adv20_dollar": config["scoring"]["gates"].get("min_adv20_dollar", 1_000_000),
            "zero_volume_fraction": config["scoring"]["gates"].get("max_zero_volume_fraction", 0.1),
        },
    )
    df = df.sort_values("symbol").reset_index(drop=True)
    return df


def _resolve_logging_path(config_dir: Path, base_dir: Path) -> Path:
    candidate = config_dir / "logging.yaml"
    if candidate.exists():
        return candidate
    fallback = base_dir / "config" / "logging.yaml"
    return fallback


def _resolve_runs_dir(
    args: argparse.Namespace, blueprint: dict[str, Any], base_dir: Path
) -> Path:
    runs_dir = args.runs_dir or blueprint["paths"]["output_dir"]
    runs_path = Path(runs_dir)
    if not runs_path.is_absolute():
        runs_path = (base_dir / runs_path).resolve()
    return runs_path


def _run_doctor(args: argparse.Namespace) -> int:
    config_path = Path(args.config).expanduser().resolve()
    errors = []
    warnings = []

    if not config_path.exists():
        errors.append(
            (
                "Config missing",
                f"Config file not found: {config_path}",
                [f"Create or copy a config file to {config_path}."],
            )
        )
        _print_doctor_summary(errors, warnings)
        return 2

    try:
        config_result = load_config(config_path)
    except Exception as exc:  # noqa: BLE001
        errors.append(
            (
                "Config invalid",
                f"Failed to parse config: {exc}",
                [f"Fix the YAML in {config_path}."],
            )
        )
        _print_doctor_summary(errors, warnings)
        return 2

    config = config_result.config
    config_dir = config_path.parent
    repo_root = find_repo_root(config_dir)
    base_dir = repo_root if repo_root.exists() else config_dir
    offline = bool(config.get("offline", True) or args.offline)

    def _resolve(path_value: str) -> Path:
        path = Path(path_value)
        return path if path.is_absolute() else (base_dir / path).resolve()

    watchlist_path = _resolve(config["paths"]["watchlist_file"])
    if not watchlist_path.exists():
        errors.append(
            (
                "Watchlist missing",
                f"Expected watchlist at {watchlist_path}.",
                ["Set paths.watchlist_file in config.yaml.", "Add a watchlist file to the path above."],
            )
        )

    data_dir = _resolve(config["paths"]["data_dir"])
    if not data_dir.exists():
        errors.append(
            (
                "Data directory missing",
                f"Expected data directory at {data_dir}.",
                ["Set paths.data_dir in config.yaml.", "Create the directory or update the path."],
            )
        )

    output_dir = _resolve(config["paths"]["output_dir"])
    if not output_dir.exists():
        output_dir.mkdir(parents=True, exist_ok=True)
        warnings.append(
            (
                "Output directory created",
                f"Created output directory at {output_dir}.",
                ["Ensure this path is on local storage with enough space."],
            )
        )

    nasdaq_dir = config["paths"].get("nasdaq_daily_dir") or ""
    nasdaq_path = _resolve(nasdaq_dir) if nasdaq_dir else None
    if offline:
        if not nasdaq_path or not nasdaq_path.exists():
            errors.append(
                (
                    "NASDAQ daily data missing",
                    "Offline mode requires local OHLCV data.",
                    [
                        "Set paths.nasdaq_daily_dir in config.yaml.",
                        "Point it at tests/fixtures/ohlcv for fixtures.",
                    ],
                )
            )
    elif nasdaq_path and not nasdaq_path.exists():
        warnings.append(
            (
                "NASDAQ daily data missing",
                f"Configured path not found: {nasdaq_path}.",
                ["Provide data or run with --offline for fixture-only runs."],
            )
        )

    macro_series = config.get("macro", {}).get("series", [])
    for entry in macro_series:
        rel_path = entry.get("file")
        if not rel_path:
            continue
        macro_path = data_dir / rel_path
        if not macro_path.exists():
            warnings.append(
                (
                    "Macro series missing",
                    f"Missing macro series file: {macro_path}.",
                    ["Add the macro CSV or remove it from config.yaml."],
                )
            )

    watchlists_yaml = config_dir / "watchlists.yaml"
    if not watchlists_yaml.exists():
        watchlists_yaml = base_dir / "watchlists.yaml"
    if not watchlists_yaml.exists():
        warnings.append(
            (
                "Theme watchlists missing",
                f"watchlists.yaml not found at {watchlists_yaml}.",
                ["Add a watchlists.yaml file to enable theme tagging."],
            )
        )

    _print_doctor_summary(errors, warnings)
    return 2 if errors else 0


def _print_doctor_summary(
    errors: list[tuple[str, str, list[str]]], warnings: list[tuple[str, str, list[str]]]
) -> None:
    print("[doctor] Market App config diagnostics")
    for title, detail, fix_steps in errors:
        print(f"[ERROR] {title}\n  {detail}")
        for step in fix_steps:
            print(f"  - {step}")
    for title, detail, fix_steps in warnings:
        print(f"[WARN] {title}\n  {detail}")
        for step in fix_steps:
            print(f"  - {step}")
    summary = "PASS" if not errors else "FAIL"
    print(f"[doctor] Summary: {summary} (errors={len(errors)}, warnings={len(warnings)})")


def run_cli(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.command == "doctor":
        return _run_doctor(args)

    config_path = Path(args.config).expanduser().resolve()
    config_dir = config_path.parent
    repo_root = find_repo_root(config_dir)
    base_dir = repo_root if repo_root.exists() else config_dir
    config_result = load_config(config_path)
    blueprint = config_result.config

    offline = bool(blueprint.get("offline", True) or args.offline)
    watchlists_path = config_dir / "watchlists.yaml"
    theme_rules = _load_watchlists(watchlists_path)
    variant = "opportunistic" if args.opportunistic else "conservative"
    weights = _select_weights(blueprint, opportunistic=bool(args.opportunistic))
    watchlist_path = Path(blueprint["paths"]["watchlist_file"])
    if not watchlist_path.is_absolute():
        watchlist_path = (base_dir / watchlist_path).resolve()
    engine_config = map_to_engine_config(
        blueprint=blueprint,
        config_hash=config_result.config_hash,
        base_dir=base_dir,
        theme_rules=theme_rules,
        weights=weights,
    )
    engine_config["data"]["offline_mode"] = offline
    top_n = args.top_n or blueprint["run"]["top_n"]
    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    run_id = args.run_id or _deterministic_run_id(
        config_hash=config_result.config_hash,
        watchlist_path=watchlist_path,
        variant=variant,
        top_n=top_n,
    )

    runs_dir = _resolve_runs_dir(args, blueprint, base_dir)
    run_dir = (runs_dir / run_id).resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    logger = _configure_logging(_resolve_logging_path(config_dir, base_dir), run_dir)
    logger.info("Starting blueprint-compatible run")

    pipeline_result: PipelineResult = run_pipeline(
        engine_config,
        base_dir=base_dir,
        mode="watchlist",
        watchlist_path=watchlist_path,
        output_dir=run_dir,
        run_id=run_id,
        logger=logger,
        write_legacy_outputs=True,
        run_timestamp=run_timestamp,
    )
    universe = pipeline_result.universe_df.sort_values("symbol").reset_index(drop=True)
    classified = _build_classified(universe, theme_rules)
    features = _prepare_features(pipeline_result.scored_df)
    features = normalize_features(features, REQUIRED_FEATURES)
    eligible_df, eligible_mask = apply_blueprint_gates(
        features, blueprint["scoring"]["gates"]
    )
    regime = _build_regime(blueprint, base_dir, run_timestamp)
    forward_summary = compute_forward_outcome_summary(
        symbols=universe["symbol"].tolist(),
        provider=pipeline_result.provider,
        horizons=[21, 63],
    )
    blueprint_dict = dict(blueprint)
    blueprint_dict["forward_summary"] = forward_summary
    scored = _build_scored(features, classified, blueprint_dict, regime, top_n, variant)
    scored["forward_outcome_summary"] = build_forward_outcome_column(
        scored, forward_summary, top_n
    )

    write_csv(universe, run_dir / "universe.csv")
    write_csv(classified, run_dir / "classified.csv")
    write_csv(features, run_dir / "features.csv")
    write_csv(eligible_df, run_dir / "eligible.csv")
    write_csv(scored, run_dir / "scored.csv")
    (run_dir / "regime.json").write_text(json.dumps(regime, indent=2, sort_keys=True))

    build_report(
        run_dir / "report.md",
        run_id=run_id,
        run_timestamp=run_timestamp,
        scored=scored,
        regime=regime,
        forward_summary=forward_summary,
        top_n=top_n,
    )
    dataset_paths = _collect_dataset_paths(blueprint, base_dir)
    git_sha = resolve_git_commit(base_dir)
    manifest = build_manifest(
        run_id=run_id,
        run_timestamp=run_timestamp,
        config_hash=config_result.config_hash,
        git_sha=git_sha,
        offline=offline,
        output_dir=run_dir,
        config_path=config_path,
        dataset_paths=dataset_paths,
        dependency_versions=_dependency_versions(),
    )
    (run_dir / "manifest.json").write_text(manifest.to_json(), encoding="utf-8")
    _write_digest(
        run_dir=run_dir,
        config_result=config_result,
        git_sha=git_sha,
        offline=offline,
        variant=variant,
        top_n=top_n,
        watchlist_path=watchlist_path,
        dataset_paths=dataset_paths,
    )
    logger.info("Completed blueprint-compatible run")
    print(f"[done] Run artifacts: {run_dir}")
    return 0


def main() -> int:
    try:
        return run_cli()
    except Exception as exc:  # noqa: BLE001
        print(f"[error] {exc}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
