from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from market_monitor.config_schema import DEFAULT_CONFIG

DEFAULT_BLUEPRINT_CONFIG: dict[str, Any] = {
    "offline": True,
    "run": {"top_n": 15},
    "determinism": {
        "as_of_date": None,
        "now_utc": None,
        "allowed_vary_columns": {
            "global": [],
        },
        "allowed_vary_json_keys": {
            "global": [],
        },
    },
    "paths": {
        "data_dir": "./data",
        "output_dir": "./outputs/runs",
        "model_dir": "./models",
        "text_corpus_dir": "./data/text_corpus",
        "watchlist_file": "./watchlists/watchlist_core.csv",
        "nasdaq_daily_dir": "",
    },
    "macro": {
        "lookback_years": 5,
        "zscore_thresholds": {"low": -1.0, "high": 1.0},
        "series": [],
    },
    "themes": {"theme_weights": {"etf": 0.6, "keywords": 0.3, "sector": 0.1}},
    "scoring": {
        "gates": {
            "min_price_above_sma200": True,
            "min_adv20_dollar": 5_000_000,
            "max_zero_volume_fraction": 0.05,
            "min_market_cap": None,
            "max_missing_day_rate": 0.2,
        },
        "weights_conservative": {
            "return_6m": 0.25,
            "return_12m": 0.25,
            "momentum": 0.15,
            "volatility": -0.15,
            "drawdown": -0.1,
            "liquidity": 0.2,
        },
        "weights_opportunistic": {
            "return_1m": 0.3,
            "return_3m": 0.3,
            "momentum": 0.2,
            "volatility": -0.1,
            "drawdown": -0.1,
            "liquidity": 0.1,
        },
        "theme_bonus": 0.05,
    },
    "regime_overlay": {
        "contraction": {"volatility_penalty": 1.2},
        "expansion": {"return_bonus": 1.2},
    },
    "report": {"include_scenario_section": True, "include_analog_section": True},
}


@dataclass(frozen=True)
class ConfigResult:
    config: dict[str, Any]
    config_hash: str


def _hash_config(data: dict[str, Any]) -> str:
    payload = json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return __import__("hashlib").sha256(payload.encode("utf-8")).hexdigest()


def _deep_merge(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    result = dict(base)
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_config(path: Path) -> ConfigResult:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    env_offline = os.getenv("OFFLINE_MODE")
    if env_offline is not None:
        raw["offline"] = env_offline.strip().lower() in {"1", "true", "yes"}
    env_watchlist = os.getenv("MARKET_APP_WATCHLIST_FILE")
    if env_watchlist:
        raw.setdefault("paths", {})["watchlist_file"] = env_watchlist
    config = _deep_merge(DEFAULT_BLUEPRINT_CONFIG, raw)
    _validate_config(config)
    config_hash = _hash_config(config)
    return ConfigResult(config=config, config_hash=config_hash)


def _validate_config(config: dict[str, Any]) -> None:
    required = ["run", "paths", "scoring", "themes", "macro", "regime_overlay"]
    for key in required:
        if key not in config:
            raise ValueError(f"Missing required config section: {key}")
    if "top_n" not in config["run"]:
        raise ValueError("run.top_n is required")
    if "output_dir" not in config["paths"]:
        raise ValueError("paths.output_dir is required")


def map_to_engine_config(
    *,
    blueprint: dict[str, Any],
    config_hash: str,
    base_dir: Path,
    theme_rules: dict[str, dict[str, list[str]]],
    weights: dict[str, float],
    as_of_date: str | None = None,
    now_utc: str | None = None,
) -> dict[str, Any]:
    engine = json.loads(json.dumps(DEFAULT_CONFIG))
    engine["data"]["offline_mode"] = bool(blueprint.get("offline", True))
    nasdaq_dir = blueprint.get("paths", {}).get("nasdaq_daily_dir")
    if nasdaq_dir:
        engine["data"]["paths"]["nasdaq_daily_dir"] = nasdaq_dir
    else:
        env_nasdaq = os.getenv("MARKET_APP_NASDAQ_DAILY_DIR")
        if env_nasdaq:
            engine["data"]["paths"]["nasdaq_daily_dir"] = env_nasdaq
    engine["paths"]["watchlist_file"] = blueprint["paths"]["watchlist_file"]
    engine["paths"]["outputs_dir"] = str(Path(blueprint["paths"]["output_dir"]))
    engine["paths"]["logs_dir"] = str(Path(blueprint["paths"]["output_dir"]) / "logs")
    engine["score"]["weights"] = weights
    engine["themes"] = theme_rules
    engine["run"]["max_symbols_per_run"] = blueprint["run"]["top_n"]
    engine["config_hash"] = config_hash
    if as_of_date:
        engine["pipeline"]["asof_default"] = as_of_date
    if now_utc:
        engine["pipeline"]["now_utc"] = now_utc
    return engine
