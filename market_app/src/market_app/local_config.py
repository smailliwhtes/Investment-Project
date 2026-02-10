from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


DEFAULT_CONFIG: dict[str, Any] = {
    "schema_version": "v2",
    "offline": True,
    "online": False,
    "run": {
        "top_n": 50,
        "max_symbols": None,
        "as_of_date": None,
    },
    "paths": {
        "symbols_dir": "",
        "ohlcv_dir": "",
        "macro_dir": "",
        "output_dir": "./outputs/runs",
        "training_output_dir": "./outputs/training",
        "model_dir": "./models",
        "geopolitics_dir": "",
        "watchlists_file": "./config/watchlists.yaml",
        "logging_config": "./config/logging.yaml",
        "sources_config": "./config/sources.yaml",
    },
    "filters": {
        "include_warrants": False,
        "include_rights": False,
        "include_units": False,
        "include_preferreds": False,
    },
    "gates": {
        "min_history_days": 252,
        "min_adv20_usd": 1_000_000,
        "price_floor": 1.0,
        "zero_volume_max_frac": 0.2,
        "max_lag_days": 5,
    },
    "risk_thresholds": {
        "extreme_volatility": 0.6,
        "deep_drawdown": -0.4,
        "tail_risk": -0.2,
    },
    "scoring": {
        "weights": {
            "trend": 0.35,
            "liquidity": 0.2,
            "risk_penalty": 0.2,
            "drawdown_penalty": 0.15,
            "theme_purity": 0.1,
            "volume_missing_penalty": 0.05,
        }
    },
    "themes": {
        "defense": {
            "keywords": ["defense", "aerospace", "missile", "munitions", "tactical", "radar"],
            "tickers": [],
        },
        "strategic_tech": {
            "keywords": ["semiconductor", "chip", "ai", "artificial intelligence", "quantum"],
            "tickers": [],
        },
        "critical_materials": {
            "keywords": ["lithium", "uranium", "copper", "rare earth", "nickel", "graphite"],
            "tickers": [],
        },
    },
    "report": {
        "top_n": 50,
    },
}


ENV_OVERRIDES = {
    "MARKET_APP_OHLCV_DIR": ("paths", "ohlcv_dir"),
    "MARKET_APP_SYMBOLS_DIR": ("paths", "symbols_dir"),
    "MARKET_APP_OUTPUT_DIR": ("paths", "output_dir"),
    "MARKET_APP_GDELT_DIR": ("paths", "geopolitics_dir"),
    "MARKET_APP_MODEL_DIR": ("paths", "model_dir"),
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


def _clone_config(config: dict[str, Any]) -> dict[str, Any]:
    return json.loads(json.dumps(config, default=str))


def _apply_env_overrides(config: dict[str, Any]) -> dict[str, Any]:
    updated = _clone_config(config)
    for env_key, path in ENV_OVERRIDES.items():
        value = os.getenv(env_key)
        if value:
            target = updated
            for key in path[:-1]:
                target = target.setdefault(key, {})
            target[path[-1]] = value
    offline_env = os.getenv("OFFLINE_MODE")
    if offline_env is not None:
        offline = offline_env.strip().lower() in {"1", "true", "yes"}
        updated["offline"] = offline
        updated["online"] = not offline
    online_env = os.getenv("MARKET_APP_ONLINE")
    if online_env is not None:
        online = online_env.strip().lower() in {"1", "true", "yes"}
        updated["online"] = online
        updated["offline"] = not online
    return updated


def _apply_cli_overrides(config: dict[str, Any], cli_overrides: dict[str, Any]) -> dict[str, Any]:
    updated = _clone_config(config)
    for key, value in cli_overrides.items():
        if value is None:
            continue
        if key == "offline":
            updated["offline"] = bool(value)
            updated["online"] = not bool(value)
        elif key == "online":
            updated["online"] = bool(value)
            updated["offline"] = not bool(value)
        elif key in {"symbols_dir", "ohlcv_dir", "output_dir", "geopolitics_dir", "model_dir"}:
            updated["paths"][key] = value
        elif key == "training_output_dir":
            updated["paths"]["training_output_dir"] = value
        elif key == "top_n":
            updated.setdefault("run", {})["top_n"] = int(value)
        elif key == "run" and isinstance(value, dict):
            updated["run"] = _deep_merge(updated.get("run", {}), value)
        else:
            updated[key] = value
    return updated


def _resolve_paths(config: dict[str, Any], base_dir: Path) -> dict[str, Any]:
    resolved = _clone_config(config)
    for key, value in resolved.get("paths", {}).items():
        if not isinstance(value, str) or not value:
            continue
        expanded = Path(os.path.expandvars(os.path.expanduser(value)))
        if expanded.is_absolute():
            resolved["paths"][key] = str(expanded)
        else:
            resolved["paths"][key] = str((base_dir / expanded).resolve())
    return resolved


def _validate_config(config: dict[str, Any]) -> None:
    if "paths" not in config:
        raise ValueError("Missing paths section in config.")
    if "run" not in config:
        raise ValueError("Missing run section in config.")


def load_config(path: Path, *, cli_overrides: dict[str, Any] | None = None) -> ConfigResult:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    config = _deep_merge(DEFAULT_CONFIG, raw)
    config = _apply_env_overrides(config)
    if cli_overrides:
        config = _apply_cli_overrides(config, cli_overrides)
    if config.get("online"):
        config["offline"] = False
    config = _resolve_paths(config, path.parent)
    _validate_config(config)
    config_hash = _hash_config(config)
    return ConfigResult(config=config, config_hash=config_hash)
