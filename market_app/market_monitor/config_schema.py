import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

DEFAULT_CONFIG: Dict[str, Any] = {
    "run": {
        "asof_timezone": "America/New_York",
        "max_symbols_per_run": 200,
        "batch_strategy": "round_robin",
    },
    "universe": {
        "mode": "watchlist",
        "include_etfs": True,
        "exclude_test_issues": True,
        "allowed_security_types": ["COMMON", "ETF"],
        "allowed_currencies": ["USD"],
    },
    "staging": {
        "stage1_micro_days": 7,
        "stage2_short_days": 60,
        "stage3_deep_days": 600,
        "history_min_days": 252,
    },
    "data": {
        "max_cache_age_days": 2,
        "max_workers": 6,
        "provider": "stooq",
        "fallback_chain": ["stooq"],
        "budget": {
            "twelvedata": {"max_requests_per_run": 600},
            "alphavantage": {"max_requests_per_run": 80},
            "finnhub": {"max_requests_per_run": 200},
        },
        "throttling": {
            "base_delay_s": 0.3,
            "max_retries": 3,
        },
    },
    "gates": {
        "price_max": 10.0,
        "min_adv20_dollar": 1_000_000,
        "max_zero_volume_frac": 0.10,
    },
    "score": {
        "weights": {
            "trend": 0.25,
            "momentum": 0.25,
            "liquidity": 0.15,
            "vol_penalty": 0.15,
            "dd_penalty": 0.10,
            "tail_penalty": 0.05,
            "theme_bonus": 0.05,
        }
    },
    "themes": {
        "defense": {"symbols": [], "keywords": ["defense", "aero", "missile"]},
        "tech": {"symbols": [], "keywords": ["semiconductor", "ai", "cloud"]},
        "metals": {"symbols": [], "keywords": ["lithium", "copper", "uranium"]},
    },
    "paths": {
        "watchlist_file": "inputs/watchlist.txt",
        "universe_csv": "data/universe/universe.csv",
        "cache_dir": "data/cache",
        "state_file": "data/state/batch_state.json",
        "outputs_dir": "outputs",
        "logs_dir": "outputs/logs",
    },
}


class ConfigError(ValueError):
    pass


@dataclass
class ConfigResult:
    config: Dict[str, Any]
    config_hash: str


def _deep_merge(base: Dict[str, Any], patch: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def _hash_config(config: Dict[str, Any]) -> str:
    payload = json.dumps(config, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def load_config(path: Path, overrides: Optional[Dict[str, Any]] = None) -> ConfigResult:
    if not path.exists():
        raise ConfigError(f"Config file not found at {path}. Create one with 'python -m market_monitor init-config --out {path}'.")
    try:
        config_data = json.loads(path.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError as exc:
        raise ConfigError(f"Config file is not valid JSON: {exc}") from exc

    config = _deep_merge(DEFAULT_CONFIG, config_data)
    if overrides:
        config = _deep_merge(config, overrides)

    _validate_config(config)
    return ConfigResult(config=config, config_hash=_hash_config(config))


def write_default_config(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(DEFAULT_CONFIG, indent=2), encoding="utf-8")


def _require(config: Dict[str, Any], keys: List[str]) -> None:
    node = config
    for key in keys:
        if key not in node:
            raise ConfigError(f"Config missing {'.'.join(keys)}. Set it in config.json or pass a CLI override.")
        node = node[key]


def _validate_config(config: Dict[str, Any]) -> None:
    required_paths = [
        ["paths", "watchlist_file"],
        ["paths", "outputs_dir"],
        ["paths", "cache_dir"],
    ]
    for path_keys in required_paths:
        _require(config, path_keys)

    if config["data"]["provider"] not in {"stooq", "twelvedata", "alphavantage", "finnhub"}:
        raise ConfigError("data.provider must be one of stooq, twelvedata, alphavantage, finnhub.")

    if config["staging"]["stage1_micro_days"] != 7:
        pass

    for stage_key in ["stage1_micro_days", "stage2_short_days", "stage3_deep_days", "history_min_days"]:
        if config["staging"][stage_key] <= 0:
            raise ConfigError(f"staging.{stage_key} must be > 0.")

    if config["gates"]["price_max"] <= 0:
        raise ConfigError("gates.price_max must be > 0.")

    if config["gates"]["max_zero_volume_frac"] < 0:
        raise ConfigError("gates.max_zero_volume_frac must be >= 0.")

    if config["data"]["max_workers"] <= 0:
        raise ConfigError("data.max_workers must be > 0.")
