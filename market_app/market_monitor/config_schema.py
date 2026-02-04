import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONFIG: dict[str, Any] = {
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
        "provider": "nasdaq_daily",
        "fallback_chain": [],
        "offline_mode": True,
        "budget": {
            "twelvedata": {"max_requests_per_run": 600},
            "alphavantage": {"max_requests_per_run": 80},
            "finnhub": {"max_requests_per_run": 200},
        },
        "throttling": {
            "base_delay_s": 0.3,
            "max_retries": 3,
            "jitter_s": 0.2,
        },
        "paths": {
            "market_app_data_root": "",
            "nasdaq_daily_dir": "",
            "silver_prices_dir": "",
        },
    },
    "data_roots": {
        "ohlcv_dir": "",
        "gdelt_raw_dir": "",
        "gdelt_dir": "",
        "outputs_dir": "outputs",
    },
    "gates": {
        "price_min": None,
        "price_max": None,
        "risk_flags": {
            "penny_like": 1.0,
            "illiquid_adv20_dollar": 1_000_000,
            "short_history_days": 252,
            "extreme_vol_annual": 1.2,
            "gap_atr": 2.0,
            "zero_volume_frac": 0.2,
            "missing_day_rate": 0.2,
        },
    },
    "corpus": {
        "root_dir": "",
        "gdelt_conflict_dir": "",
        "gdelt_events_raw_dir": "gdelt_events_raw",
        "features": {
            "rootcode_top_n": 8,
            "country_top_k": 8,
        },
        "analogs": {
            "top_n": 8,
            "spike_stddev": 2.0,
            "forward_days": [1, 5, 20],
        },
    },
    "evaluation": {
        "symbols": [],
        "lookback_days": 252,
        "forward_return_days": 5,
        "classification_threshold": 0.02,
        "walk_forward_folds": 3,
        "min_history_days": 252,
        "mode": "both",
    },
    "score": {
        "weights": {
            "trend": 0.22,
            "momentum": 0.2,
            "liquidity": 0.12,
            "quality": 0.14,
            "vol_penalty": 0.12,
            "dd_penalty": 0.1,
            "tail_penalty": 0.05,
            "attention": 0.05,
            "theme_bonus": 0.05,
            "volume_missing_penalty": 0.05,
        }
    },
    "prediction": {
        "enabled": False,
        "min_history_days": 252,
        "lookback_days": 252,
        "forward_return_days": 20,
        "forward_drawdown_days": 60,
        "drawdown_threshold": -0.2,
        "walk_forward_folds": 3,
        "embargo_days": 60,
    },
    "themes": {
        "defense": {"symbols": [], "keywords": ["defense", "aero", "missile"]},
        "tech": {"symbols": [], "keywords": ["semiconductor", "ai", "cloud"]},
        "metals": {"symbols": [], "keywords": ["lithium", "copper", "uranium"]},
    },
    "paths": {
        "watchlist_file": "watchlists/watchlist_core.csv",
        "universe_csv": "data/universe/universe.csv",
        "cache_dir": "data/cache",
        "state_file": "data/state/batch_state.json",
        "outputs_dir": "outputs",
        "logs_dir": "outputs/logs",
        "ohlcv_raw_dir": "data/ohlcv_raw",
        "ohlcv_daily_dir": "data/ohlcv_daily",
        "exogenous_daily_dir": "data/exogenous/daily_features",
    },
    "pipeline": {
        "auto_normalize_ohlcv": True,
        "include_raw_exogenous_same_day": False,
        "asof_default": None,
        "benchmarks": ["SPY", "QQQ", "IWM", "TLT", "GLD"],
    },
    "scoring": {
        "minimum_history_days": 252,
        "price_floor": 1.0,
        "average_dollar_volume_floor": 1_000_000.0,
        "max_vol_20d_cap": None,
        "base_score": 5.0,
        "weight_momentum": 2.0,
        "weight_trend": 1.5,
        "weight_stability": 1.2,
        "weight_liquidity": 0.8,
        "regime_risk_off_penalty": 1.0,
        "regime_risk_on_bonus": 0.3,
        "vol_target": 0.4,
        "liquidity_target": 1_000_000.0,
    },
    "bulk": {
        "paths": {
            "raw_dir": "data/raw",
            "curated_dir": "data/curated",
            "manifest_dir": "data/manifests",
        },
        "sources": [
            {
                "name": "stooq",
                "base_url": "https://stooq.pl/q/d/l",
                "symbol_template": "?s={symbol}.us&i=d",
                "file_extension": "",
            },
            {
                "name": "treasury_yield_curve",
                "base_url": "https://home.treasury.gov/resource-center/data-chart-center/interest-rates",
                "static_path": "/DailyTreasuryYieldCurveRateData.csv",
                "file_extension": "",
            },
        ],
    },
}


class ConfigError(ValueError):
    pass


@dataclass
class ConfigResult:
    config: dict[str, Any]
    config_hash: str


def _deep_merge(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def _hash_config(config: dict[str, Any]) -> str:
    payload = json.dumps(config, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _parse_bool(value: str | None) -> bool | None:
    if value is None:
        return None
    value = value.strip().lower()
    if value in {"1", "true", "yes", "y"}:
        return True
    if value in {"0", "false", "no", "n"}:
        return False
    return None


def _load_env_overrides() -> dict[str, Any]:
    overrides: dict[str, Any] = {"data": {"paths": {}}, "corpus": {}, "paths": {}, "data_roots": {}}
    root = os.getenv("MARKET_APP_DATA_ROOT")
    ohlcv = (
        os.getenv("MARKET_APP_OHLCV_DIR")
        or os.getenv("MARKET_APP_NASDAQ_DAILY_DIR")
        or os.getenv("NASDAQ_DAILY_DIR")
    )
    silver = (
        os.getenv("MARKET_APP_SILVER_PRICES_DIR")
        or os.getenv("SILVER_PRICES_DIR")
        or os.getenv("SILVER_PRICES_CSV")
    )
    corpus_root = os.getenv("MARKET_APP_CORPUS_ROOT")
    gdelt_dir = os.getenv("MARKET_APP_GDELT_DIR") or os.getenv("MARKET_APP_GDELT_CONFLICT_DIR")
    gdelt_raw_dir = os.getenv("MARKET_APP_GDELT_RAW_DIR") or os.getenv("MARKET_APP_GDELT_EVENTS_RAW_DIR")
    outputs_dir = os.getenv("MARKET_APP_OUTPUTS_DIR") or os.getenv("OUTPUTS_DIR")
    offline = _parse_bool(os.getenv("OFFLINE_MODE"))

    if root:
        overrides["data"]["paths"]["market_app_data_root"] = root
    if ohlcv:
        overrides["data"]["paths"]["nasdaq_daily_dir"] = ohlcv
        overrides["data_roots"]["ohlcv_dir"] = ohlcv
    if silver:
        overrides["data"]["paths"]["silver_prices_dir"] = silver
    if corpus_root:
        overrides["corpus"]["root_dir"] = corpus_root
    if gdelt_dir:
        overrides["corpus"]["gdelt_conflict_dir"] = gdelt_dir
        overrides["data_roots"]["gdelt_dir"] = gdelt_dir
    if gdelt_raw_dir:
        overrides["corpus"]["gdelt_events_raw_dir"] = gdelt_raw_dir
        overrides["data_roots"]["gdelt_raw_dir"] = gdelt_raw_dir
    if outputs_dir:
        overrides["paths"]["outputs_dir"] = outputs_dir
        overrides["data_roots"]["outputs_dir"] = outputs_dir
    if offline is not None:
        overrides["data"]["offline_mode"] = offline
    return overrides


def _apply_data_roots(config: dict[str, Any]) -> None:
    data_roots = config.setdefault("data_roots", {})
    data_cfg = config.setdefault("data", {})
    data_paths = data_cfg.setdefault("paths", {})
    paths_cfg = config.setdefault("paths", {})
    corpus_cfg = config.setdefault("corpus", {})

    ohlcv_dir = data_roots.get("ohlcv_dir") or data_paths.get("nasdaq_daily_dir")
    if ohlcv_dir:
        data_roots["ohlcv_dir"] = ohlcv_dir
        data_paths["nasdaq_daily_dir"] = ohlcv_dir

    gdelt_dir = (
        data_roots.get("gdelt_dir")
        or corpus_cfg.get("root_dir")
        or corpus_cfg.get("gdelt_conflict_dir")
    )
    if gdelt_dir:
        data_roots["gdelt_dir"] = gdelt_dir
        corpus_cfg.setdefault("root_dir", gdelt_dir)
        corpus_cfg.setdefault("gdelt_conflict_dir", gdelt_dir)

    gdelt_raw_dir = data_roots.get("gdelt_raw_dir") or corpus_cfg.get("gdelt_events_raw_dir")
    if gdelt_raw_dir:
        data_roots["gdelt_raw_dir"] = gdelt_raw_dir
        corpus_cfg.setdefault("gdelt_events_raw_dir", gdelt_raw_dir)

    outputs_dir = data_roots.get("outputs_dir") or paths_cfg.get("outputs_dir") or "outputs"
    data_roots["outputs_dir"] = outputs_dir
    paths_cfg["outputs_dir"] = outputs_dir


def _load_config_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ConfigError(
            f"Config file not found at {path}. Create one with 'python -m market_monitor init-config --out {path}'."
        )
    suffix = path.suffix.lower()
    content = path.read_text(encoding="utf-8-sig")
    if suffix in {".yaml", ".yml"}:
        data = yaml.safe_load(content) or {}
    elif suffix == ".toml":
        try:
            import tomli
        except ImportError as exc:
            raise ConfigError("tomli is required to parse TOML config files.") from exc
        data = tomli.loads(content) or {}
    else:
        try:
            data = json.loads(content)
        except json.JSONDecodeError as exc:
            raise ConfigError(f"Config file is not valid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigError("Config file must define a top-level object.")
    return data


def load_config(path: Path, overrides: dict[str, Any] | None = None) -> ConfigResult:
    config_data = _load_config_file(path)

    config = _deep_merge(DEFAULT_CONFIG, config_data)
    env_overrides = _load_env_overrides()
    if env_overrides:
        config = _deep_merge(config, env_overrides)
    if overrides:
        config = _deep_merge(config, overrides)

    _apply_data_roots(config)

    if config["data"].get("offline_mode", False):
        config["data"]["provider"] = "nasdaq_daily"
        config["data"]["fallback_chain"] = []

    _validate_config(config)
    return ConfigResult(config=config, config_hash=_hash_config(config))


def write_default_config(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix.lower() in {".yaml", ".yml"}:
        path.write_text(yaml.safe_dump(DEFAULT_CONFIG, sort_keys=False), encoding="utf-8")
    else:
        path.write_text(json.dumps(DEFAULT_CONFIG, indent=2), encoding="utf-8")


def _require(config: dict[str, Any], keys: list[str]) -> None:
    node = config
    for key in keys:
        if key not in node:
            raise ConfigError(
                f"Config missing {'.'.join(keys)}. Set it in config.yaml or pass a CLI override."
            )
        node = node[key]


def _validate_config(config: dict[str, Any]) -> None:
    required_paths = [
        ["paths", "watchlist_file"],
        ["paths", "outputs_dir"],
        ["paths", "cache_dir"],
    ]
    for path_keys in required_paths:
        _require(config, path_keys)

    if config["data"]["provider"] not in {
        "nasdaq_daily",
        "stooq",
        "twelvedata",
        "alphavantage",
        "finnhub",
    }:
        raise ConfigError(
            "data.provider must be one of nasdaq_daily, stooq, twelvedata, alphavantage, finnhub."
        )

    if config["staging"]["stage1_micro_days"] != 7:
        pass

    for stage_key in [
        "stage1_micro_days",
        "stage2_short_days",
        "stage3_deep_days",
        "history_min_days",
    ]:
        if config["staging"][stage_key] <= 0:
            raise ConfigError(f"staging.{stage_key} must be > 0.")

    price_min = config["gates"].get("price_min")
    price_max = config["gates"].get("price_max")
    if price_min is not None and price_min <= 0:
        raise ConfigError("gates.price_min must be > 0 when set.")
    if price_max is not None and price_max <= 0:
        raise ConfigError("gates.price_max must be > 0 when set.")
    if price_min is not None and price_max is not None and price_min > price_max:
        raise ConfigError("gates.price_min must be <= gates.price_max.")

    risk_cfg = config["gates"].get("risk_flags", {})
    if risk_cfg.get("zero_volume_frac") is not None and risk_cfg["zero_volume_frac"] < 0:
        raise ConfigError("gates.risk_flags.zero_volume_frac must be >= 0.")

    if config["data"]["max_workers"] <= 0:
        raise ConfigError("data.max_workers must be > 0.")

    _validate_bulk_section(config)


def _validate_bulk_section(config: dict[str, Any]) -> None:
    bulk_cfg = config.get("bulk", {})
    sources = bulk_cfg.get("sources", [])
    names: set[str] = set()

    for source in sources:
        name = source.get("name")
        base_url = source.get("base_url")
        if not name or not base_url:
            raise ConfigError("bulk.sources entries must include name and base_url.")
        if name in names:
            raise ConfigError(f"bulk.sources contains duplicate name: {name}")
        names.add(name)

        supports_archive = bool(source.get("supports_bulk_archive", False))
        has_symbol = bool(source.get("symbol_template"))
        has_static = bool(source.get("static_path"))
        if not (supports_archive or has_symbol or has_static):
            raise ConfigError(
                f"bulk.sources entry '{name}' must provide symbol_template, static_path, or supports_bulk_archive."
            )
