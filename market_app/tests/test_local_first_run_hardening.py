"""Tests for local-first-run hardening: watchlist, exogenous optional, env overrides, out-dir creation."""
from __future__ import annotations

import json
import os
import textwrap
from pathlib import Path

import pandas as pd
import pytest

from market_monitor.config_schema import ConfigError, load_config
from market_monitor.validation import validate_data, validate_watchlist


def _write_ohlcv(path: Path, symbol: str, rows: int = 300) -> None:
    """Write a minimal valid OHLCV CSV for ``symbol``."""
    dates = pd.bdate_range(end="2025-01-15", periods=rows)
    df = pd.DataFrame(
        {
            "date": dates.strftime("%Y-%m-%d"),
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "volume": 1_000_000,
        }
    )
    path.mkdir(parents=True, exist_ok=True)
    df.to_csv(path / f"{symbol}.csv", index=False)


def _write_config(path: Path, extra: dict | None = None) -> None:
    """Write a minimal config.yaml."""
    import yaml

    cfg = {
        "schema_version": "v2",
        "offline": True,
        "paths": {
            "watchlist_file": "watchlists/watchlist_core.csv",
            "outputs_dir": "outputs",
            "cache_dir": "data/cache",
            "ohlcv_daily_dir": "data/ohlcv_daily",
            "exogenous_daily_dir": "data/exogenous/daily_features",
        },
        "data": {"offline_mode": True, "provider": "nasdaq_daily"},
    }
    if extra:
        cfg.update(extra)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(cfg), encoding="utf-8")


def _write_watchlist(path: Path, symbols: list[str]) -> None:
    """Write a valid watchlist CSV."""
    lines = ["symbol,theme_bucket,asset_type"]
    for sym in symbols:
        lines.append(f"{sym},,equity")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


# ---------------------------------------------------------------------------
# 1) Missing/empty watchlist → validation fails fast with actionable message
# ---------------------------------------------------------------------------

class TestMissingWatchlist:
    def test_validate_watchlist_missing_file(self, tmp_path: Path) -> None:
        missing = tmp_path / "missing_watchlist.csv"
        df, errors = validate_watchlist(missing)
        assert df.empty
        assert any("missing" in e.lower() or "empty" in e.lower() for e in errors)
        assert str(missing) in errors[0]

    def test_validate_watchlist_empty_file(self, tmp_path: Path) -> None:
        empty = tmp_path / "empty.csv"
        empty.write_text("symbol,theme_bucket,asset_type\n", encoding="utf-8")
        df, errors = validate_watchlist(empty)
        assert df.empty
        assert any("empty" in e.lower() or "missing" in e.lower() for e in errors)

    def test_validate_data_missing_watchlist(self, tmp_path: Path) -> None:
        ohlcv_dir = tmp_path / "ohlcv"
        ohlcv_dir.mkdir()
        result = validate_data(
            watchlist_path=tmp_path / "no_such_watchlist.csv",
            ohlcv_daily_dir=ohlcv_dir,
            exogenous_daily_dir=tmp_path / "exog",
            asof_date="2025-01-15",
            min_history_days=252,
            benchmark_symbols=[],
        )
        assert not result.ok
        assert any("missing" in e.lower() or "empty" in e.lower() for e in result.errors)


# ---------------------------------------------------------------------------
# 2) conversion_errors.csv in OHLCV dir must NOT break run
# ---------------------------------------------------------------------------

class TestConversionErrorsSkipped:
    def test_conversion_errors_csv_ignored_in_ohlcv_dir(self, tmp_path: Path) -> None:
        """Non-OHLCV files like conversion_errors.csv must not cause failures."""
        ohlcv_dir = tmp_path / "ohlcv"
        _write_ohlcv(ohlcv_dir, "AAPL")

        # Write a non-OHLCV file
        (ohlcv_dir / "conversion_errors.csv").write_text(
            "file,error\nfoo.csv,bad date\n", encoding="utf-8"
        )

        watchlist_path = tmp_path / "watchlist.csv"
        _write_watchlist(watchlist_path, ["AAPL"])

        result = validate_data(
            watchlist_path=watchlist_path,
            ohlcv_daily_dir=ohlcv_dir,
            exogenous_daily_dir=tmp_path / "exog",
            asof_date="2025-01-15",
            min_history_days=10,
            benchmark_symbols=[],
        )
        # Should not fail because of conversion_errors.csv
        assert result.ok

    def test_compute_daily_features_skips_conversion_errors(self, tmp_path: Path) -> None:
        """compute_daily_features must skip conversion_errors.csv."""
        from market_monitor.features.compute_daily_features import compute_daily_features

        ohlcv_dir = tmp_path / "ohlcv"
        _write_ohlcv(ohlcv_dir, "AAPL")

        # Write a non-OHLCV file that would crash read_ohlcv()
        (ohlcv_dir / "conversion_errors.csv").write_text(
            "file,error\nfoo.csv,bad date\n", encoding="utf-8"
        )

        out_dir = tmp_path / "features_out"
        out_dir.mkdir()

        # Should not raise even though conversion_errors.csv exists
        result = compute_daily_features(
            ohlcv_dir=ohlcv_dir,
            out_dir=out_dir,
            asof_date="2025-01-15",
        )
        assert (out_dir / "features_by_symbol.csv").exists()

    def test_normalize_directory_skips_conversion_errors(self, tmp_path: Path) -> None:
        """ohlcv_doctor.normalize_directory must skip conversion_errors.csv."""
        from market_monitor.ohlcv_doctor import normalize_directory

        raw_dir = tmp_path / "raw"
        _write_ohlcv(raw_dir, "AAPL")

        (raw_dir / "conversion_errors.csv").write_text(
            "file,error\nfoo.csv,bad date\n", encoding="utf-8"
        )

        out_dir = tmp_path / "normalized"
        result = normalize_directory(
            raw_dir=raw_dir,
            out_dir=out_dir,
            date_col=None,
            delimiter=None,
            symbol_from_filename=True,
            coerce=True,
            strict=False,
            streaming=False,
            chunk_rows=200_000,
        )
        # Should succeed; only AAPL should appear in results
        symbols = [r.symbol for r in result["results"]]
        assert "AAPL" in symbols
        assert "CONVERSION_ERRORS" not in symbols


# ---------------------------------------------------------------------------
# 4) Env var overrides
# ---------------------------------------------------------------------------

class TestEnvOverrides:
    def test_ohlcv_daily_dir_env_override(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        ohlcv_dir = tmp_path / "my_ohlcv"
        ohlcv_dir.mkdir()
        monkeypatch.setenv("MARKET_APP_OHLCV_DAILY_DIR", str(ohlcv_dir))
        config_path = tmp_path / "config.yaml"
        _write_config(config_path)
        result = load_config(config_path)
        assert result.config["data"]["paths"]["nasdaq_daily_dir"] == str(ohlcv_dir)

    def test_corpus_dir_env_override(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        corpus_dir = tmp_path / "corpus"
        corpus_dir.mkdir()
        monkeypatch.setenv("MARKET_APP_CORPUS_DIR", str(corpus_dir))
        config_path = tmp_path / "config.yaml"
        _write_config(config_path)
        result = load_config(config_path)
        assert result.config["corpus"]["root_dir"] == str(corpus_dir)

    def test_exogenous_daily_dir_env_override(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        exog_dir = tmp_path / "exogenous"
        exog_dir.mkdir()
        monkeypatch.setenv("MARKET_APP_EXOGENOUS_DAILY_DIR", str(exog_dir))
        config_path = tmp_path / "config.yaml"
        # Config without exogenous_daily_dir so env var takes precedence
        import yaml
        cfg = {
            "schema_version": "v2",
            "offline": True,
            "paths": {
                "watchlist_file": "watchlists/watchlist_core.csv",
                "outputs_dir": "outputs",
                "cache_dir": "data/cache",
                "ohlcv_daily_dir": "data/ohlcv_daily",
            },
            "data": {"offline_mode": True, "provider": "nasdaq_daily"},
        }
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
        result = load_config(config_path)
        assert result.config["paths"]["exogenous_daily_dir"] == str(exog_dir)


# ---------------------------------------------------------------------------
# 5) Exogenous optional
# ---------------------------------------------------------------------------

class TestExogenousOptional:
    def test_exogenous_disabled_no_dir_ok(self, tmp_path: Path) -> None:
        """When exogenous is disabled, missing exogenous dir must not fail."""
        ohlcv_dir = tmp_path / "ohlcv"
        _write_ohlcv(ohlcv_dir, "AAPL")
        watchlist_path = tmp_path / "watchlist.csv"
        _write_watchlist(watchlist_path, ["AAPL"])

        result = validate_data(
            watchlist_path=watchlist_path,
            ohlcv_daily_dir=ohlcv_dir,
            exogenous_daily_dir=tmp_path / "no_such_exog",
            asof_date="2025-01-15",
            min_history_days=10,
            benchmark_symbols=[],
            exogenous_enabled=False,
        )
        assert result.ok

    def test_exogenous_enabled_empty_dir_fails(self, tmp_path: Path) -> None:
        """When exogenous is enabled and dir is empty, validation must fail."""
        ohlcv_dir = tmp_path / "ohlcv"
        _write_ohlcv(ohlcv_dir, "AAPL")
        watchlist_path = tmp_path / "watchlist.csv"
        _write_watchlist(watchlist_path, ["AAPL"])
        exog_dir = tmp_path / "exog"
        exog_dir.mkdir()

        result = validate_data(
            watchlist_path=watchlist_path,
            ohlcv_daily_dir=ohlcv_dir,
            exogenous_daily_dir=exog_dir,
            asof_date="2025-01-15",
            min_history_days=10,
            benchmark_symbols=[],
            exogenous_enabled=True,
        )
        assert not result.ok
        assert any("exogenous" in e.lower() for e in result.errors)

    def test_exogenous_enabled_missing_dir_fails(self, tmp_path: Path) -> None:
        """When exogenous is enabled and dir is missing, validation must fail."""
        ohlcv_dir = tmp_path / "ohlcv"
        _write_ohlcv(ohlcv_dir, "AAPL")
        watchlist_path = tmp_path / "watchlist.csv"
        _write_watchlist(watchlist_path, ["AAPL"])

        result = validate_data(
            watchlist_path=watchlist_path,
            ohlcv_daily_dir=ohlcv_dir,
            exogenous_daily_dir=tmp_path / "missing_exog",
            asof_date="2025-01-15",
            min_history_days=10,
            benchmark_symbols=[],
            exogenous_enabled=True,
        )
        assert not result.ok
        assert any("exogenous" in e.lower() for e in result.errors)

    def test_default_config_has_exogenous_disabled(self, tmp_path: Path) -> None:
        config_path = tmp_path / "config.yaml"
        _write_config(config_path)
        result = load_config(config_path)
        assert result.config.get("exogenous", {}).get("enabled") is False


# ---------------------------------------------------------------------------
# 6) out-dir creation
# ---------------------------------------------------------------------------

class TestOutDirCreation:
    def test_run_watchlist_creates_output_dir_early(self, tmp_path: Path) -> None:
        """The run_watchlist function should create output_dir at start, before validation."""
        from unittest.mock import patch
        import argparse
        from market_monitor.run_watchlist import run_watchlist

        ohlcv_dir = tmp_path / "ohlcv"
        _write_ohlcv(ohlcv_dir, "AAPL")
        watchlist_path = tmp_path / "watchlist.csv"
        _write_watchlist(watchlist_path, ["AAPL"])
        config_path = tmp_path / "config.yaml"
        _write_config(config_path)

        outputs_dir = tmp_path / "outputs"
        run_dir = outputs_dir / "test_run"

        args = argparse.Namespace(
            config=str(config_path),
            watchlist=str(watchlist_path),
            asof="2025-01-15",
            run_id="test_run",
            ohlcv_raw_dir=None,
            ohlcv_daily_dir=str(ohlcv_dir),
            exogenous_daily_dir=str(tmp_path / "exog"),
            outputs_dir=str(outputs_dir),
            include_raw_gdelt=False,
            log_level="WARNING",
            workers=1,
            profile=False,
        )

        # The run will likely fail at feature computation, but output_dir should exist
        try:
            run_watchlist(args)
        except Exception:
            pass

        assert run_dir.exists(), "Output dir should be created early even if run fails"


# ---------------------------------------------------------------------------
# Preflight --offline flag accepted
# ---------------------------------------------------------------------------

class TestPreflightOfflineFlag:
    def test_preflight_accepts_offline_flag(self) -> None:
        """preflight parser should accept --offline without error."""
        from market_monitor.cli import build_parser
        parser = build_parser()
        # Should not raise
        args = parser.parse_args(["preflight", "--config", "config.yaml", "--offline"])
        assert hasattr(args, "offline")
