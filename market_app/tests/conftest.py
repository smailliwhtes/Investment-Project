import os
import socket
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture(autouse=True)
def block_network(monkeypatch: pytest.MonkeyPatch) -> None:
    allowed_hosts = {"localhost", "127.0.0.1", "::1"}
    real_connect = socket.socket.connect

    def guarded_connect(self, address):
        host = address[0] if isinstance(address, tuple) else address
        if host in allowed_hosts:
            return real_connect(self, address)
        raise RuntimeError("Network access is blocked during tests.")

    monkeypatch.setattr(socket.socket, "connect", guarded_connect)


_MARKET_APP_ENV_KEYS = [
    "MARKET_APP_DATA_ROOT",
    "MARKET_APP_OHLCV_DIR",
    "MARKET_APP_OHLCV_DAILY_DIR",
    "MARKET_APP_NASDAQ_DAILY_DIR",
    "NASDAQ_DAILY_DIR",
    "MARKET_APP_SILVER_PRICES_DIR",
    "SILVER_PRICES_DIR",
    "SILVER_PRICES_CSV",
    "MARKET_APP_CORPUS_DIR",
    "MARKET_APP_CORPUS_ROOT",
    "MARKET_APP_GDELT_DIR",
    "MARKET_APP_GDELT_CONFLICT_DIR",
    "MARKET_APP_GDELT_RAW_DIR",
    "MARKET_APP_GDELT_EVENTS_RAW_DIR",
    "MARKET_APP_EXOGENOUS_DAILY_DIR",
    "MARKET_APP_OUTPUTS_DIR",
    "OUTPUTS_DIR",
    "OFFLINE_MODE",
]


@pytest.fixture(scope="session", autouse=True)
def clear_market_app_env_overrides_session(request: pytest.FixtureRequest) -> None:
    snapshot = {key: os.environ.get(key) for key in _MARKET_APP_ENV_KEYS}
    request.config._market_app_env_snapshot = snapshot  # type: ignore[attr-defined]
    for key in _MARKET_APP_ENV_KEYS:
        os.environ.pop(key, None)
    yield
    for key in _MARKET_APP_ENV_KEYS:
        os.environ.pop(key, None)
    for key, value in snapshot.items():
        if value is not None:
            os.environ[key] = value


@pytest.fixture
def restore_market_app_env_overrides(monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest) -> None:
    """Opt out of the env scrubber for tests that intentionally rely on MARKET_APP_* overrides."""
    snapshot = getattr(request.config, "_market_app_env_snapshot", {})
    for key in _MARKET_APP_ENV_KEYS:
        if key not in snapshot or snapshot[key] is None:
            monkeypatch.delenv(key, raising=False)
        else:
            monkeypatch.setenv(key, snapshot[key])


@pytest.fixture(autouse=True)
def clear_market_app_env_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    override_vars = [
        *_MARKET_APP_ENV_KEYS,
    ]
    for key in override_vars:
        monkeypatch.delenv(key, raising=False)
