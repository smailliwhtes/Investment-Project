import importlib

import pytest

from market_monitor.offline import OfflineModeError, set_offline_mode
from market_monitor.providers.http import request_with_backoff
from market_monitor.universe import fetch_universe


def test_offline_blocks_network_calls() -> None:
    set_offline_mode(True)
    try:
        with pytest.raises(OfflineModeError):
            request_with_backoff("https://example.com")
        with pytest.raises(OfflineModeError):
            fetch_universe()
        with pytest.raises(OfflineModeError):
            importlib.import_module("market_monitor.providers.stooq")
    finally:
        set_offline_mode(False)
