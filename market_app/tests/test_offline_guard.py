from __future__ import annotations

from market_app.offline_guard import OfflineNetworkError, offline_guard


def test_offline_guard_blocks_network_path() -> None:
    called = {"value": False}

    @offline_guard(True, "fake_network.fetch")
    def _network_call() -> int:
        called["value"] = True
        return 1

    try:
        _network_call()
        assert False, "expected OfflineNetworkError"
    except OfflineNetworkError as exc:
        assert "fake_network.fetch" in str(exc)
    assert called["value"] is False
