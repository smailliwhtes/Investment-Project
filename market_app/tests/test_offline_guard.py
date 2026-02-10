from __future__ import annotations

import socket

import pytest

from market_app.offline_guard import OfflineNetworkError, enforce_offline_network_block


def test_offline_guard_blocks_external_network_calls() -> None:
    with enforce_offline_network_block(True):
        with pytest.raises(OfflineNetworkError):
            socket.create_connection(("example.com", 80), timeout=0.5)


def test_offline_guard_disabled_does_not_patch_socket() -> None:
    with enforce_offline_network_block(False):
        with pytest.raises(OSError):
            socket.create_connection(("127.0.0.1", 9), timeout=0.1)
