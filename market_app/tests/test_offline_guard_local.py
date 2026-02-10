from __future__ import annotations

import socket

import pytest

from market_app.offline_guard import OfflineModeError, enforce_offline


def test_offline_guard_blocks_socket() -> None:
    enforce_offline(True)
    try:
        with pytest.raises(OfflineModeError):
            socket.create_connection(("example.com", 80), timeout=1)
    finally:
        enforce_offline(False)
