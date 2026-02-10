from __future__ import annotations

import socket
from typing import Callable


class OfflineModeError(RuntimeError):
    pass


_ORIGINAL_CONNECT: Callable[..., object] | None = None
_ORIGINAL_CREATE_CONNECTION: Callable[..., socket.socket] | None = None
_ENABLED = False


def enforce_offline(enabled: bool) -> None:
    global _ORIGINAL_CONNECT, _ORIGINAL_CREATE_CONNECTION, _ENABLED
    if not enabled:
        if _ENABLED:
            _restore_network()
        return
    if _ENABLED:
        return

    def _blocked_create_connection(*args, **kwargs):
        raise OfflineModeError(
            "Offline mode enabled; blocked network connection. "
            "To allow online provisioning, rerun with --online."
        )

    def _blocked_connect(*args, **kwargs):
        raise OfflineModeError(
            "Offline mode enabled; blocked network socket usage. "
            "To allow online provisioning, rerun with --online."
        )

    _ORIGINAL_CONNECT = socket.socket.connect
    _ORIGINAL_CREATE_CONNECTION = socket.create_connection
    socket.socket.connect = _blocked_connect  # type: ignore[assignment]
    socket.create_connection = _blocked_create_connection  # type: ignore[assignment]
    _ENABLED = True


def _restore_network() -> None:
    global _ORIGINAL_CONNECT, _ORIGINAL_CREATE_CONNECTION, _ENABLED
    if _ORIGINAL_CONNECT is not None:
        socket.socket.connect = _ORIGINAL_CONNECT  # type: ignore[assignment]
    if _ORIGINAL_CREATE_CONNECTION is not None:
        socket.create_connection = _ORIGINAL_CREATE_CONNECTION  # type: ignore[assignment]
    _ENABLED = False
