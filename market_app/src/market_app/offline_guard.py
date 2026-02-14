from __future__ import annotations

import socket
from contextlib import contextmanager
from dataclasses import dataclass
from functools import wraps
from typing import Callable, Iterator


class OfflineNetworkError(RuntimeError):
    """Raised when network access is attempted in strict offline mode."""


@dataclass(frozen=True)
class _SocketHooks:
    connect: Callable
    create_connection: Callable
    getaddrinfo: Callable


@contextmanager
def enforce_offline_network_block(enabled: bool) -> Iterator[None]:
    """Block all outbound network calls while the context is active.

    Localhost loopback traffic remains allowed to avoid breaking local test servers.
    """

    if not enabled:
        yield
        return

    allowed_hosts = {"localhost", "127.0.0.1", "::1"}
    original = _SocketHooks(
        connect=socket.socket.connect,
        create_connection=socket.create_connection,
        getaddrinfo=socket.getaddrinfo,
    )

    def _normalize_host(address) -> str:
        if isinstance(address, tuple) and address:
            return str(address[0]).lower()
        return str(address).lower()

    def _blocked_connect(self, address):
        host = _normalize_host(address)
        if host in allowed_hosts:
            return original.connect(self, address)
        raise OfflineNetworkError(f"Offline mode enabled; blocked network socket connect to '{host}'.")

    def _blocked_create_connection(address, *args, **kwargs):
        host = _normalize_host(address)
        if host in allowed_hosts:
            return original.create_connection(address, *args, **kwargs)
        raise OfflineNetworkError(
            f"Offline mode enabled; blocked network socket create_connection to '{host}'."
        )

    def _blocked_getaddrinfo(host, *args, **kwargs):
        host_s = str(host).lower()
        if host_s in allowed_hosts:
            return original.getaddrinfo(host, *args, **kwargs)
        raise OfflineNetworkError(f"Offline mode enabled; blocked DNS lookup for '{host_s}'.")

    socket.socket.connect = _blocked_connect
    socket.create_connection = _blocked_create_connection
    socket.getaddrinfo = _blocked_getaddrinfo
    try:
        yield
    finally:
        socket.socket.connect = original.connect
        socket.create_connection = original.create_connection
        socket.getaddrinfo = original.getaddrinfo


def offline_guard(is_offline: bool, action: str) -> Callable:
    """Decorator that blocks wrapped call when offline mode is enabled."""

    def _decorator(func: Callable) -> Callable:
        @wraps(func)
        def _wrapped(*args, **kwargs):
            if is_offline:
                raise OfflineNetworkError(
                    f"Offline mode enabled; blocked network call path: {action}"
                )
            return func(*args, **kwargs)

        return _wrapped

    return _decorator
