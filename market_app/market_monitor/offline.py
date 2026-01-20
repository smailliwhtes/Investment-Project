from __future__ import annotations


class OfflineModeError(RuntimeError):
    pass


_OFFLINE_MODE = False


def set_offline_mode(enabled: bool) -> None:
    global _OFFLINE_MODE
    _OFFLINE_MODE = bool(enabled)


def is_offline() -> bool:
    return _OFFLINE_MODE


def require_online(action: str) -> None:
    if _OFFLINE_MODE:
        raise OfflineModeError(f"Offline mode enabled; blocked network call: {action}")
