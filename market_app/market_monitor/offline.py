from __future__ import annotations

import os


class OfflineModeError(RuntimeError):
    pass


_OFFLINE_MODE = os.getenv("OFFLINE_MODE", "").strip().lower() in {"1", "true", "yes"}


def set_offline_mode(enabled: bool) -> None:
    global _OFFLINE_MODE
    _OFFLINE_MODE = bool(enabled)


def is_offline() -> bool:
    return _OFFLINE_MODE


def require_online(action: str) -> None:
    if _OFFLINE_MODE:
        raise OfflineModeError(f"Offline mode enabled; blocked network call: {action}")
