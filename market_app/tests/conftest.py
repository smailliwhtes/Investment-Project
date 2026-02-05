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
