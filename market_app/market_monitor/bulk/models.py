from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class BulkSource:
    name: str
    base_url: str
    symbol_template: str | None = None
    supports_bulk_archive: bool = False
    archive_path: str | None = None
    static_path: str | None = None
    file_extension: str = ".csv"

    def build_symbol_url(self, symbol: str) -> str:
        if not self.symbol_template:
            raise ValueError(f"Source {self.name} does not support symbol URLs.")
        symbol_slug = self.symbol_template.format(symbol=symbol)
        suffix = "" if self.file_extension == "" else self.file_extension
        return _join_url(self.base_url, f"{symbol_slug}{suffix}")

    def build_archive_url(self) -> str:
        if not self.supports_bulk_archive or not self.archive_path:
            raise ValueError(f"Source {self.name} does not support archive downloads.")
        return _join_url(self.base_url, self.archive_path)

    def build_static_url(self) -> str:
        if not self.static_path:
            raise ValueError(f"Source {self.name} does not support static downloads.")
        return _join_url(self.base_url, self.static_path)


@dataclass(frozen=True)
class BulkDownloadTask:
    source_name: str
    url: str
    destination: Path
    symbol: str | None = None
    is_archive: bool = False
    kind: str = "symbol"


def _join_url(base_url: str, path: str) -> str:
    if path.startswith("?") or path.startswith("/"):
        return f"{base_url.rstrip('/')}{path}"
    return f"{base_url.rstrip('/')}/{path.lstrip('/')}"
