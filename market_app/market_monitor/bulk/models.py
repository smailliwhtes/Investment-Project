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
    file_extension: str = ".csv"
    static_path: str | None = None

    def build_symbol_url(self, symbol: str) -> str:
        if not self.symbol_template:
            raise ValueError(f"Source {self.name} does not support symbol URLs.")
        symbol_slug = self.symbol_template.format(symbol=symbol)
        return f"{self.base_url.rstrip('/')}/{symbol_slug}{self.file_extension}"

    def build_archive_url(self) -> str:
        if not self.supports_bulk_archive or not self.archive_path:
            raise ValueError(f"Source {self.name} does not support archive downloads.")
        return f"{self.base_url.rstrip('/')}/{self.archive_path.lstrip('/')}"

    def build_static_url(self) -> str:
        if not self.static_path:
            raise ValueError(f"Source {self.name} does not define a static path.")
        return f"{self.base_url.rstrip('/')}/{self.static_path.lstrip('/')}"


@dataclass(frozen=True)
class BulkDownloadTask:
    source_name: str
    url: str
    destination: Path
    symbol: str | None = None
    is_archive: bool = False
