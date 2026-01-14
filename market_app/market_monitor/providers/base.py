from dataclasses import dataclass
from typing import Dict, List, Optional

import pandas as pd


class ProviderError(RuntimeError):
    pass


class ProviderAccessError(ProviderError):
    pass


class ProviderLimitError(ProviderError):
    pass


@dataclass(frozen=True)
class ProviderCapabilities:
    supports_history: bool
    supports_quote: bool
    supports_adjusted: bool
    rate_limit_model: str


@dataclass
class Quote:
    symbol: str
    last_price: Optional[float]
    prev_close: Optional[float]
    volume: Optional[float]
    quote_timestamp_utc: Optional[str]


class HistoryProvider:
    name = "base"
    capabilities = ProviderCapabilities(False, False, False, "unknown")

    def get_history(self, symbol: str, days: int) -> pd.DataFrame:
        raise NotImplementedError

    def get_quote(self, symbol: str) -> Quote:
        raise NotImplementedError

    def supports(self, feature: str) -> bool:
        return getattr(self.capabilities, feature, False)

    def prepare(self) -> None:
        return None


class BudgetManager:
    def __init__(self, max_requests: int) -> None:
        self.max_requests = max_requests
        self.used = 0

    def consume(self) -> None:
        if self.used >= self.max_requests:
            raise ProviderLimitError("LIMIT_EXCEEDED")
        self.used += 1

    def remaining(self) -> int:
        return max(self.max_requests - self.used, 0)
