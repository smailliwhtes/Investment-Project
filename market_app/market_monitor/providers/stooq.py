import time
from io import StringIO

import pandas as pd
import requests

from market_monitor.providers.base import HistoryProvider, ProviderCapabilities, ProviderError


class StooqProvider(HistoryProvider):
    name = "stooq"
    capabilities = ProviderCapabilities(True, False, False, "polite")

    def __init__(self, sleep_ms: int = 0) -> None:
        self.sleep_ms = sleep_ms

    def get_history(self, symbol: str, days: int) -> pd.DataFrame:
        stooq_symbol = f"{symbol.lower()}.us"
        url = f"https://stooq.pl/q/d/l/?s={stooq_symbol}&i=d"
        response = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        if response.status_code != 200:
            raise ProviderError(f"Stooq HTTP {response.status_code}")
        if "Date,Open,High,Low,Close,Volume" not in response.text:
            raise ProviderError("Stooq returned non-CSV response")
        df = pd.read_csv(StringIO(response.text))
        df = df.rename(columns=str.title)
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.dropna(subset=["Date"]).sort_values("Date")
        for col in ["Open", "High", "Low", "Close", "Volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["Close"])
        if self.sleep_ms > 0:
            time.sleep(self.sleep_ms / 1000.0)
        if len(df) > days:
            df = df.tail(days)
        return df
