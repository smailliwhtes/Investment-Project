import time
from typing import Optional

import pandas as pd
import requests

from market_monitor.providers.base import HistoryProvider, ProviderCapabilities, ProviderError, Quote


class TwelveDataProvider(HistoryProvider):
    name = "twelvedata"
    capabilities = ProviderCapabilities(True, False, False, "credit")

    def __init__(self, api_key: str, base_url: str = "https://api.twelvedata.com") -> None:
        self.api_key = api_key
        self.base_url = base_url

    def get_history(self, symbol: str, days: int) -> pd.DataFrame:
        params = {
            "symbol": symbol,
            "interval": "1day",
            "apikey": self.api_key,
            "outputsize": days,
            "format": "JSON",
        }
        response = requests.get(f"{self.base_url}/time_series", params=params, timeout=30)
        if response.status_code != 200:
            raise ProviderError(f"TwelveData HTTP {response.status_code}")
        data = response.json()
        if "values" not in data:
            message = data.get("message", "No values in response")
            raise ProviderError(f"TwelveData error: {message}")
        df = pd.DataFrame(data["values"])
        df = df.rename(columns={
            "datetime": "Date",
            "open": "Open",
            "high": "High",
            "low": "Low",
            "close": "Close",
            "volume": "Volume",
        })
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.dropna(subset=["Date"]).sort_values("Date")
        for col in ["Open", "High", "Low", "Close", "Volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["Close"])
        return df

    def get_quote(self, symbol: str) -> Quote:
        raise ProviderError("TwelveData quote not implemented")
