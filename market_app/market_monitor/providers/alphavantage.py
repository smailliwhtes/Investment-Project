import pandas as pd
import requests

from market_monitor.providers.base import (
    HistoryProvider,
    ProviderCapabilities,
    ProviderError,
    Quote,
)
from market_monitor.providers.http import RetryConfig, request_with_backoff


class AlphaVantageProvider(HistoryProvider):
    name = "alphavantage"
    capabilities = ProviderCapabilities(True, False, True, "credit")

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://www.alphavantage.co",
        retry_config: RetryConfig | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url
        self.retry_config = retry_config
        self.session = session

    def get_history(self, symbol: str, days: int) -> pd.DataFrame:
        params = {
            "function": "TIME_SERIES_DAILY_ADJUSTED",
            "symbol": symbol,
            "apikey": self.api_key,
            "outputsize": "full",
        }
        response = request_with_backoff(
            f"{self.base_url}/query",
            params=params,
            session=self.session,
            retry=self.retry_config,
            timeout=30,
        )
        if response.status_code != 200:
            raise ProviderError(f"AlphaVantage HTTP {response.status_code}")
        data = response.json()
        if "Time Series (Daily)" not in data:
            note = data.get("Note") or data.get("Error Message") or "No time series in response"
            raise ProviderError(f"AlphaVantage error: {note}")
        rows = []
        for date_str, values in data["Time Series (Daily)"].items():
            rows.append(
                {
                    "Date": date_str,
                    "Open": values.get("1. open"),
                    "High": values.get("2. high"),
                    "Low": values.get("3. low"),
                    "Close": values.get("4. close"),
                    "Adjusted_Close": values.get("5. adjusted close"),
                    "Volume": values.get("6. volume"),
                }
            )
        df = pd.DataFrame(rows)
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.dropna(subset=["Date"]).sort_values("Date")
        for col in ["Open", "High", "Low", "Close", "Adjusted_Close", "Volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["Close"])
        if len(df) > days:
            df = df.tail(days)
        return df

    def get_quote(self, symbol: str) -> Quote:
        raise ProviderError("AlphaVantage quote not implemented")
