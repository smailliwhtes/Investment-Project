import pandas as pd
import requests

from market_monitor.offline import OfflineModeError, is_offline, require_online
from market_monitor.providers.base import (
    HistoryProvider,
    ProviderCapabilities,
    ProviderError,
    Quote,
)
from market_monitor.providers.http import RetryConfig, request_with_backoff


if is_offline():
    raise OfflineModeError("Offline mode enabled; blocked import of TwelveDataProvider.")


class TwelveDataProvider(HistoryProvider):
    name = "twelvedata"
    capabilities = ProviderCapabilities(True, False, False, "credit")

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.twelvedata.com",
        retry_config: RetryConfig | None = None,
        session: requests.Session | None = None,
    ) -> None:
        require_online("TwelveDataProvider init")
        self.api_key = api_key
        self.base_url = base_url
        self.retry_config = retry_config
        self.session = session

    def get_history(self, symbol: str, days: int) -> pd.DataFrame:
        params = {
            "symbol": symbol,
            "interval": "1day",
            "apikey": self.api_key,
            "outputsize": days,
            "format": "JSON",
        }
        response = request_with_backoff(
            f"{self.base_url}/time_series",
            params=params,
            session=self.session,
            retry=self.retry_config,
            timeout=30,
        )
        if response.status_code != 200:
            raise ProviderError(f"TwelveData HTTP {response.status_code}")
        data = response.json()
        if "values" not in data:
            message = data.get("message", "No values in response")
            raise ProviderError(f"TwelveData error: {message}")
        df = pd.DataFrame(data["values"])
        df = df.rename(
            columns={
                "datetime": "Date",
                "open": "Open",
                "high": "High",
                "low": "Low",
                "close": "Close",
                "volume": "Volume",
            }
        )
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.dropna(subset=["Date"]).sort_values("Date")
        for col in ["Open", "High", "Low", "Close", "Volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["Close"])
        return df

    def get_quote(self, symbol: str) -> Quote:
        raise ProviderError("TwelveData quote not implemented")
