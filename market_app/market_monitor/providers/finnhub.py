import time
from datetime import datetime, timezone

import pandas as pd
import requests

from market_monitor.providers.base import (
    HistoryProvider,
    ProviderAccessError,
    ProviderCapabilities,
    ProviderError,
    Quote,
)
from market_monitor.providers.http import RetryConfig, request_with_backoff


class FinnhubProvider(HistoryProvider):
    name = "finnhub"
    capabilities = ProviderCapabilities(True, True, False, "credit")

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://finnhub.io/api/v1",
        retry_config: RetryConfig | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url
        self.retry_config = retry_config
        self.session = session

    def get_quote(self, symbol: str) -> Quote:
        params = {"symbol": symbol, "token": self.api_key}
        response = request_with_backoff(
            f"{self.base_url}/quote",
            params=params,
            session=self.session,
            retry=self.retry_config,
            timeout=20,
        )
        if response.status_code != 200:
            raise ProviderError(f"Finnhub HTTP {response.status_code}")
        data = response.json()
        if not data:
            raise ProviderError("Finnhub empty quote response")
        ts = data.get("t")
        ts_iso = None
        if ts:
            ts_iso = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        return Quote(
            symbol=symbol,
            last_price=data.get("c"),
            prev_close=data.get("pc"),
            volume=None,
            quote_timestamp_utc=ts_iso,
        )

    def get_history(self, symbol: str, days: int) -> pd.DataFrame:
        end = int(time.time())
        start = end - (days + 5) * 86400
        params = {
            "symbol": symbol,
            "resolution": "D",
            "from": start,
            "to": end,
            "token": self.api_key,
        }
        response = request_with_backoff(
            f"{self.base_url}/stock/candle",
            params=params,
            session=self.session,
            retry=self.retry_config,
            timeout=20,
        )
        if response.status_code == 403:
            raise ProviderAccessError("Finnhub candles not available on current plan")
        if response.status_code != 200:
            raise ProviderError(f"Finnhub HTTP {response.status_code}")
        data = response.json()
        if data.get("s") != "ok":
            raise ProviderAccessError("Finnhub candles not available")
        df = pd.DataFrame(
            {
                "Date": pd.to_datetime(data["t"], unit="s", utc=True).date,
                "Open": data["o"],
                "High": data["h"],
                "Low": data["l"],
                "Close": data["c"],
                "Volume": data["v"],
            }
        )
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.dropna(subset=["Date"]).sort_values("Date")
        if len(df) > days:
            df = df.tail(days)
        return df
