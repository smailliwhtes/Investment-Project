import argparse
import datetime as dt
import json
import math
import os
import socket
import time
from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from typing import Iterable, Optional

import numpy as np
import pandas as pd
import requests

from market_monitor.offline import require_online
from market_monitor.universe import read_watchlist

# --- .env autoload (optional) ---
try:
    from dotenv import load_dotenv  # python-dotenv

    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
except Exception:
    pass
# --- end .env autoload ---

STOOQ_URL = "https://stooq.com/q/d/l/?s={symbol}.us&i=d"
FINNHUB_QUOTE_URL = "https://finnhub.io/api/v1/quote"
TWELVEDATA_TS_URL = "https://api.twelvedata.com/time_series"
RETRY_STATUSES = {429, 500, 502, 503, 504}


@dataclass
class ProviderStats:
    requests: int = 0
    success: int = 0
    cache_hits: int = 0
    errors: dict = None
    last_error: Optional[str] = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = {}

    def record_error(self, error_type: str) -> None:
        self.errors[error_type] = self.errors.get(error_type, 0) + 1
        self.last_error = error_type


def _init_provider_stats() -> dict:
    return {
        "stooq": ProviderStats(),
        "twelvedata": ProviderStats(),
        "finnhub": ProviderStats(),
        "cache": ProviderStats(),
    }


def _read_watchlist(path: str):
    if Path(path).suffix.lower() == ".csv":
        df = read_watchlist(Path(path))
        symbols = df["symbol"].astype(str).tolist()
        themes_joined = (
            df.set_index("symbol")["theme_bucket"].fillna("").astype(str).to_dict()
            if "theme_bucket" in df.columns
            else {symbol: "" for symbol in symbols}
        )
        return symbols, themes_joined

    themes = {}
    order = []
    cur_theme = None
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            if line.startswith("#"):
                cur_theme = line.lstrip("#").strip() or None
                continue
            sym = line.upper()
            if sym not in themes:
                order.append(sym)
                themes[sym] = []
            if cur_theme and cur_theme not in themes[sym]:
                themes[sym].append(cur_theme)

    themes_joined = {k: "; ".join(v) if v else "" for k, v in themes.items()}
    seen = set()
    symbols = []
    for s in order:
        if s not in seen:
            seen.add(s)
            symbols.append(s)
    return symbols, themes_joined


def _stooq_reachable(timeout_sec: float = 2.0) -> bool:
    require_online("stooq reachability check")
    try:
        sock = socket.create_connection(("stooq.com", 443), timeout=timeout_sec)
        sock.close()
        return True
    except Exception:
        return False


def _request_with_retry(
    provider: str,
    url: str,
    stats: dict,
    params: Optional[dict] = None,
    timeout: tuple = (3, 15),
    max_retries: int = 4,
    backoff_base: float = 0.7,
) -> requests.Response:
    require_online(f"{provider} request {url}")
    last_exc = None
    for attempt in range(max_retries + 1):
        stats[provider].requests += 1
        try:
            resp = requests.get(url, params=params, timeout=timeout, headers={"User-Agent": "watchlist-mode/1.0"})
        except requests.exceptions.Timeout as exc:
            last_exc = exc
            stats[provider].record_error("timeout")
            if attempt < max_retries:
                time.sleep(backoff_base * (2 ** attempt) + (0.05 * attempt))
                continue
            raise
        except requests.exceptions.RequestException as exc:
            last_exc = exc
            stats[provider].record_error("network")
            if attempt < max_retries:
                time.sleep(backoff_base * (2 ** attempt) + (0.05 * attempt))
                continue
            raise

        if resp.status_code in RETRY_STATUSES:
            error_type = "rate_limit" if resp.status_code == 429 else "server_error"
            stats[provider].record_error(error_type)
            if attempt < max_retries:
                retry_after = resp.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    time.sleep(float(retry_after))
                else:
                    time.sleep(backoff_base * (2 ** attempt) + (0.1 * attempt))
                continue

        if resp.status_code == 401 or resp.status_code == 403:
            stats[provider].record_error("auth")
        elif resp.status_code == 404:
            stats[provider].record_error("not_found")
        elif resp.status_code >= 400:
            stats[provider].record_error("http_error")
        resp.raise_for_status()
        stats[provider].success += 1
        return resp

    if last_exc:
        raise last_exc
    raise RuntimeError("request_failed")


def _cache_path(cache_dir: str, symbol: str, provider: str) -> str:
    safe_symbol = symbol.replace("/", "_")
    return os.path.join(cache_dir, f"{safe_symbol}_{provider}.csv")


def _read_cached_history(cache_dir: str, symbol: str, provider: str) -> Optional[pd.DataFrame]:
    path = _cache_path(cache_dir, symbol, provider)
    if not os.path.exists(path):
        return None
    df = pd.read_csv(path)
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.dropna(subset=["Date"]).sort_values("Date")
    return df


def _write_cache(cache_dir: str, symbol: str, provider: str, df: pd.DataFrame) -> None:
    os.makedirs(cache_dir, exist_ok=True)
    path = _cache_path(cache_dir, symbol, provider)
    df.to_csv(path, index=False)


def _fetch_stooq_ohlcv(symbol: str, stats: dict, timeout=10) -> pd.DataFrame:
    url = STOOQ_URL.format(symbol=symbol.lower())
    r = _request_with_retry("stooq", url, stats, timeout=(3, timeout))
    txt = r.text.strip()
    if not txt or txt.lower().startswith("<!doctype"):
        stats["stooq"].record_error("schema")
        raise ValueError("Stooq returned non-CSV content")
    df = pd.read_csv(StringIO(txt))
    if "Date" not in df.columns or "Close" not in df.columns:
        stats["stooq"].record_error("schema")
        raise ValueError(f"Unexpected Stooq schema: {df.columns.tolist()}")

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"]).sort_values("Date")

    for c in ["Open", "High", "Low", "Close", "Volume"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["Close"])
    return df


def _fetch_twelvedata_ohlcv(symbol: str, api_key: str, stats: dict, timeout=15, outputsize=5000) -> pd.DataFrame:
    params = {
        "symbol": symbol,
        "interval": "1day",
        "outputsize": outputsize,
        "apikey": api_key,
        "format": "JSON",
    }
    r = _request_with_retry("twelvedata", TWELVEDATA_TS_URL, stats, params=params, timeout=(3, timeout))
    j = r.json()

    if isinstance(j, dict) and j.get("status") == "error":
        stats["twelvedata"].record_error("api_error")
        raise ValueError(f"TwelveData error: {j.get('message')}")

    values = j.get("values") if isinstance(j, dict) else None
    if not values:
        stats["twelvedata"].record_error("schema")
        raise ValueError("TwelveData missing 'values'")

    df = pd.DataFrame(values)
    if "datetime" in df.columns:
        df = df.rename(columns={"datetime": "Date"})
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

    rename_map = {"open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume"}
    for k, v in rename_map.items():
        if k in df.columns and v not in df.columns:
            df = df.rename(columns={k: v})

    for col in ["Open", "High", "Low", "Close", "Volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["Date", "Close"]).sort_values("Date")
    return df


def _fetch_finnhub_quote(symbol: str, api_key: str, stats: dict, timeout=10) -> dict:
    params = {"symbol": symbol, "token": api_key}
    r = _request_with_retry("finnhub", FINNHUB_QUOTE_URL, stats, params=params, timeout=(3, timeout))
    return r.json()


def _max_drawdown(series: pd.Series) -> float:
    x = series.dropna().astype(float)
    if len(x) < 2:
        return np.nan
    peak = x.cummax()
    dd = (x / peak) - 1.0
    return float(dd.min())


def _safe_zscore(x: pd.Series) -> pd.Series:
    x = pd.to_numeric(x, errors="coerce")
    mu = x.mean(skipna=True)
    sd = x.std(skipna=True)
    if sd is None or sd == 0 or np.isnan(sd):
        return pd.Series([0.0] * len(x), index=x.index)
    return (x - mu) / sd


def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def _rsi(series: pd.Series, window: int = 14) -> float:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(window=window, min_periods=window).mean()
    avg_loss = loss.rolling(window=window, min_periods=window).mean()
    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    if len(rsi.dropna()) == 0:
        if avg_gain.iloc[-1] > 0 and avg_loss.iloc[-1] == 0:
            return 100.0
        if avg_gain.iloc[-1] == 0 and avg_loss.iloc[-1] == 0:
            return 50.0
        return np.nan
    return float(rsi.iloc[-1])


def _macd(series: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> tuple:
    ema_fast = _ema(series, fast)
    ema_slow = _ema(series, slow)
    macd_line = ema_fast - ema_slow
    signal_line = _ema(macd_line, signal)
    hist = macd_line - signal_line
    return float(macd_line.iloc[-1]), float(signal_line.iloc[-1]), float(hist.iloc[-1])


def _trend_regression_stats(series: pd.Series, window: int) -> tuple:
    if len(series) < window:
        return np.nan, np.nan
    y = np.log(series.tail(window).astype(float).values)
    x = np.arange(len(y))
    if np.all(np.isfinite(y)) is False:
        return np.nan, np.nan
    slope, intercept = np.polyfit(x, y, 1)
    y_hat = slope * x + intercept
    ss_res = float(np.sum((y - y_hat) ** 2))
    ss_tot = float(np.sum((y - np.mean(y)) ** 2))
    r2 = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else np.nan
    return float(slope), float(r2)


def _forecast_return(series: pd.Series, window: int, horizon: int) -> float:
    slope, _ = _trend_regression_stats(series, window)
    if np.isnan(slope):
        return np.nan
    log_return = slope * horizon
    return float(math.exp(log_return) - 1.0)


def _hurst_exponent(series: pd.Series, max_lag: int = 20) -> float:
    if len(series) < max_lag + 2:
        return np.nan
    lags = range(2, max_lag + 1)
    tau = []
    for lag in lags:
        diff = series.diff(lag).dropna()
        tau.append(np.sqrt(np.std(diff)))
    if not np.all(np.isfinite(tau)):
        return np.nan
    poly = np.polyfit(np.log(list(lags)), np.log(tau), 1)
    return float(poly[0] * 2.0)


def _rolling_autocorr(series: pd.Series, lag: int = 1, window: int = 63) -> float:
    if len(series) < window + lag:
        return np.nan
    s = series.tail(window + lag)
    return float(s.autocorr(lag=lag))


def _load_events(path: Optional[str]) -> Optional[pd.DataFrame]:
    if not path:
        return None
    events = pd.read_csv(path)
    if "date" not in events.columns:
        raise ValueError("events file must include a 'date' column")
    events["date"] = pd.to_datetime(events["date"], errors="coerce")
    events = events.dropna(subset=["date"])
    if "weight" not in events.columns:
        events["weight"] = 1.0
    return events


def _event_impact_scores(df: pd.DataFrame, events: pd.DataFrame, window: int) -> tuple:
    if events is None or events.empty:
        return np.nan, np.nan
    series = df.set_index("Date").sort_index()
    returns = []
    weighted = []
    for _, row in events.iterrows():
        event_date = row["date"]
        weight = float(row.get("weight", 1.0))
        idx = series.index.searchsorted(event_date)
        if idx >= len(series):
            continue
        end_idx = idx + window
        if end_idx >= len(series):
            continue
        start_price = series["Close"].iloc[idx]
        end_price = series["Close"].iloc[end_idx]
        ret = float((end_price / start_price) - 1.0)
        returns.append(ret)
        weighted.append(ret * weight)
    if not returns:
        return np.nan, np.nan
    return float(np.mean(returns)), float(np.mean(weighted))


def compute_features(symbol: str, df: pd.DataFrame, events: Optional[pd.DataFrame] = None, event_window: int = 5) -> dict:
    close = df["Close"].astype(float).reset_index(drop=True)
    vol = df["Volume"].astype(float).reset_index(drop=True) if "Volume" in df.columns else pd.Series([np.nan] * len(close))

    last_close = float(close.iloc[-1])
    n = len(close)

    def horizon_return(days: int):
        if n <= days:
            return np.nan
        return float((close.iloc[-1] / close.iloc[-1 - days]) - 1.0)

    def sma(days: int):
        if n < days:
            return np.nan
        return float(close.tail(days).mean())

    lr = np.log(close).diff()

    def ann_vol(window: int):
        if n < window + 1:
            return np.nan
        s = lr.tail(window).std()
        if s is None or np.isnan(s):
            return np.nan
        return float(s * math.sqrt(252.0))

    def downside_vol(window: int):
        if n < window + 1:
            return np.nan
        w = lr.tail(window).dropna()
        w = w[w < 0]
        if len(w) < 2:
            return 0.0 if len(w) == 1 else np.nan
        return float(w.std() * math.sqrt(252.0))

    worst_5d = np.nan
    if n >= 126:
        w = close.tail(126)
        r5 = w.pct_change(5)
        worst_5d = float(r5.min(skipna=True))

    mdd_6m = np.nan
    if n >= 126:
        mdd_6m = _max_drawdown(close.tail(126))

    adv20_usd = np.nan
    if n >= 20 and vol.notna().any():
        dv = (close.tail(20).reset_index(drop=True) * vol.tail(20).reset_index(drop=True))
        adv20_usd = float(dv.mean(skipna=True))

    zero_vol_frac = np.nan
    if n >= 126 and "Volume" in df.columns:
        wv = vol.tail(126)
        denom = len(wv)
        zero_vol_frac = float((wv.fillna(0.0) <= 0.0).sum() / denom) if denom else np.nan

    sma20 = sma(20)
    sma50 = sma(50)
    sma200 = sma(200)

    pct_days_above_sma200 = np.nan
    if n >= 200:
        w = close.tail(126) if n >= 126 else close
        s200 = close.rolling(200).mean().tail(len(w)).reset_index(drop=True)
        if len(w) == len(s200):
            pct_days_above_sma200 = float((w.reset_index(drop=True) > s200).mean())

    trend_slope_63d, trend_r2_63d = _trend_regression_stats(close, 63)
    forecast_return_21d = _forecast_return(close, 63, 21)
    rsi_14 = _rsi(close, 14)
    macd_line, macd_signal, macd_hist = _macd(close)
    hurst_100d = _hurst_exponent(close.tail(100), max_lag=20)
    autocorr_1_63d = _rolling_autocorr(close.pct_change().dropna(), lag=1, window=63)
    skew_126d = float(lr.tail(126).skew()) if n >= 126 else np.nan
    kurt_126d = float(lr.tail(126).kurtosis()) if n >= 126 else np.nan
    vol_of_vol_60d = float(lr.rolling(20).std().tail(60).std()) if n >= 80 else np.nan
    momentum_12m_minus_1m = horizon_return(252) - horizon_return(21)

    event_mean_impact, event_weighted_impact = _event_impact_scores(df, events, event_window)

    return {
        "symbol": symbol,
        "last_close": last_close,
        "price_under_10": bool(last_close <= 10.0),
        "return_1m": horizon_return(21),
        "return_3m": horizon_return(63),
        "return_6m": horizon_return(126),
        "return_12m": horizon_return(252),
        "sma20": sma20,
        "sma50": sma50,
        "sma200": sma200,
        "sma20_sma50": (sma20 / sma50) if sma20 and sma50 else np.nan,
        "sma50_sma200": (sma50 / sma200) if sma50 and sma200 else np.nan,
        "pct_days_above_sma200_6m": pct_days_above_sma200,
        "volatility_20d": ann_vol(20),
        "volatility_60d": ann_vol(60),
        "downside_volatility_60d": downside_vol(60),
        "worst_5d_return_6m": worst_5d,
        "max_drawdown_6m": mdd_6m,
        "adv20_usd": adv20_usd,
        "zero_volume_fraction_6m": zero_vol_frac,
        "trend_slope_63d": trend_slope_63d,
        "trend_r2_63d": trend_r2_63d,
        "forecast_return_21d": forecast_return_21d,
        "rsi_14": rsi_14,
        "macd_line": macd_line,
        "macd_signal": macd_signal,
        "macd_hist": macd_hist,
        "hurst_100d": hurst_100d,
        "autocorr_1_63d": autocorr_1_63d,
        "skew_126d": skew_126d,
        "kurtosis_126d": kurt_126d,
        "vol_of_vol_60d": vol_of_vol_60d,
        "momentum_12m_minus_1m": momentum_12m_minus_1m,
        "event_impact_mean": event_mean_impact,
        "event_impact_weighted": event_weighted_impact,
        "n_obs": int(n),
        "last_date": str(df["Date"].iloc[-1].date()) if "Date" in df.columns else "",
    }


def apply_gates(row: dict, min_history=252, min_price=1.0, min_adv_usd=100_000.0, max_price=None, max_zero_vol_frac=0.10):
    flags = []
    eligible = True

    if row.get("n_obs", 0) < min_history:
        eligible = False
        flags.append("insufficient_history")

    if row.get("last_close", np.nan) < min_price:
        eligible = False
        flags.append("price_below_min")

    if max_price is not None and row.get("last_close", np.nan) > float(max_price):
        eligible = False
        flags.append("price_above_cap")

    adv = row.get("adv20_usd", np.nan)
    if adv is None or np.isnan(adv) or adv < min_adv_usd:
        eligible = False
        flags.append("adv20_below_min")

    zv = row.get("zero_volume_fraction_6m", np.nan)
    if zv is not None and not np.isnan(zv) and zv > max_zero_vol_frac:
        eligible = False
        flags.append("too_many_zero_volume_days")

    return eligible, ";".join(flags)


def _series_or_zero(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([0.0] * len(df), index=df.index)
    return pd.to_numeric(df[col], errors="coerce")


def score_frame(df: pd.DataFrame) -> pd.DataFrame:
    m = (
        0.35 * _safe_zscore(_series_or_zero(df, "return_6m")) +
        0.25 * _safe_zscore(_series_or_zero(df, "return_12m")) +
        0.20 * _safe_zscore(_series_or_zero(df, "sma20_sma50")) +
        0.20 * _safe_zscore(_series_or_zero(df, "sma50_sma200"))
    )
    l = _safe_zscore(_series_or_zero(df, "adv20_usd")).clip(-2, 3)
    r = (
        0.50 * _safe_zscore(_series_or_zero(df, "volatility_20d")) +
        0.30 * _safe_zscore(_series_or_zero(df, "max_drawdown_6m").abs()) +
        0.20 * _safe_zscore(_series_or_zero(df, "worst_5d_return_6m").abs())
    )
    predictive = 0.5 * _safe_zscore(_series_or_zero(df, "forecast_return_21d")) + 0.5 * _safe_zscore(
        _series_or_zero(df, "trend_r2_63d")
    )

    raw = 0.30 * m + 0.30 * l - 0.25 * r + 0.15 * predictive

    out = df.copy()
    out["score_raw"] = raw
    out["predictive_signal"] = predictive

    eligible_mask = out["eligible"].astype(bool)
    if eligible_mask.sum() >= 5:
        ranks = out.loc[eligible_mask, "score_raw"].rank(pct=True)
        out.loc[eligible_mask, "monitor_score_1_10"] = (np.ceil(ranks * 10)).clip(1, 10).astype(int)
    else:
        out["monitor_score_1_10"] = np.where(eligible_mask, 5, 0)

    out.loc[~eligible_mask, "monitor_score_1_10"] = 0
    return out


def _summarize_stats(stats: dict) -> dict:
    summary = {}
    for provider, provider_stats in stats.items():
        summary[provider] = {
            "requests": provider_stats.requests,
            "success": provider_stats.success,
            "cache_hits": provider_stats.cache_hits,
            "errors": provider_stats.errors,
            "last_error": provider_stats.last_error,
        }
    return summary


def _format_stats_markdown(stats: dict) -> str:
    rows = []
    for provider, provider_stats in stats.items():
        error_pairs = ", ".join(f"{k}:{v}" for k, v in sorted(provider_stats.errors.items()))
        rows.append(
            {
                "provider": provider,
                "requests": provider_stats.requests,
                "success": provider_stats.success,
                "cache_hits": provider_stats.cache_hits,
                "errors": error_pairs or "-",
                "last_error": provider_stats.last_error or "-",
            }
        )
    df = pd.DataFrame(rows)
    return df.to_markdown(index=False)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--watchlist", required=True)
    ap.add_argument("--outdir", default="outputs")
    ap.add_argument("--use-finnhub", action="store_true")
    ap.add_argument("--history-provider", choices=["auto", "stooq", "twelvedata"], default="auto")
    ap.add_argument("--cache-dir", default="data_cache/watchlist")
    ap.add_argument("--offline", action="store_true")
    ap.add_argument("--events-file", default=None)
    ap.add_argument("--event-window-days", type=int, default=5)
    ap.add_argument("--min-history", type=int, default=252)
    ap.add_argument("--min-price", type=float, default=1.0)
    ap.add_argument("--min-adv-usd", type=float, default=100_000.0)
    ap.add_argument("--max-price", type=float, default=None)
    ap.add_argument("--max-zero-vol-frac", type=float, default=0.10)
    ap.add_argument("--finnhub-sleep-sec", type=float, default=1.2)
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    symbols, themes = _read_watchlist(args.watchlist)

    finnhub_key = os.getenv("FINNHUB_API_KEY", "").strip()
    td_key = os.getenv("TWELVEDATA_API_KEY", "").strip()
    td_sleep = float(os.getenv("TWELVEDATA_SLEEP_SEC", "8"))

    stooq_ok = _stooq_reachable() if args.history_provider in ("auto", "stooq") else False
    if args.history_provider == "stooq":
        stooq_ok = True

    events = _load_events(args.events_file)
    stats = _init_provider_stats()

    print(f"History provider mode: {args.history_provider}", flush=True)
    print(f"Stooq reachable: {stooq_ok}", flush=True)
    print(f"Offline mode: {args.offline}", flush=True)
    if (args.history_provider in ("auto", "twelvedata")) and (not td_key):
        print("WARNING: TWELVEDATA_API_KEY not set; TwelveData history will fail.", flush=True)

    rows = []
    quote_rows = []

    for i, sym in enumerate(symbols, 1):
        print(f"[{i}/{len(symbols)}] {sym}", flush=True)
        try:
            df = None
            src = None

            if args.offline:
                providers = [args.history_provider] if args.history_provider != "auto" else ["stooq", "twelvedata"]
                for provider in providers:
                    cached = _read_cached_history(args.cache_dir, sym, provider)
                    if cached is not None:
                        stats["cache"].cache_hits += 1
                        df = cached
                        src = f"cache:{provider}"
                        break
                if df is None:
                    raise RuntimeError("offline_cache_miss")
            else:
                if args.history_provider == "twelvedata":
                    if not td_key:
                        raise RuntimeError("no_twelvedata_key")
                    df = _fetch_twelvedata_ohlcv(sym, td_key, stats)
                    src = "twelvedata"
                    _write_cache(args.cache_dir, sym, "twelvedata", df)
                    time.sleep(td_sleep)
                else:
                    if stooq_ok:
                        try:
                            df = _fetch_stooq_ohlcv(sym, stats)
                            src = "stooq"
                            _write_cache(args.cache_dir, sym, "stooq", df)
                        except Exception:
                            df = None
                            src = None

                    if df is None:
                        if not td_key:
                            raise RuntimeError("stooq_failed_and_no_twelvedata_key")
                        df = _fetch_twelvedata_ohlcv(sym, td_key, stats)
                        src = "twelvedata"
                        _write_cache(args.cache_dir, sym, "twelvedata", df)
                        time.sleep(td_sleep)

            feat = compute_features(sym, df, events=events, event_window=args.event_window_days)
            feat["themes"] = themes.get(sym, "")
            feat["data_source"] = src

            eligible, risk_flags = apply_gates(
                feat,
                min_history=args.min_history,
                min_price=args.min_price,
                min_adv_usd=args.min_adv_usd,
                max_price=args.max_price,
                max_zero_vol_frac=args.max_zero_vol_frac,
            )
            feat["eligible"] = bool(eligible)
            feat["risk_flags"] = risk_flags
            rows.append(feat)

        except Exception as e:
            rows.append({
                "symbol": sym,
                "themes": themes.get(sym, ""),
                "eligible": False,
                "risk_flags": f"data_error:{type(e).__name__}",
                "error": str(e),
                "data_source": "",
            })

        if args.use_finnhub and finnhub_key and not args.offline:
            try:
                q = _fetch_finnhub_quote(sym, finnhub_key, stats)
                quote_rows.append({
                    "symbol": sym,
                    "finnhub_c": q.get("c", np.nan),
                    "finnhub_t": q.get("t", np.nan),
                    "finnhub_dp": q.get("dp", np.nan),
                })
            except Exception:
                quote_rows.append({"symbol": sym, "finnhub_c": np.nan, "finnhub_t": np.nan, "finnhub_dp": np.nan})
            time.sleep(max(0.0, args.finnhub_sleep_sec))

    feat_df = pd.DataFrame(rows)

    if quote_rows:
        qdf = pd.DataFrame(quote_rows)
        feat_df = feat_df.merge(qdf, on="symbol", how="left")
        feat_df["spot_price"] = pd.to_numeric(feat_df.get("finnhub_c"), errors="coerce")
        feat_df["spot_price"] = feat_df["spot_price"].where(feat_df["spot_price"].notna(), feat_df.get("last_close"))
    else:
        feat_df["spot_price"] = feat_df.get("last_close")

    for col in [
        "return_1m",
        "return_3m",
        "return_6m",
        "return_12m",
        "sma20_sma50",
        "sma50_sma200",
        "volatility_20d",
        "volatility_60d",
        "downside_volatility_60d",
        "worst_5d_return_6m",
        "max_drawdown_6m",
        "adv20_usd",
        "zero_volume_fraction_6m",
        "spot_price",
        "trend_slope_63d",
        "trend_r2_63d",
        "forecast_return_21d",
        "rsi_14",
        "macd_line",
        "macd_signal",
        "macd_hist",
        "hurst_100d",
        "autocorr_1_63d",
        "skew_126d",
        "kurtosis_126d",
        "vol_of_vol_60d",
        "momentum_12m_minus_1m",
        "event_impact_mean",
        "event_impact_weighted",
    ]:
        if col in feat_df.columns:
            feat_df[col] = pd.to_numeric(feat_df[col], errors="coerce")

    scored = score_frame(feat_df)

    features_path = os.path.join(args.outdir, "features_watchlist.csv")
    scored_path = os.path.join(args.outdir, "scored_watchlist.csv")
    report_path = os.path.join(args.outdir, "report_watchlist.md")
    stats_path = os.path.join(args.outdir, "provider_stats.json")

    feat_df.to_csv(features_path, index=False)
    scored.sort_values(["monitor_score_1_10", "spot_price"], ascending=[False, True]).to_csv(scored_path, index=False)

    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    eligible_ct = int(scored["eligible"].sum()) if "eligible" in scored.columns else 0
    total_ct = int(len(scored))
    top = scored[scored["eligible"] == True].sort_values("monitor_score_1_10", ascending=False).head(15)

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("# Watchlist run report\n\n")
        f.write(f"- Generated: {now}\n")
        f.write(f"- Symbols: {total_ct}\n")
        f.write(f"- Eligible: {eligible_ct}\n\n")
        f.write("## Top eligible (by monitor_score_1_10)\n\n")
        if len(top) == 0:
            f.write("_No eligible symbols under current gates._\n")
        else:
            cols = [
                "symbol",
                "themes",
                "data_source",
                "monitor_score_1_10",
                "spot_price",
                "adv20_usd",
                "return_6m",
                "return_12m",
                "forecast_return_21d",
                "trend_r2_63d",
                "event_impact_mean",
                "volatility_20d",
                "max_drawdown_6m",
                "risk_flags",
            ]
            cols = [c for c in cols if c in top.columns]
            f.write(top[cols].to_markdown(index=False))
            f.write("\n\n")

        f.write("## Provider stats\n\n")
        f.write(_format_stats_markdown(stats))
        f.write("\n")

    with open(stats_path, "w", encoding="utf-8") as f:
        json.dump(_summarize_stats(stats), f, indent=2)

    print(f"Wrote: {features_path}", flush=True)
    print(f"Wrote: {scored_path}", flush=True)
    print(f"Wrote: {report_path}", flush=True)
    print(f"Wrote: {stats_path}", flush=True)


if __name__ == "__main__":
    from market_monitor.run_watchlist import main as run_watchlist_main

    run_watchlist_main()
