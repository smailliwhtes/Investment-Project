import argparse
import datetime as dt
import math
import os

# --- .env autoload (python-dotenv) ---
try:
    from dotenv import load_dotenv  # python-dotenv
    _DOTENV = os.path.join(os.path.dirname(__file__), "..", ".env")
    load_dotenv(_DOTENV)
except Exception:
    pass
# --- end .env autoload ---
import time
import socket
from io import StringIO

import numpy as np
import pandas as pd
import requests


STOOQ_URL = "https://stooq.com/q/d/l/?s={symbol}.us&i=d"
FINNHUB_QUOTE_URL = "https://finnhub.io/api/v1/quote"


def _read_watchlist(path: str):
    """Parses watchlist with section headers starting with '#'.
    Returns: list(symbols), dict(symbol->semicolon-separated theme tags)
    """
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
    # Deduplicate while preserving order
    seen = set()
    symbols = []
    for s in order:
        if s not in seen:
            seen.add(s)
            symbols.append(s)
    return symbols, themes_joined


def _fetch_stooq_ohlcv(symbol: str, timeout=30) -> pd.DataFrame:
    url = STOOQ_URL.format(symbol=symbol.lower())
    r = requests.get(url, timeout=timeout, headers={"User-Agent": "watchlist-mode/1.0"})
    r.raise_for_status()
    txt = r.text.strip()
    if not txt or txt.lower().startswith("<!doctype"):
        raise ValueError("Stooq returned non-CSV content")
    df = pd.read_csv(StringIO(txt))
    # Expected columns: Date, Open, High, Low, Close, Volume
    if "Date" not in df.columns or "Close" not in df.columns:
        raise ValueError(f"Unexpected Stooq schema for {symbol}: {df.columns.tolist()}")
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"]).sort_values("Date")
    for c in ["Open", "High", "Low", "Close", "Volume"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["Close"])
    return df


def _fetch_finnhub_quote(symbol: str, api_key: str, timeout=20) -> dict:
    params = {"symbol": symbol, "token": api_key}
    r = requests.get(FINNHUB_QUOTE_URL, params=params, timeout=timeout, headers={"User-Agent": "watchlist-mode/1.0"})
    r.raise_for_status()
    j = r.json()
    # Finnhub returns: c (current), t (unix), etc.
    return j


def _max_drawdown(series: pd.Series) -> float:
    # returns negative number (e.g., -0.35 for -35%)
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


def _fetch_twelvedata_ohlcv(symbol: str, api_key: str, timeout=20, outputsize=5000) -> pd.DataFrame:
    params = {
        "symbol": symbol,
        "interval": "1day",
        "outputsize": outputsize,
        "apikey": api_key,
        "format": "JSON",
    }
    r = requests.get(
        TWELVEDATA_TS_URL,
        params=params,
        timeout=(3, timeout),
        headers={"User-Agent": "watchlist-mode/1.0"},
    )
    r.raise_for_status()
    j = r.json()

    if isinstance(j, dict) and j.get("status") == "error":
        raise ValueError(f"TwelveData error: {j.get('message')}")

    values = j.get("values") if isinstance(j, dict) else None
    if not values:
        raise ValueError("TwelveData missing 'values'")

    df = pd.DataFrame(values)
    # Twelve Data returns newest-first; normalize to expected schema
    if "datetime" in df.columns:
        df = df.rename(columns={"datetime": "Date"})
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

    # Standardize column names to match Stooq schema expectations
    rename_map = {
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "volume": "Volume",
    }
    for k, v in rename_map.items():
        if k in df.columns and v not in df.columns:
            df = df.rename(columns={k: v})

    for col in ["Open", "High", "Low", "Close", "Volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["Date", "Close"]).sort_values("Date")
    return df
def compute_features(symbol: str, df: pd.DataFrame) -> dict:
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

    # log returns for volatility
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

    # worst 5D return over last 6m (~126d): min rolling 5-day return
    worst_5d = np.nan
    if n >= 6 + 126:
        w = close.tail(126)
        r5 = w.pct_change(5)
        worst_5d = float(r5.min(skipna=True))

    # max drawdown over last 6m
    mdd_6m = np.nan
    if n >= 126:
        mdd_6m = _max_drawdown(close.tail(126))

    # ADV20$ = mean(close*volume) over last 20
    adv20_usd = np.nan
    if n >= 20 and vol.notna().any():
        dv = (close.tail(20).reset_index(drop=True) * vol.tail(20).reset_index(drop=True))
        adv20_usd = float(dv.mean(skipna=True))

    # zero-volume fraction over last 126
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
        "n_obs": int(n),
        "last_date": str(df["Date"].iloc[-1].date()) if "Date" in df.columns else "",
    }


def apply_gates(row: dict, min_history=252, min_price=1.0, min_adv_usd=100_000.0, max_price=None, max_zero_vol_frac=0.10):
    flags = []
    eligible = True

    if row["n_obs"] < min_history:
        eligible = False
        flags.append("insufficient_history")

    if row["last_close"] < min_price:
        eligible = False
        flags.append("price_below_min")

    if max_price is not None and row["last_close"] > float(max_price):
        # gate only if user wants it
        eligible = False
        flags.append("price_above_cap")

    if (row["adv20_usd"] is None) or (np.isnan(row["adv20_usd"])) or (row["adv20_usd"] < min_adv_usd):
        eligible = False
        flags.append("adv20_below_min")

    zv = row.get("zero_volume_fraction_6m", np.nan)
    if zv is not None and not np.isnan(zv) and zv > max_zero_vol_frac:
        eligible = False
        flags.append("too_many_zero_volume_days")

    return eligible, ";".join(flags)


def score_frame(df: pd.DataFrame) -> pd.DataFrame:
    # Conservative monitor score (1–10) using z-scored components.
    # Momentum: returns + MA ratios
    m = (
        0.35 * _safe_zscore(df["return_6m"]) +
        0.25 * _safe_zscore(df["return_12m"]) +
        0.20 * _safe_zscore(df["sma20_sma50"]) +
        0.20 * _safe_zscore(df["sma50_sma200"])
    )

    # Liquidity: adv20$
    l = _safe_zscore(df["adv20_usd"]).clip(-2, 3)

    # Risk penalty: vol + drawdown + worst 5d (absolute)
    r = (
        0.50 * _safe_zscore(df["volatility_20d"]) +
        0.30 * _safe_zscore(df["max_drawdown_6m"].abs()) +
        0.20 * _safe_zscore(df["worst_5d_return_6m"].abs())
    )

    raw = 0.35 * m + 0.35 * l - 0.30 * r

    # Map to 1–10 by deciles on eligible subset
    out = df.copy()
    out["score_raw"] = raw

    eligible_mask = out["eligible"].astype(bool)
    if eligible_mask.sum() >= 5:
        ranks = out.loc[eligible_mask, "score_raw"].rank(pct=True)
        out.loc[eligible_mask, "monitor_score_1_10"] = (np.ceil(ranks * 10)).clip(1, 10).astype(int)
    else:
        out["monitor_score_1_10"] = np.where(eligible_mask, 5, 0)

    out.loc[~eligible_mask, "monitor_score_1_10"] = 0
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--watchlist", required=True)
    ap.add_argument("--outdir", default="outputs")
    ap.add_argument("--use-finnhub", action="store_true")
    ap.add_argument("--min-history", type=int, default=252)
    ap.add_argument("--min-price", type=float, default=1.0)
    ap.add_argument("--min-adv-usd", type=float, default=100_000.0)
    ap.add_argument("--max-price", type=float, default=None)  # set e.g. 10.0 to enforce
    ap.add_argument("--max-zero-vol-frac", type=float, default=0.10)
    ap.add_argument("--finnhub-sleep-sec", type=float, default=1.2)  # stay under 60/min
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    symbols, themes = _read_watchlist(args.watchlist)

    rows = []
    quote_rows = []
    api_key = os.getenv("FINNHUB_API_KEY", "").strip()
    td_key = os.getenv("TWELVEDATA_API_KEY", "").strip()

    # One-time reachability test: avoid hanging on blocked Stooq
    stooq_ok = True
    try:
        s = socket.create_connection(("stooq.com", 443), timeout=2)
        s.close()
    except Exception:
        stooq_ok = False

    print(f"Stooq reachable: {stooq_ok}", flush=True)
    if (not stooq_ok) and (not td_key):
        print("WARNING: Stooq blocked and TWELVEDATA_API_KEY not set; history pulls will fail.", flush=True)

    for i, sym in enumerate(symbols, 1):
        try:
        df = None
        src = None

        if stooq_ok:
            try:
                df = _fetch_stooq_ohlcv(sym)
                src = "stooq"
            except Exception:
                df = None
                src = None

        if df is None:
            if not td_key:
                raise RuntimeError("stooq_unreachable_and_no_twelvedata_key")
            df = _fetch_twelvedata_ohlcv(sym, td_key)
            src = "twelvedata"
            # Free-tier pacing (override via TWELVEDATA_SLEEP_SEC in env/.env)
            time.sleep(float(os.getenv("TWELVEDATA_SLEEP_SEC", "8")))
            feat = compute_features(sym, df)
            feat["themes"] = themes.get(sym, "")
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
            })

        if args.use_finnhub and api_key:
            try:
                q = _fetch_finnhub_quote(sym, api_key)
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

    # Merge quotes if present
    if quote_rows:
        qdf = pd.DataFrame(quote_rows)
        feat_df = feat_df.merge(qdf, on="symbol", how="left")
        # Prefer finnhub current if present; else last_close
        feat_df["spot_price"] = pd.to_numeric(feat_df.get("finnhub_c"), errors="coerce")
        feat_df["spot_price"] = feat_df["spot_price"].where(feat_df["spot_price"].notna(), feat_df.get("last_close"))
    else:
        feat_df["spot_price"] = feat_df.get("last_close")

    # Ensure numeric cols
    for col in ["return_1m","return_3m","return_6m","return_12m","sma20_sma50","sma50_sma200",
                "volatility_20d","volatility_60d","downside_volatility_60d","worst_5d_return_6m",
                "max_drawdown_6m","adv20_usd","zero_volume_fraction_6m","spot_price"]:
        if col in feat_df.columns:
            feat_df[col] = pd.to_numeric(feat_df[col], errors="coerce")

    scored = score_frame(feat_df)

    features_path = os.path.join(args.outdir, "features_watchlist.csv")
    scored_path = os.path.join(args.outdir, "scored_watchlist.csv")
    report_path = os.path.join(args.outdir, "report_watchlist.md")

    feat_df.to_csv(features_path, index=False)
    scored.sort_values(["monitor_score_1_10","spot_price"], ascending=[False, True]).to_csv(scored_path, index=False)

    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    eligible_ct = int(scored["eligible"].sum()) if "eligible" in scored.columns else 0
    total_ct = int(len(scored))
    top = scored[scored["eligible"] == True].sort_values("monitor_score_1_10", ascending=False).head(15)

    with open(report_path, "w", encoding="utf-8") as f:
        f.write(f"# Watchlist run report\n\n")
        f.write(f"- Generated: {now}\n")
        f.write(f"- Symbols: {total_ct}\n")
        f.write(f"- Eligible: {eligible_ct}\n\n")
        f.write("## Top eligible (by monitor_score_1_10)\n\n")
        if len(top) == 0:
            f.write("_No eligible symbols under current gates._\n")
        else:
            cols = ["symbol","themes","monitor_score_1_10","spot_price","adv20_usd","return_6m","return_12m","volatility_20d","max_drawdown_6m","risk_flags"]
            cols = [c for c in cols if c in top.columns]
            f.write(top[cols].to_markdown(index=False))
            f.write("\n")

    print(f"Wrote: {features_path}")
    print(f"Wrote: {scored_path}")
    print(f"Wrote: {report_path}")


if __name__ == "__main__":
    main()



