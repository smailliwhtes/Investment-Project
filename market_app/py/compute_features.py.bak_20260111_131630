import argparse, json
import numpy as np
import pandas as pd

def ulcer_index(close: pd.Series, window: int = 252) -> float:
    roll_max = close.rolling(window, min_periods=window).max()
    dd = (close / roll_max - 1.0) * 100.0
    ui = np.sqrt((dd.pow(2)).rolling(window, min_periods=window).mean())
    return float(ui.iloc[-1]) if not np.isnan(ui.iloc[-1]) else np.nan

def max_drawdown(close: pd.Series, window: int = 252) -> float:
    roll_max = close.rolling(window, min_periods=window).max()
    dd = close / roll_max - 1.0
    return float(dd.iloc[-window:].min()) if len(dd.dropna()) >= window else np.nan

def var_cvar(returns: pd.Series, alpha: float = 0.05) -> tuple[float, float]:
    r = returns.dropna().values
    if r.size < 50:
        return (np.nan, np.nan)
    var = np.quantile(r, alpha)
    cvar = r[r <= var].mean() if np.any(r <= var) else np.nan
    return float(var), float(cvar)

ap = argparse.ArgumentParser()
ap.add_argument("--symbol", required=True)
ap.add_argument("--stooq_csv", required=True)
ap.add_argument("--price", type=float, default=np.nan)
ap.add_argument("--quote_unix", type=float, default=np.nan)
ap.add_argument("--config", required=True)
args = ap.parse_args()

cfg = json.loads(open(args.config, "r", encoding="utf-8-sig").read())

df = pd.read_csv(args.stooq_csv)
df.columns = [c.strip().lower() for c in df.columns]
# Stooq typical columns: date, open, high, low, close, volume
df["date"] = pd.to_datetime(df["date"])
df = df.sort_values("date").reset_index(drop=True)

close = df["close"].astype(float)
high  = df["high"].astype(float)
low   = df["low"].astype(float)
vol   = df["volume"].astype(float)

last_close = float(close.iloc[-1])

# Price gate uses latest official close by default; if Finnhub quote exists, keep it as "price_live"
price_live = args.price if args.price and np.isfinite(args.price) else np.nan
price_for_gates = price_live if np.isfinite(price_live) else last_close

# Returns
log_ret = np.log(close).diff()

# ADV20 ($)
adv20 = (close * vol).rolling(20, min_periods=20).mean().iloc[-1]

# ATR14
prev_close = close.shift(1)
tr = np.maximum.reduce([
    (high - low).values,
    (high - prev_close).abs().values,
    (low - prev_close).abs().values
])
atr14 = pd.Series(tr, index=df.index).rolling(14, min_periods=14).mean().iloc[-1]

# sigma20: std of log returns over 20 days (daily units)
sigma20 = log_ret.rolling(20, min_periods=20).std(ddof=1).iloc[-1]

# Momentum ROC
roc20 = 100.0 * (close.iloc[-1] / close.shift(20).iloc[-1] - 1.0) if len(close) > 20 else np.nan
roc60 = 100.0 * (close.iloc[-1] / close.shift(60).iloc[-1] - 1.0) if len(close) > 60 else np.nan

# Moving averages
sma20  = close.rolling(20,  min_periods=20).mean().iloc[-1]
sma50  = close.rolling(50,  min_periods=50).mean().iloc[-1]
sma200 = close.rolling(200, min_periods=200).mean().iloc[-1]

# Drawdown & Ulcer (1y ~ 252 trading days)
mdd_1y = max_drawdown(close, window=252)
ui_1y  = ulcer_index(close, window=252)

# VaR/CVaR on horizon returns (default 10d)
h = int(cfg.get("var_horizon_days", 10))
hret = close.pct_change(h)
var_h, cvar_h = var_cvar(hret, alpha=float(cfg.get("var_alpha", 0.05)))

out = {
    "symbol": args.symbol,
    "date": df["date"].iloc[-1].date().isoformat(),
    "price_live": None if not np.isfinite(price_live) else float(price_live),
    "price_for_gates": float(price_for_gates),
    "last_close": last_close,
    "history_days": int(df.shape[0]),

    "adv20_dollar": None if np.isnan(adv20) else float(adv20),
    "atr14": None if np.isnan(atr14) else float(atr14),
    "sigma20": None if np.isnan(sigma20) else float(sigma20),
    "roc20": None if np.isnan(roc20) else float(roc20),
    "roc60": None if np.isnan(roc60) else float(roc60),
    "sma20": None if np.isnan(sma20) else float(sma20),
    "sma50": None if np.isnan(sma50) else float(sma50),
    "sma200": None if np.isnan(sma200) else float(sma200),
    "mdd_1y": None if np.isnan(mdd_1y) else float(mdd_1y),
    "ulcer_1y": None if np.isnan(ui_1y) else float(ui_1y),
    "var_h": None if np.isnan(var_h) else float(var_h),
    "cvar_h": None if np.isnan(cvar_h) else float(cvar_h),

    "quote_unix": None if not np.isfinite(args.quote_unix) else int(args.quote_unix)
}

print(json.dumps(out))
