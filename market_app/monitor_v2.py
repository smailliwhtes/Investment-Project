import argparse, json, os, re, time
from datetime import datetime
from typing import Dict, List, Tuple, Optional

import numpy as np
import pandas as pd
import requests

NASDAQLISTED_URL = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
OTHERLISTED_URL  = "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt"
STOOQ_URL_TMPL   = "https://stooq.com/q/d/l/?s={symbol}&i=d"

THEMES: Dict[str, List[str]] = {
    "prime defense & aerospace": ["defense", "defence", "aerospace", "missile", "munitions", "ordnance", "armament"],
    "weapons systems / munitions / guidance": ["missile", "munition", "guidance", "rocket", "warhead", "torpedo"],
    "defense electronics & sensors": ["radar", "sensor", "avionics", "electro-optic", "rf", "microwave"],
    "space/launch & satcom (dual-use)": ["space", "satellite", "satcom", "launch", "rocket", "orbit"],
    "military logistics & services": ["logistics", "defense services", "defence services", "tactical", "armored"],
    "semiconductors": ["semiconductor", "chip", "microelectronics", "fab", "foundry"],
    "AI compute/data center infrastructure": ["ai", "data center", "datacenter", "cloud", "gpu", "compute"],
    "cybersecurity": ["cyber", "security", "zero trust", "threat", "endpoint"],
    "communications (RF, photonics, fiber)": ["fiber", "fibre", "photon", "optical", "coherent", "rf", "microwave"],
    "industrial automation/robotics": ["robot", "robotics", "automation", "industrial control", "factory"],
    "energy grid modernization": ["grid", "power systems", "electrical equipment", "transformer", "substation"],
    "copper / aluminum / steel inputs": ["copper", "aluminum", "aluminium", "steel"],
    "lithium / nickel / cobalt / graphite / manganese": ["lithium", "nickel", "cobalt", "graphite", "manganese"],
    "rare earths & magnets": ["rare earth", "neodymium", "praseodymium", "dysprosium", "magnet"],
    "uranium / nuclear fuel cycle": ["uranium", "nuclear", "fuel cycle", "reactor"],
    "precious metals / strategic sensitivity": ["gold", "silver", "platinum", "palladium"],
}

ASSETTYPE_EXCLUDE_PATTERNS = [
    r"\bwarrant(s)?\b", r"\bright(s)?\b", r"\bunit(s)?\b", r"\bpreferred\b",
    r"\bdepositary\b", r"\bnote(s)?\b", r"\bdebenture(s)?\b"
]

UA = {"User-Agent": "market-monitor/2.0 (monitoring only)"}

def run_id() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def safe_read_text(url: str, timeout: int = 30) -> str:
    r = requests.get(url, headers=UA, timeout=timeout)
    r.raise_for_status()
    return r.text

def parse_pipe_file(text: str) -> pd.DataFrame:
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    lines = [ln for ln in lines if not ln.lower().startswith("file creation time")]
    header = lines[0].split("|")
    rows = [ln.split("|") for ln in lines[1:]]
    return pd.DataFrame(rows, columns=header)

def normalize_symbol(sym: str) -> str:
    return sym.strip().upper()

def is_excluded_asset(security_name: str) -> bool:
    s = (security_name or "").lower()
    return any(re.search(pat, s) for pat in ASSETTYPE_EXCLUDE_PATTERNS)

def theme_tag(security_name: str) -> Tuple[List[str], List[float], List[str]]:
    name = (security_name or "").lower()
    labels, confs, evidence = [], [], []
    for theme, kws in THEMES.items():
        hits = [kw for kw in kws if kw in name]
        if hits:
            labels.append(theme)
            confs.append(min(1.0, len(hits) / max(3, len(kws))))
            evidence.append(", ".join(sorted(set(hits))[:8]))
    return labels, confs, evidence

def stooq_symbol(symbol: str) -> str:
    sym = symbol.lower()
    return sym if sym.endswith(".us") else f"{sym}.us"

def fetch_stooq_history(symbol: str, cache_dir: str) -> Optional[pd.DataFrame]:
    os.makedirs(cache_dir, exist_ok=True)
    sym = stooq_symbol(symbol)
    cache_path = os.path.join(cache_dir, f"{symbol}.csv")

    if os.path.exists(cache_path):
        mtime = datetime.fromtimestamp(os.path.getmtime(cache_path))
        if mtime.date() == datetime.now().date():
            try:
                return pd.read_csv(cache_path)
            except Exception:
                pass

    url = STOOQ_URL_TMPL.format(symbol=sym)
    try:
        r = requests.get(url, headers=UA, timeout=30)
        if r.status_code != 200 or len(r.text) < 50:
            return None
        with open(cache_path, "w", encoding="utf-8") as f:
            f.write(r.text)
        return pd.read_csv(cache_path)
    except Exception:
        return None

def compute_max_drawdown(prices: np.ndarray) -> float:
    if len(prices) < 2:
        return np.nan
    peak = np.maximum.accumulate(prices)
    dd = (prices / peak) - 1.0
    return float(np.min(dd))

def pct_rank(series: pd.Series, higher_is_better: bool = True) -> pd.Series:
    s = series.astype(float).copy()
    lo, hi = np.nanpercentile(s, [5, 95])
    s = s.clip(lo, hi)
    r = s.rank(pct=True)
    return r if higher_is_better else (1.0 - r)

def build_universe() -> pd.DataFrame:
    ntext = safe_read_text(NASDAQLISTED_URL)
    otext = safe_read_text(OTHERLISTED_URL)

    nd = parse_pipe_file(ntext)
    od = parse_pipe_file(otext)

    nd = nd.rename(columns={"Symbol": "symbol", "Security Name": "security_name", "Test Issue": "test_issue",
                            "Financial Status": "financial_status", "ETF": "etf"})
    nd["exchange"] = "NASDAQ"
    nd["symbol"] = nd["symbol"].map(normalize_symbol)

    od = od.rename(columns={"CQS Symbol": "symbol", "Security Name": "security_name", "Test Issue": "test_issue",
                            "ETF": "etf", "Exchange": "exchange"})
    od["symbol"] = od["symbol"].map(normalize_symbol)

    uni = pd.concat([
        nd[["symbol", "security_name", "exchange", "test_issue", "financial_status", "etf"]],
        od[["symbol", "security_name", "exchange", "test_issue", "etf"]].assign(financial_status="")
    ], ignore_index=True)

    uni = uni.dropna(subset=["symbol"]).drop_duplicates(subset=["symbol"], keep="first")
    uni["test_issue"] = uni["test_issue"].fillna("N")
    uni = uni[uni["test_issue"].str.upper().ne("Y")].copy()

    uni["security_name"] = uni["security_name"].fillna("")
    uni = uni[~uni["security_name"].apply(is_excluded_asset)].copy()
    uni["is_etf"] = uni["etf"].fillna("N").str.upper().eq("Y")
    return uni.reset_index(drop=True)

def compute_features_for_symbol(hist: pd.DataFrame) -> Dict[str, float]:
    df = hist.copy()
    if "Date" not in df.columns or "Close" not in df.columns:
        return {"_missing_price_data": 1.0}

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"]).sort_values("Date")
    for c in ["Open", "High", "Low", "Close", "Volume"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["Close"])
    if len(df) < 260:
        return {"_insufficient_history": 1.0}

    close = df["Close"].to_numpy(dtype=float)
    vol = df["Volume"].fillna(0).to_numpy(dtype=float)

    def ret_n(n: int) -> float:
        if len(close) <= n:
            return np.nan
        return float(close[-1] / close[-1 - n] - 1.0)

    r1m, r3m, r6m, r12m = ret_n(21), ret_n(63), ret_n(126), ret_n(252)

    sma20 = pd.Series(close).rolling(20).mean().iloc[-1]
    sma50 = pd.Series(close).rolling(50).mean().iloc[-1]
    sma200 = pd.Series(close).rolling(200).mean().iloc[-1]

    sma20_sma50 = float(sma20 / sma50 - 1.0) if sma50 and not np.isnan(sma50) else np.nan
    sma50_sma200 = float(sma50 / sma200 - 1.0) if sma200 and not np.isnan(sma200) else np.nan

    cser = pd.Series(close)
    sma200_series = cser.rolling(200).mean()
    tail = pd.DataFrame({"c": cser, "sma200": sma200_series}).iloc[-252:]
    pct_above = float((tail["c"] > tail["sma200"]).mean())

    lr = np.diff(np.log(close))
    vol20 = float(np.nanstd(lr[-20:], ddof=1) * np.sqrt(252)) if len(lr) >= 20 else np.nan
    vol60 = float(np.nanstd(lr[-60:], ddof=1) * np.sqrt(252)) if len(lr) >= 60 else np.nan

    neg = lr[lr < 0]
    downside = float(np.nanstd(neg, ddof=1) * np.sqrt(252)) if len(neg) >= 10 else np.nan

    worst5 = float(np.min(close[-252:] / close[-257:-5] - 1.0)) if len(close) >= 257 else np.nan
    adv20 = float(np.nanmean((close[-20:] * vol[-20:])))
    zero_vol_frac = float(np.mean(vol[-252:] <= 0))
    mdd6m = compute_max_drawdown(close[-126:]) if len(close) >= 126 else np.nan

    return {
        "return_1m": r1m,
        "return_3m": r3m,
        "return_6m": r6m,
        "return_12m": r12m,
        "sma20_sma50": sma20_sma50,
        "sma50_sma200": sma50_sma200,
        "pct_days_above_sma200": pct_above,
        "volatility_20d": vol20,
        "volatility_60d": vol60,
        "downside_volatility": downside,
        "worst_5d_return": worst5,
        "adv20_usd": adv20,
        "zero_volume_fraction": zero_vol_frac,
        "last_price": float(close[-1]),
        "max_drawdown_6m": mdd6m,
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe", choices=["themed", "all", "watchlist"], default="themed")
    ap.add_argument("--watchlist", default="watchlist.txt")
    ap.add_argument("--outdir", default="output")
    ap.add_argument("--price-cap", type=float, default=10.0)
    ap.add_argument("--price-floor", type=float, default=1.0)
    ap.add_argument("--adv-min-usd", type=float, default=100000.0)
    ap.add_argument("--strict", action="store_true", help="If set, apply stricter tradability gates (DD/vol/zero-vol/breadth).")
    ap.add_argument("--max-dd-6m", type=float, default=-0.50)
    ap.add_argument("--vol60-max", type=float, default=1.25)
    ap.add_argument("--zero-vol-max", type=float, default=0.02)
    ap.add_argument("--pct-above-sma200-min", type=float, default=0.30)
    args = ap.parse_args()

    rid = run_id()
    os.makedirs(args.outdir, exist_ok=True)
    cache_dir = os.path.join(args.outdir, "..", "data_cache", "stooq")
    os.makedirs(cache_dir, exist_ok=True)

    uni = build_universe()

    labels, confs, evid = [], [], []
    for nm in uni["security_name"].tolist():
        l, c, e = theme_tag(nm)
        labels.append(l); confs.append(c); evid.append(e)

    uni["theme_labels"] = [json.dumps(x, ensure_ascii=False) for x in labels]
    uni["theme_confidences"] = [json.dumps(x, ensure_ascii=False) for x in confs]
    uni["theme_evidence"] = [json.dumps(x, ensure_ascii=False) for x in evid]

    if args.universe == "watchlist":
        if os.path.exists(args.watchlist):
            wl = [normalize_symbol(x) for x in open(args.watchlist, "r", encoding="utf-8").read().splitlines() if x.strip()]
            uni = uni[uni["symbol"].isin(wl)].copy()
        else:
            raise FileNotFoundError(f"Watchlist not found: {args.watchlist}")

    if args.universe == "themed":
        uni = uni[uni["theme_labels"].ne("[]")].copy()

    rows = []
    symbols = uni["symbol"].tolist()
    for i, sym in enumerate(symbols, start=1):
        hist = fetch_stooq_history(sym, cache_dir=cache_dir)
        feats = compute_features_for_symbol(hist) if hist is not None else {"_missing_price_data": 1.0}

        theme_list = json.loads(uni.loc[uni["symbol"] == sym, "theme_labels"].iloc[0])
        feats["theme_purity"] = 1.0 if len(theme_list) > 0 else 0.0

        rows.append({"symbol": sym, **feats})

        if i % 100 == 0:
            print(f"Processed {i} / {len(symbols)} symbols...")

        time.sleep(0.08)

    feat_df = pd.DataFrame(rows)
    out = uni.merge(feat_df, on="symbol", how="left")

    # Gates + flags
    risk_flags, eligible, reasons = [], [], []
    for _, r in out.iterrows():
        rf, fail = [], []

        if float(r.get("_missing_price_data", 0.0) or 0.0) == 1.0:
            fail.append("missing_price_or_volume")
            rf.append("RED: Missing OHLCV history (source coverage gap)")

        if float(r.get("_insufficient_history", 0.0) or 0.0) == 1.0:
            fail.append("insufficient_history")
            rf.append("RED: Insufficient history (<~260 trading days)")

        lp = r.get("last_price", np.nan)
        adv = r.get("adv20_usd", np.nan)

        if pd.notna(lp) and lp < args.price_floor:
            fail.append("price_below_min")
            rf.append(f"RED: Penny stock (price < ${args.price_floor:g})")

        if pd.notna(lp) and lp > args.price_cap:
            fail.append("price_above_cap")

        if pd.notna(adv) and adv < args.adv_min_usd:
            fail.append("adv20_below_min")
            rf.append(f"RED: Low liquidity (ADV20USD < ${args.adv_min_usd:,.0f})")

        # Institutional-style risk flags (do not necessarily disqualify unless --strict)
        vol60 = r.get("volatility_60d", np.nan)
        mdd6  = r.get("max_drawdown_6m", np.nan)
        w5    = r.get("worst_5d_return", np.nan)
        pab   = r.get("pct_days_above_sma200", np.nan)
        zv    = r.get("zero_volume_fraction", np.nan)

        if pd.notna(vol60) and vol60 >= 1.50:
            rf.append("RED: Extreme realized volatility (60D annualized)")
        elif pd.notna(vol60) and vol60 >= 1.00:
            rf.append("AMBER: High realized volatility (60D annualized)")

        if pd.notna(mdd6) and mdd6 <= -0.50:
            rf.append("RED: Deep 6M drawdown (<= -50%)")

        if pd.notna(w5) and w5 <= -0.45:
            rf.append("RED: Severe worst-5D return (<= -45%)")
        elif pd.notna(w5) and w5 <= -0.35:
            rf.append("AMBER: Large worst-5D return (<= -35%)")

        if pd.notna(pab) and pab < 0.20:
            rf.append("RED: Mostly below SMA200 (12M breadth)")
        elif pd.notna(pab) and pab < 0.35:
            rf.append("AMBER: Weak SMA200 breadth (12M)")

        if pd.notna(zv) and zv > 0.05:
            rf.append("RED: Frequent zero-volume prints (>5% of days)")
        elif pd.notna(zv) and zv > 0.02:
            rf.append("AMBER: Some zero-volume prints (>2% of days)")

        # Weak theme match flag
        try:
            confs = json.loads(r.get("theme_confidences", "[]"))
            if len(confs) > 0 and max(confs) < 0.25:
                rf.append("AMBER: Weak theme match (name keyword heuristic)")
        except Exception:
            pass

        if args.strict:
            if pd.notna(mdd6) and mdd6 <= args.max_dd_6m:
                fail.append("drawdown_too_deep")
            if pd.notna(vol60) and vol60 >= args.vol60_max:
                fail.append("volatility_too_high")
            if pd.notna(zv) and zv > args.zero_vol_max:
                fail.append("too_many_zero_volume_days")
            if pd.notna(pab) and pab < args.pct_above_sma200_min:
                fail.append("weak_sma200_breadth")

        ok = (len(fail) == 0)
        risk_flags.append(json.dumps(rf, ensure_ascii=False))
        eligible.append(bool(ok))
        reasons.append(json.dumps(fail, ensure_ascii=False))

    out["risk_flags"] = risk_flags
    out["eligible"] = eligible
    out["gate_fail_reasons"] = reasons

    # Risk-adjusted score (eligible only)
    score = pd.Series(np.nan, index=out.index, dtype=float)
    elig = out["eligible"].astype(bool)
    if elig.any():
        ed = out.loc[elig].copy()

        trend = (
            pct_rank(ed["return_6m"], True) +
            pct_rank(ed["return_12m"], True) +
            pct_rank(ed["sma20_sma50"], True) +
            pct_rank(ed["pct_days_above_sma200"], True)
        ) / 4.0

        liq = (
            pct_rank(ed["adv20_usd"], True) +
            pct_rank(ed["zero_volume_fraction"], False)
        ) / 2.0

        vol_pen = (
            pct_rank(ed["volatility_60d"], False) +
            pct_rank(ed["downside_volatility"], False)
        ) / 2.0

        dd_pen = (
            pct_rank(ed["max_drawdown_6m"], True) +
            pct_rank(ed["worst_5d_return"], True)
        ) / 2.0

        theme = pct_rank(ed["theme_purity"], True)

        raw = (0.35 * trend) + (0.20 * liq) + (0.20 * vol_pen) + (0.20 * dd_pen) + (0.05 * theme)
        dec = pd.qcut(raw.rank(method="first"), 10, labels=False) + 1
        score.loc[elig] = dec.astype(float)

    out["monitor_priority_score"] = score

    feature_cols = [
        "symbol","theme_labels","theme_confidences","theme_evidence",
        "return_1m","return_3m","return_6m","return_12m",
        "sma20_sma50","sma50_sma200","pct_days_above_sma200",
        "volatility_20d","volatility_60d","downside_volatility","worst_5d_return",
        "adv20_usd","zero_volume_fraction","last_price","max_drawdown_6m","theme_purity"
    ]
    score_cols = feature_cols + ["risk_flags","eligible","gate_fail_reasons","monitor_priority_score"]

    features_path = os.path.join(args.outdir, f"features_{rid}.csv")
    scored_path   = os.path.join(args.outdir, f"scored_{rid}.csv")
    eligible_path = os.path.join(args.outdir, f"eligible_{rid}.csv")

    out[feature_cols].to_csv(features_path, index=False)
    out[score_cols].to_csv(scored_path, index=False)
    out.loc[out["eligible"].astype(bool), score_cols].to_csv(eligible_path, index=False)

    print("Wrote:")
    print(" ", features_path)
    print(" ", scored_path)
    print(" ", eligible_path)

if __name__ == "__main__":
    main()
