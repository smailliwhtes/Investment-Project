import argparse, json, os, math, time
from datetime import datetime, timezone
from dateutil import tz
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests


def _p(msg: str) -> None:
    print(msg, flush=True)
NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
OTHER_LISTED_URL  = "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt"

def now_local(tz_name: str) -> datetime:
    return datetime.now(tz.gettz(tz_name))

def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def read_json(path: str, default):
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def write_json(path: str, obj) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)

def http_get_text(url: str, timeout: int = 30) -> str:
    r = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    return r.text

def parse_pipe_table(txt: str) -> pd.DataFrame:
    lines = [ln.strip() for ln in txt.splitlines() if ln.strip()]
    header_idx = None
    for i, ln in enumerate(lines):
        if "|" in ln and (ln.startswith("Symbol|") or ln.startswith("ACT Symbol|")):
            header_idx = i
            break
    if header_idx is None:
        raise ValueError("Could not find header row in symdir text.")

    header = lines[header_idx].split("|")
    rows = []
    for ln in lines[header_idx + 1:]:
        if ln.startswith("File Creation Time"):
            break
        parts = ln.split("|")
        if len(parts) != len(header):
            continue
        rows.append(parts)

    return pd.DataFrame(rows, columns=header)

def normalize_symbol(sym: str) -> str:
    sym = sym.strip().upper()
    if any(ch in sym for ch in ["^", "/", "\\"]):
        return ""
    return sym

def to_stooq_symbol(sym: str) -> str:
    return f"{sym.lower()}.us"

def stooq_csv_url(stooq_symbol: str) -> str:
    return f"https://stooq.com/q/d/l/?s={stooq_symbol}&i=d"

def file_is_fresh(path: str, refresh_hours: float) -> bool:
    if not os.path.exists(path):
        return False
    age_seconds = time.time() - os.path.getmtime(path)
    return age_seconds < refresh_hours * 3600.0

def load_stooq_daily(csv_path: str) -> Optional[pd.DataFrame]:
    if not os.path.exists(csv_path):
        return None
    df = pd.read_csv(csv_path)
    required = {"Date", "Open", "High", "Low", "Close", "Volume"}
    if not required.issubset(set(df.columns)):
        return None
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"]).sort_values("Date")
    for c in ["Open", "High", "Low", "Close", "Volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["Close"])
    return df

def download_stooq_daily(stooq_symbol: str, out_path: str) -> bool:
    url = stooq_csv_url(stooq_symbol)
    try:
        txt = http_get_text(url, timeout=30)
        if "Date,Open,High,Low,Close,Volume" not in txt:
            return False
        ensure_dir(os.path.dirname(out_path))
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(txt)
        return True
    except Exception:
        return False

def compute_features(df: pd.DataFrame) -> Dict[str, float]:
    close = df["Close"].to_numpy(dtype=float)
    volu  = df["Volume"].to_numpy(dtype=float)
    n = len(close)

    def safe_ret(k: int) -> float:
        if n <= k or close[-k-1] <= 0:
            return np.nan
        return (close[-1] / close[-k-1]) - 1.0

    def sma(k: int) -> float:
        if n < k:
            return np.nan
        return float(np.nanmean(close[-k:]))

    lr = np.diff(np.log(np.clip(close, 1e-12, None)))
    if len(lr) < 2:
        vol20 = np.nan
        dvol20 = np.nan
        worst5d = np.nan
    else:
        def ann_vol(window: int) -> float:
            if len(lr) < window:
                return np.nan
            return float(np.nanstd(lr[-window:], ddof=1) * math.sqrt(252.0))

        vol20 = ann_vol(20)
        if len(lr) >= 20:
            neg = lr[-20:][lr[-20:] < 0]
            dvol20 = float(np.nanstd(neg, ddof=1) * math.sqrt(252.0)) if len(neg) >= 2 else 0.0
        else:
            dvol20 = np.nan

        if n >= 6:
            r5 = (close[5:] / close[:-5]) - 1.0
            worst5d = float(np.nanmin(r5)) if len(r5) else np.nan
        else:
            worst5d = np.nan

    dd_window = min(n, 126)
    if dd_window >= 2:
        c = close[-dd_window:]
        peak = np.maximum.accumulate(c)
        dd = (c / peak) - 1.0
        maxdd6m = float(np.nanmin(dd))
    else:
        maxdd6m = np.nan

    sma20 = sma(20); sma50 = sma(50); sma200 = sma(200)
    close_last = float(close[-1])
    trend_sma = np.nan
    if np.isfinite(sma50) and sma50 > 0:
        trend_sma = (close_last / sma50) - 1.0

    pct_days_above_sma200 = np.nan
    if n >= 200:
        roll200 = pd.Series(close).rolling(200).mean().to_numpy()
        above = (close[-200:] > roll200[-200:]).astype(float)
        pct_days_above_sma200 = float(np.nanmean(above))

    adv20_dollar = np.nan
    if n >= 20:
        adv20_dollar = float(np.nanmean(close[-20:] * volu[-20:]))

    zv_window = min(n, 60)
    zf = float(np.mean(volu[-zv_window:] <= 0)) if zv_window >= 1 else np.nan

    return {
        "last_close": close_last,
        "ret_1m": safe_ret(21),
        "ret_3m": safe_ret(63),
        "ret_6m": safe_ret(126),
        "ret_12m": safe_ret(252),
        "sma20": sma20,
        "sma50": sma50,
        "sma200": sma200,
        "trend_sma50": trend_sma,
        "pct_days_above_sma200": pct_days_above_sma200,
        "vol20_ann": vol20,
        "downside_vol20_ann": dvol20,
        "worst5d": worst5d,
        "maxdd_6m": maxdd6m,
        "adv20_dollar": adv20_dollar,
        "zero_volume_frac_60d": zf,
        "n_days": float(n)
    }

def gate_and_flags(feat: Dict[str, float], cfg: Dict) -> Tuple[bool, List[str], List[str]]:
    reasons = []
    flags = []
    gates = cfg["gates"]
    data = cfg["data"]

    if not np.isfinite(feat.get("n_days", np.nan)) or feat["n_days"] < data["min_history_days"]:
        reasons.append(f"min_history_days<{data['min_history_days']}")

    px = feat.get("last_close", np.nan)
    if not np.isfinite(px) or px > gates["price_max"]:
        reasons.append(f"price>{gates['price_max']}")

    adv = feat.get("adv20_dollar", np.nan)
    if not np.isfinite(adv) or adv < gates["adv20_dollar_min"]:
        reasons.append(f"adv20_dollar<{gates['adv20_dollar_min']}")

    zf = feat.get("zero_volume_frac_60d", np.nan)
    if np.isfinite(zf) and zf > gates["zero_volume_frac_max"]:
        reasons.append(f"zero_volume_frac>{gates['zero_volume_frac_max']}")

    vol = feat.get("vol20_ann", np.nan)
    if np.isfinite(vol) and vol > 1.00:
        flags.append("HIGH_VOL_20D_GT_100PCT")
    mdd = feat.get("maxdd_6m", np.nan)
    if np.isfinite(mdd) and mdd < -0.60:
        flags.append("DEEP_DRAWDOWN_6M_LT_-60PCT")
    worst5 = feat.get("worst5d", np.nan)
    if np.isfinite(worst5) and worst5 < -0.30:
        flags.append("WORST_5D_LT_-30PCT")

    return (len(reasons) == 0), reasons, flags

def score_symbol(feat: Dict[str, float], cfg: Dict) -> Tuple[float, Dict[str, float]]:
    w = cfg["score"]["weights"]

    mom6 = feat.get("ret_6m", np.nan)
    mom6_s = np.tanh(2.0 * mom6) if np.isfinite(mom6) else 0.0

    trend = feat.get("trend_sma50", np.nan)
    trend_s = np.tanh(3.0 * trend) if np.isfinite(trend) else 0.0

    vol = feat.get("vol20_ann", np.nan)
    vol_pen = -np.tanh(max(0.0, vol - 0.30)) if np.isfinite(vol) else 0.0

    mdd = feat.get("maxdd_6m", np.nan)
    dd_pen = -np.tanh(max(0.0, (-mdd) - 0.20)) if np.isfinite(mdd) else 0.0

    adv = feat.get("adv20_dollar", np.nan)
    liq = np.tanh(math.log10(max(1.0, adv)) - 6.0) if np.isfinite(adv) else 0.0

    comps = {
        "comp_momentum_6m": float(mom6_s),
        "comp_trend_sma": float(trend_s),
        "comp_vol_penalty": float(vol_pen),
        "comp_dd_penalty": float(dd_pen),
        "comp_liquidity": float(liq)
    }

    raw = (
        w["momentum_6m"] * comps["comp_momentum_6m"] +
        w["trend_sma"] * comps["comp_trend_sma"] +
        w["volatility_penalty"] * comps["comp_vol_penalty"] +
        w["drawdown_penalty"] * comps["comp_dd_penalty"] +
        w["liquidity"] * comps["comp_liquidity"]
    )

    score_1_10 = float(np.clip(1.0 + 4.5 * (raw + 1.0), 1.0, 10.0))
    return score_1_10, comps

def load_watchlist(path: str) -> List[str]:
    if not os.path.exists(path):
        return []
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln or ln.startswith("#"):
                continue
            s = normalize_symbol(ln)
            if s:
                out.append(s)
    return sorted(list(set(out)))

def build_universe(cfg: Dict) -> pd.DataFrame:
    n_txt = http_get_text(NASDAQ_LISTED_URL)
    o_txt = http_get_text(OTHER_LISTED_URL)
    n_df = parse_pipe_table(n_txt)
    o_df = parse_pipe_table(o_txt)

    n_df["Symbol"] = n_df["Symbol"].map(normalize_symbol)
    n_df = n_df[n_df["Symbol"] != ""]
    n_df["is_etf"] = (n_df.get("ETF", "N") == "Y")
    n_df["exchange"] = "NASDAQ"

    o_df["ACT Symbol"] = o_df["ACT Symbol"].map(normalize_symbol)
    o_df = o_df[o_df["ACT Symbol"] != ""]
    o_df["is_etf"] = (o_df.get("ETF", "N") == "Y")
    o_df["exchange"] = o_df.get("Exchange", "")

    univ = pd.concat([
        n_df.rename(columns={"Symbol": "symbol"})[["symbol", "Security Name", "is_etf", "exchange", "Test Issue"]],
        o_df.rename(columns={"ACT Symbol": "symbol"})[["symbol", "Security Name", "is_etf", "exchange", "Test Issue"]]
    ], ignore_index=True)

    if cfg["universe"].get("exclude_test_issues", True):
        univ = univ[univ["Test Issue"].astype(str).str.upper() != "Y"]

    if not cfg["universe"].get("include_etfs", True):
        univ = univ[~univ["is_etf"]]

    univ = univ.drop_duplicates(subset=["symbol"]).sort_values("symbol").reset_index(drop=True)
    return univ

def select_batch(symbols: List[str], state_file: str, max_n: int, strategy: str) -> List[str]:
    if max_n <= 0 or max_n >= len(symbols):
        return symbols

    state = read_json(state_file, default={"cursor": 0})
    cursor = int(state.get("cursor", 0))
    if strategy != "round_robin":
        cursor = 0

    batch = symbols[cursor: cursor + max_n]
    new_cursor = cursor + len(batch)
    if new_cursor >= len(symbols):
        new_cursor = 0

    write_json(state_file, {"cursor": new_cursor, "updated_utc": datetime.now(timezone.utc).isoformat()})
    return batch

def merge_update_csv(path: str, new_df: pd.DataFrame, key: str = "symbol") -> None:
    ensure_dir(os.path.dirname(path))
    if os.path.exists(path):
        old = pd.read_csv(path)
        combined = pd.concat([old, new_df], ignore_index=True)
        combined = combined.sort_values(["symbol", "asof_date"]).drop_duplicates(subset=[key], keep="last")
        combined.to_csv(path, index=False)
    else:
        new_df.to_csv(path, index=False)

def write_report(cfg: Dict, eligible: pd.DataFrame, scored: pd.DataFrame, universe_count: int, processed_count: int) -> None:
    p = cfg["paths"]["report_out"]
    ts = now_local(cfg["run"]["asof_timezone"]).strftime("%Y-%m-%d %H:%M:%S %Z")
    lines = []
    lines.append("# Market Monitor Report\n")
    lines.append(f"- Run time: {ts}")
    lines.append(f"- Universe size: {universe_count}")
    lines.append(f"- Processed this run: {processed_count}")
    lines.append(f"- Eligible (this run snapshot): {len(eligible)}\n")

    if "risk_flags" in scored.columns:
        flags = scored["risk_flags"].fillna("").astype(str)
        all_flags = []
        for s in flags:
            if s.strip():
                all_flags.extend([x for x in s.split(";") if x])
        if all_flags:
            vc = pd.Series(all_flags).value_counts().head(20)
            lines.append("## Risk flag counts (top 20)\n")
            for k, v in vc.items():
                lines.append(f"- {k}: {int(v)}")
            lines.append("")

    lines.append("## Top eligible (by monitor_priority)\n")
    top = eligible.sort_values("monitor_priority", ascending=False).head(25)
    if len(top) == 0:
        lines.append("_None this run._")
    else:
        for _, r in top.iterrows():
            lines.append(
                f"- {r['symbol']}: {r['monitor_priority']:.2f} | px={r['last_close']:.4g} | "
                f"adv20$={r['adv20_dollar']:.3g} | flags={r.get('risk_flags','')}"
            )
    lines.append("")

    with open(p, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()

    with open(args.config, "r", encoding="utf-8-sig") as f:
        cfg = json.load(f)

    paths = cfg["paths"]
    ensure_dir(os.path.dirname(paths["universe_csv"]))
    ensure_dir(paths["stooq_dir"])
    ensure_dir(os.path.dirname(paths["state_file"]))
    ensure_dir(os.path.dirname(paths["features_out"]))
    ensure_dir(os.path.dirname(paths["report_out"]))

    if cfg["universe"]["mode"] == "watchlist":
        syms = load_watchlist(paths["watchlist_file"])
        univ = pd.DataFrame({"symbol": syms})
    else:
            _p('[stage] downloading/parsing NASDAQ universe')
    univ = build_universe(cfg)
    _p(f'[info] universe symbols: {len(univ)}')
    univ.to_csv(paths["universe_csv"], index=False)

    symbols = univ["symbol"].tolist()
    universe_count = len(symbols)

    _p('[stage] selecting batch')
    batch = select_batch(
        symbols,
        state_file=paths["state_file"],
        max_n=int(cfg["run"]["max_symbols_per_run"]),
        strategy=str(cfg["run"].get("batch_strategy", "round_robin"))
    )

    _p(f'[info] batch size: {len(batch)}')
    refresh_h = float(cfg['data']['stooq_refresh_hours'])
    rows_features = []
    rows_scored = []

    for i, sym in enumerate(batch, start=1):
        if i == 1 or (i % 25 == 0) or (i == len(batch)):
            _p(f'[progress] {i}/{len(batch)}  {sym}')
        stooq_sym = to_stooq_symbol(sym)
        csv_path = os.path.join(paths["stooq_dir"], f"{stooq_sym}.csv")

        if not file_is_fresh(csv_path, refresh_h):
            ok = download_stooq_daily(stooq_sym, csv_path)
            if not ok:
                continue

        df = load_stooq_daily(csv_path)
        if df is None or len(df) < 5:
            continue

        feat = compute_features(df)
        passed, reasons, flags = gate_and_flags(feat, cfg)

        asof_date = df["Date"].iloc[-1].date().isoformat()
        base = {
            "symbol": sym,
            "stooq_symbol": stooq_sym,
            "asof_date": asof_date,
            "eligible": bool(passed),
            "gate_reasons": ";".join(reasons),
            "risk_flags": ";".join(flags)
        }
        base.update(feat)

        score = np.nan
        comps = {}
        if passed:
            score, comps = score_symbol(feat, cfg)
        base["monitor_priority"] = score
        base.update(comps)

        rows_scored.append(base)
        rows_features.append({k: base.get(k) for k in [
            "symbol","stooq_symbol","asof_date","last_close","ret_1m","ret_3m","ret_6m","ret_12m",
            "sma20","sma50","sma200","trend_sma50","pct_days_above_sma200",
            "vol20_ann","downside_vol20_ann","worst5d","maxdd_6m","adv20_dollar","zero_volume_frac_60d","n_days"
        ]})

    scored_df = pd.DataFrame(rows_scored)
    feat_df   = pd.DataFrame(rows_features)

    if len(scored_df) == 0:
        print("No symbols processed successfully in this run (data/provider constraints).")
        return 2

    merge_update_csv(paths["features_out"], feat_df, key="symbol")
    merge_update_csv(paths["scored_out"], scored_df, key="symbol")

    eligible = scored_df[scored_df["eligible"] == True].copy()
    eligible.to_csv(paths["eligible_out"], index=False)

    write_report(cfg, eligible=eligible, scored=scored_df, universe_count=universe_count, processed_count=len(scored_df))
    _p(f'[done] processed: {len(scored_df)} / {universe_count} (this run batch)')
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
