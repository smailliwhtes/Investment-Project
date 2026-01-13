import argparse, json
import numpy as np
import pandas as pd

def clamp01(x: float) -> float:
    return float(max(0.0, min(1.0, x)))

def robust_norm(x, p05, p95, invert=False):
    if pd.isna(x) or pd.isna(p05) or pd.isna(p95) or p95 == p05:
        return 0.5
    v = clamp01((x - p05) / (p95 - p05))
    return 1.0 - v if invert else v

ap = argparse.ArgumentParser()
ap.add_argument("--features_csv", required=True)
ap.add_argument("--universe_csv", required=True)
ap.add_argument("--config", required=True)
ap.add_argument("--scored_csv", required=True)
ap.add_argument("--eligible_csv", required=True)
ap.add_argument("--report_md", required=True)
args = ap.parse_args()

cfg = json.loads(open(args.config, "r", encoding="utf-8-sig").read())
w = cfg["score_weights"]

feat = pd.read_csv(args.features_csv)
univ = pd.read_csv(args.universe_csv)

df = feat.merge(univ[["symbol","is_etf","test_issue","financial_status"]], on="symbol", how="left")

# Calibration percentiles computed cross-sectionally for this run (MVP).
# The blueprintÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢s longer-term target is rolling historical percentiles. :contentReference[oaicite:9]{index=9}
cal_cols = ["adv20_dollar","atr14","sigma20","roc20","roc60","mdd_1y","ulcer_1y","cvar_h"]
p = {}
for c in cal_cols:
    s = pd.to_numeric(df[c], errors="coerce")
    p[c] = (s.quantile(0.05), s.quantile(0.95))

def gates(row):
    reasons = []
    if row.get("price_for_gates", np.inf) > cfg["price_cap"]:
        reasons.append("Price over cap")
    if pd.isna(row.get("adv20_dollar")) or row["adv20_dollar"] < cfg["adv20_threshold"]:
        reasons.append("Low liquidity (ADV20)")
    if row.get("test_issue", False) is True:
        reasons.append("Test issue")
    if str(row.get("financial_status","")) in set(cfg["universe"]["exclude_fin_status"]):
        reasons.append("Financial distress flag")
    return (len(reasons) == 0), reasons

flags = []
scores = []
eligible = []

for _, r in df.iterrows():
    ok, reasons = gates(r)

    # Sub-scores: normalized 0..1 (MVP)
    liq = robust_norm(r["adv20_dollar"], *p["adv20_dollar"], invert=False)
    vol = 0.5 * robust_norm(r["atr14"], *p["atr14"], invert=True) + 0.5 * robust_norm(r["sigma20"], *p["sigma20"], invert=True)
    mom = 0.5 * robust_norm(r["roc20"], *p["roc20"], invert=False) + 0.5 * robust_norm(r["roc60"], *p["roc60"], invert=False)
    dd  = 0.5 * robust_norm(r["mdd_1y"], *p["mdd_1y"], invert=False) + 0.5 * robust_norm(r["ulcer_1y"], *p["ulcer_1y"], invert=True)

    # Placeholders for factor/regime until implemented
    factor = 0.5
    regime = 0.5

    raw = (
        w["liquidity_size"] * liq +
        w["vol_tail"] * vol +
        w["momentum_trend"] * mom +
        w["drawdown_recovery"] * dd +
        w["factor_alignment"] * factor +
        w["regime_fit"] * regime
    )

    # Map to 1ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Å“10 by deciles (cross-sectional MVP)
    # (Blueprint calls for deciling against a rolling historical distribution long-term.) :contentReference[oaicite:10]{index=10}
    scores.append(raw)
    eligible.append(ok)

df["raw_score"] = scores
df["eligible"] = eligible

# Decile mapping across this run
df["score_1_10"] = pd.qcut(df["raw_score"].rank(method="first"), 10, labels=False) + 1

# Risk flags (based on blueprint examples) :contentReference[oaicite:11]{index=11}
def make_flags(row):
    out = []
    if row["price_for_gates"] >= cfg["near_cap"]:
        out.append("AMBER: Near price cap")
    if (pd.notna(row.get("roc20")) and row["roc20"] < -20) or (pd.notna(row.get("roc60")) and row["roc60"] < -20):
        out.append("RED: Extreme negative momentum")
    if pd.notna(row.get("mdd_1y")) and row["mdd_1y"] < -0.30:
        out.append("AMBER: Deep drawdown")
    if pd.notna(row.get("adv20_dollar")) and row["adv20_dollar"] < cfg["adv20_threshold"]:
        out.append("RED: Low liquidity")
    return "; ".join(out)

df["risk_flags"] = df.apply(make_flags, axis=1)

# Write outputs
df.sort_values(["eligible","score_1_10"], ascending=[False, False]).to_csv(args.scored_csv, index=False)
df[df["eligible"]].sort_values("score_1_10", ascending=False).to_csv(args.eligible_csv, index=False)

# Run report
with open(args.report_md, "w", encoding="utf-8-sig") as f:
    f.write(f"# Run report\n\n")
    f.write(f"- Rows: {len(df)}\n")
    f.write(f"- Eligible: {int(df['eligible'].sum())}\n")
    f.write(f"- Ineligible: {int((~df['eligible']).sum())}\n\n")
    top = df[df["eligible"]].nlargest(10, "score_1_10")[["symbol","score_1_10","risk_flags"]]
    f.write("## Top eligible by monitor priority (max 10 shown)\n\n")
    f.write(top.to_markdown(index=False))
    f.write("\n")
