from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from market_monitor.hash_utils import hash_file, hash_text


REQUIRED_FEATURES = [
    "return_1m",
    "return_3m",
    "return_6m",
    "return_12m",
    "sma20_ratio",
    "sma50_ratio",
    "sma200_ratio",
    "pct_days_above_sma200",
    "volatility20",
    "volatility60",
    "downside_vol20",
    "worst_5d_return",
    "max_drawdown_6m",
    "adv20_dollar",
    "zero_volume_fraction",
]


def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cleaned = df.copy()
    float_cols = cleaned.select_dtypes(include=["float", "float32", "float64"]).columns
    if len(float_cols):
        cleaned[float_cols] = cleaned[float_cols].round(6)
    cleaned.to_csv(path, index=False)


def normalize_features(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    out = df.copy()
    for col in columns:
        values = pd.to_numeric(out[col], errors="coerce")
        mean = float(values.mean())
        std = float(values.std(ddof=0)) or 1.0
        z = (values - mean) / std
        out[f"z_{col}"] = z
        out[f"z_{col}_clipped"] = z.clip(-3, 3)
    return out


def apply_blueprint_gates(
    df: pd.DataFrame, gates_cfg: dict[str, Any]
) -> tuple[pd.DataFrame, pd.Series]:
    reasons = []
    eligible_mask = pd.Series(True, index=df.index)

    min_adv = gates_cfg.get("min_adv20_dollar")
    if min_adv is not None:
        mask = df["adv20_dollar"] >= float(min_adv)
        eligible_mask &= mask
        reasons.append(("MIN_ADV20_DOLLAR", mask))

    max_zero = gates_cfg.get("max_zero_volume_fraction")
    if max_zero is not None:
        mask = df["zero_volume_fraction"] <= float(max_zero)
        eligible_mask &= mask
        reasons.append(("MAX_ZERO_VOLUME_FRACTION", mask))

    if gates_cfg.get("min_price_above_sma200", False):
        mask = df["sma200_ratio"] > 0
        eligible_mask &= mask
        reasons.append(("PRICE_ABOVE_SMA200", mask))

    max_missing = gates_cfg.get("max_missing_day_rate")
    if max_missing is not None:
        mask = df["missing_day_rate"] <= float(max_missing)
        eligible_mask &= mask
        reasons.append(("MAX_MISSING_DAY_RATE", mask))

    gate_reasons = []
    for idx in df.index:
        failed = [name for name, mask in reasons if not bool(mask.loc[idx])]
        gate_reasons.append(";".join(failed))
    eligible_df = df[["symbol", "name"]].copy()
    eligible_df["eligible"] = eligible_mask
    eligible_df["excluded_reasons"] = gate_reasons
    return eligible_df, eligible_mask


def build_risk_flags(df: pd.DataFrame, thresholds: dict[str, Any]) -> pd.Series:
    flags: list[str] = []
    results = []
    for _, row in df.iterrows():
        flags.clear()
        if row.get("volatility60", 0.0) >= thresholds.get("high_volatility", 1.2):
            flags.append("HighVolatility")
        if row.get("max_drawdown_6m", 0.0) <= thresholds.get("large_drawdown", -0.25):
            flags.append("LargeDrawdown")
        if row.get("adv20_dollar", 0.0) <= thresholds.get("illiquid_adv20_dollar", 1_000_000):
            flags.append("Illiquid")
        if row.get("zero_volume_fraction", 0.0) >= thresholds.get("zero_volume_fraction", 0.1):
            flags.append("ZeroVolume")
        if row.get("stale_data", False):
            flags.append("StaleData")
        if row.get("missing_data", False):
            flags.append("MissingData")
        if row.get("split_suspect", False):
            flags.append("SplitSuspect")
        results.append(";".join(flags))
    return pd.Series(results, index=df.index)


def compute_forward_outcome_summary(
    *,
    symbols: list[str],
    provider,
    horizons: list[int],
    slippage: float = 0.001,
) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for horizon in horizons:
        samples = []
        for symbol in symbols:
            history = provider.get_history(symbol, 0)
            if history.empty:
                continue
            history = history.copy()
            history["Date"] = pd.to_datetime(history["Date"], errors="coerce")
            history = history.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)
            close = history["Close"].to_numpy(dtype=float)
            if len(close) <= horizon + 20:
                continue
            returns = np.diff(np.log(np.clip(close, 1e-12, None)))
            for idx in range(20, len(close) - horizon):
                if close[idx] <= 0:
                    continue
                forward = close[idx + horizon] / close[idx] - 1.0
                vol_window = returns[idx - 20 : idx]
                if len(vol_window) < 5:
                    continue
                spread_proxy = float(np.nanstd(vol_window, ddof=1)) * 0.5
                net = float(forward - slippage - spread_proxy)
                samples.append(net)
        if not samples:
            summary[str(horizon)] = {
                "count": 0,
                "p05": None,
                "p25": None,
                "p50": None,
                "p75": None,
                "p95": None,
            }
            continue
        arr = np.array(samples)
        summary[str(horizon)] = {
            "count": int(len(arr)),
            "p05": float(np.nanpercentile(arr, 5)),
            "p25": float(np.nanpercentile(arr, 25)),
            "p50": float(np.nanpercentile(arr, 50)),
            "p75": float(np.nanpercentile(arr, 75)),
            "p95": float(np.nanpercentile(arr, 95)),
        }
    return summary


def build_forward_outcome_column(
    df: pd.DataFrame, summary: dict[str, Any], top_n: int
) -> pd.Series:
    payload = json.dumps(summary, sort_keys=True)
    top = df.nlargest(top_n, "total_score")["symbol"].tolist()
    return df["symbol"].apply(lambda symbol: payload if symbol in top else "")


def build_report(
    path: Path,
    *,
    run_id: str,
    run_timestamp: str,
    scored: pd.DataFrame,
    regime: dict[str, Any],
    forward_summary: dict[str, Any],
    top_n: int,
) -> None:
    lines = [
        "# Market Monitor Report (Blueprint)",
        "",
        f"Run ID: {run_id}",
        f"Run timestamp: {run_timestamp}",
        "",
        "## Regime Summary",
        "",
        f"- Regime label: {regime.get('regime_label', 'Unknown')}",
        "",
    ]
    indicators = regime.get("indicators", [])
    if indicators:
        lines.append("| Indicator | Latest Value | Z-Score | Label |")
        lines.append("| --- | --- | --- | --- |")
        for indicator in indicators:
            lines.append(
                f"| {indicator.get('name')} | {indicator.get('latest_value')} | "
                f"{indicator.get('zscore')} | {indicator.get('label')} |"
            )
    else:
        lines.append("No macro indicators available.")

    lines.extend(["", "## Risk Flag Distribution", ""])
    if "flags" in scored.columns and not scored.empty:
        counts = (
            scored["flags"]
            .fillna("")
            .apply(lambda value: [v for v in value.split(";") if v])
            .explode()
            .value_counts()
        )
        if not counts.empty:
            lines.append("| Flag | Count |")
            lines.append("| --- | --- |")
            for flag, count in counts.items():
                lines.append(f"| {flag} | {count} |")
        else:
            lines.append("No flags recorded.")
    else:
        lines.append("No scored symbols available.")

    lines.extend(["", "## Top Monitor Priority", ""])
    if not scored.empty:
        top = scored.sort_values("total_score", ascending=False).head(top_n)
        lines.append("| Symbol | Theme | Score | Flags |")
        lines.append("| --- | --- | --- | --- |")
        for _, row in top.iterrows():
            lines.append(
                f"| {row.get('symbol')} | {row.get('theme')} | "
                f"{row.get('total_score'):.4f} | {row.get('flags')} |"
            )
    else:
        lines.append("No scored symbols available.")

    lines.extend(["", "## Forward Outcome Summary (Historical Bands)", ""])
    if forward_summary:
        lines.append("| Horizon (days) | Count | P05 | P25 | P50 | P75 | P95 |")
        lines.append("| --- | --- | --- | --- | --- | --- | --- |")
        for horizon in sorted(forward_summary.keys(), key=lambda v: int(v)):
            row = forward_summary[horizon]
            lines.append(
                f"| {horizon} | {row['count']} | {row['p05']} | {row['p25']} | "
                f"{row['p50']} | {row['p75']} | {row['p95']} |"
            )
    else:
        lines.append("No forward outcome summary available.")

    lines.extend(
        [
            "",
            "## Notes",
            "This report is monitoring-only and contains no trading recommendations.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


@dataclass(frozen=True)
class Manifest:
    payload: dict[str, Any]

    def to_json(self) -> str:
        return json.dumps(self.payload, indent=2, sort_keys=True)


def build_manifest(
    *,
    run_id: str,
    run_timestamp: str,
    as_of_date: str | None,
    config_hash: str,
    git_sha: str | None,
    offline: bool,
    output_dir: Path,
    config_path: Path,
    dataset_paths: list[Path],
    dependency_versions: dict[str, str],
    stable_outputs: list[str] | None = None,
    outputs_override: dict[str, dict[str, Any]] | None = None,
) -> Manifest:
    outputs = outputs_override or {}
    if not outputs:
        stable_set = set(stable_outputs or [])
        for path in output_dir.glob("*"):
            if path.is_file():
                if stable_set and path.name not in stable_set:
                    continue
                outputs[path.name] = {"sha256": hash_file(path), "bytes": path.stat().st_size}

    datasets = []
    for path in dataset_paths:
        if path.exists():
            datasets.append({"path": str(path), "sha256": hash_file(path)})

    payload = {
        "run_id": run_id,
        "run_timestamp_utc": run_timestamp,
        "as_of_date": as_of_date,
        "config_hash": config_hash,
        "git_sha": git_sha,
        "offline": offline,
        "outputs": outputs,
        "datasets": datasets,
        "config_path": str(config_path),
        "dependency_versions": dependency_versions,
        "manifest_hash": hash_text(json.dumps(outputs, sort_keys=True)),
    }
    return Manifest(payload=payload)
