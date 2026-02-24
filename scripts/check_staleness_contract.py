from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

REQUIRED_COLUMNS = {"symbol", "last_date", "lag_days"}


def _load_csv(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise ValueError(f"missing {label} at {path}")
    frame = pd.read_csv(path)
    if frame.empty:
        raise ValueError(f"{label} is empty: {path}")
    missing = REQUIRED_COLUMNS - set(frame.columns)
    if missing:
        raise ValueError(f"{label} missing required columns: {sorted(missing)}")
    return frame


def _canonicalize(frame: pd.DataFrame, source: str) -> pd.DataFrame:
    out = frame[["symbol", "last_date", "lag_days"]].copy()
    out["symbol"] = out["symbol"].astype(str).str.strip().str.upper()
    if out["symbol"].eq("").any():
        raise ValueError(f"{source} has blank symbol values")

    dupes = out[out["symbol"].duplicated(keep=False)]["symbol"].sort_values().unique().tolist()
    if dupes:
        preview = dupes[:25]
        raise ValueError(f"{source} contains duplicate symbols; one-to-one contract violated: {preview}")

    out["last_date_norm"] = pd.to_datetime(out["last_date"], errors="coerce", format="mixed").dt.strftime("%Y-%m-%d")
    out["lag_days_norm"] = pd.to_numeric(out["lag_days"], errors="coerce")

    # Contract policy: staleness fields are mandatory for every scored row.
    if out["last_date_norm"].isna().any():
        missing = out.loc[out["last_date_norm"].isna(), "symbol"].head(25).tolist()
        raise ValueError(f"{source} has invalid/blank last_date values (first 25): {missing}")
    if out["lag_days_norm"].isna().any():
        missing = out.loc[out["lag_days_norm"].isna(), "symbol"].head(25).tolist()
        raise ValueError(f"{source} has invalid/blank lag_days values (first 25): {missing}")

    out["lag_days_norm"] = out["lag_days_norm"].astype("Int64")
    return out


def run_contract(run_dir: Path, sample_limit: int = 25) -> int:
    scored_path = run_dir / "scored.csv"
    dq_path = run_dir / "data_quality.csv"

    try:
        scored_raw = _load_csv(scored_path, "scored.csv")
        dq_raw = _load_csv(dq_path, "data_quality.csv")
        scored = _canonicalize(scored_raw, "scored.csv")
        dq = _canonicalize(dq_raw, "data_quality.csv")
    except ValueError as exc:
        print(f"[contract:error] {exc}")
        return 1

    symbols_scored = set(scored["symbol"])
    symbols_dq = set(dq["symbol"])

    missing_in_dq = sorted(symbols_scored - symbols_dq)
    missing_in_scored = sorted(symbols_dq - symbols_scored)

    if missing_in_dq or missing_in_scored:
        print("[contract:error] symbol universe mismatch between scored.csv and data_quality.csv")
        print(f"[contract:error] missing in data_quality.csv: count={len(missing_in_dq)} sample={missing_in_dq[:sample_limit]}")
        print(f"[contract:error] missing in scored.csv: count={len(missing_in_scored)} sample={missing_in_scored[:sample_limit]}")
        return 1

    try:
        merged = scored.merge(
            dq[["symbol", "last_date_norm", "lag_days_norm"]],
            on="symbol",
            how="inner",
            validate="one_to_one",
            suffixes=("_scored", "_dq"),
        )
    except Exception as exc:
        print(f"[contract:error] merge validation failed: {exc}")
        return 1

    last_date_mismatch = merged[merged["last_date_norm_scored"] != merged["last_date_norm_dq"]].copy()
    lag_days_mismatch = merged[merged["lag_days_norm_scored"] != merged["lag_days_norm_dq"]].copy()

    if not last_date_mismatch.empty or not lag_days_mismatch.empty:
        print("[contract:error] staleness mismatch detected after canonicalization")
        print(
            f"[contract:error] mismatched_last_date={len(last_date_mismatch)} "
            f"mismatched_lag_days={len(lag_days_mismatch)}"
        )
        combined = (
            pd.concat(
                [
                    last_date_mismatch[["symbol", "last_date_norm_scored", "last_date_norm_dq", "lag_days_norm_scored", "lag_days_norm_dq"]],
                    lag_days_mismatch[["symbol", "last_date_norm_scored", "last_date_norm_dq", "lag_days_norm_scored", "lag_days_norm_dq"]],
                ],
                ignore_index=True,
            )
            .drop_duplicates(subset=["symbol"])
            .sort_values("symbol")
            .head(sample_limit)
        )
        print("[contract:error] mismatch sample (up to 25 rows):")
        print(combined.to_string(index=False))
        return 1

    print(
        f"[contract:ok] symbols={len(merged)} "
        "canonicalization=enabled "
        "(symbol upper/trim, last_date as YYYY-MM-DD, lag_days numeric Int64)"
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate scored.csv staleness fields against data_quality.csv")
    parser.add_argument("--run-dir", required=True, help="Directory containing scored.csv and data_quality.csv")
    parser.add_argument("--sample-limit", type=int, default=25)
    args = parser.parse_args()

    return run_contract(Path(args.run_dir), sample_limit=args.sample_limit)


if __name__ == "__main__":
    raise SystemExit(main())
