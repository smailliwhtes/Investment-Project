from __future__ import annotations

from pathlib import Path

import pandas as pd


class EtfHoldingsError(RuntimeError):
    pass


def load_etf_holdings(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise EtfHoldingsError(f"ETF holdings file not found: {path}")
    frame = pd.read_csv(path)
    if frame.empty:
        return pd.DataFrame(
            columns=["as_of_date", "etf_symbol", "constituent_symbol", "weight", "sector", "theme"]
        )

    renamed = frame.rename(
        columns={
            "date": "as_of_date",
            "symbol": "constituent_symbol",
            "ticker": "constituent_symbol",
        }
    ).copy()
    for column in ("as_of_date", "etf_symbol", "constituent_symbol", "sector", "theme"):
        if column not in renamed.columns:
            renamed[column] = ""
    if "weight" not in renamed.columns:
        renamed["weight"] = 0.0

    renamed["etf_symbol"] = renamed["etf_symbol"].astype(str).str.upper().str.strip()
    renamed["constituent_symbol"] = renamed["constituent_symbol"].astype(str).str.upper().str.strip()
    renamed["sector"] = renamed["sector"].astype(str).str.lower().str.strip()
    renamed["theme"] = renamed["theme"].astype(str).str.lower().str.strip()
    renamed["weight"] = pd.to_numeric(renamed["weight"], errors="coerce").fillna(0.0)
    return renamed[
        ["as_of_date", "etf_symbol", "constituent_symbol", "weight", "sector", "theme"]
    ].sort_values(["etf_symbol", "weight", "constituent_symbol"], ascending=[True, False, True])
