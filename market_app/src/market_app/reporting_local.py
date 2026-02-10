from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


def write_report(
    path: Path,
    *,
    run_id: str,
    config: dict[str, Any],
    universe: pd.DataFrame,
    classified: pd.DataFrame,
    eligible: pd.DataFrame,
    scored: pd.DataFrame,
    data_quality: pd.DataFrame,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc).isoformat()
    top_n = int(config.get("run", {}).get("top_n", config.get("report", {}).get("top_n", 50)))
    eligible_scored = scored.merge(eligible, on="symbol", how="left")
    eligible_scored = eligible_scored[eligible_scored["eligible"]].head(top_n)
    theme_counts = (
        classified["themes"]
        .fillna("")
        .str.split(";")
        .explode()
        .replace("", pd.NA)
        .dropna()
        .value_counts()
    )
    data_quality_summary = data_quality[
        ["symbol", "missing_data", "stale_data", "volume_missing"]
    ].copy()

    lines = [
        "# Offline Monitor Report",
        "",
        f"- Run ID: `{run_id}`",
        f"- Generated (UTC): `{now}`",
        f"- Offline mode: `{config.get('offline', True)}`",
        "",
        "## Universe Summary",
        f"- Symbols: {len(universe)}",
        f"- Eligible: {int(eligible['eligible'].sum())}",
        f"- Ineligible: {int((~eligible['eligible']).sum())}",
        "",
        "## Theme Distribution",
    ]
    if theme_counts.empty:
        lines.append("- No theme matches found.")
    else:
        lines.extend([f"- {theme}: {count}" for theme, count in theme_counts.items()])

    lines.extend(
        [
            "",
            "## Eligibility Breakdown",
            "",
            _frame_to_markdown(
                eligible[["symbol", "eligible", "gate_fail_reasons"]].head(20)
            ),
            "",
            "## Top Eligible Symbols",
            "",
        ]
    )
    if eligible_scored.empty:
        lines.append("_No eligible symbols in this run._")
    else:
        lines.append(
            _frame_to_markdown(
                eligible_scored[
                    ["symbol", "monitor_score", "risk_level", "risk_flags", "themes"]
                ]
            )
        )

    lines.extend(
        [
            "",
            "## Data Quality",
            "",
            _frame_to_markdown(data_quality_summary.head(20)),
        ]
    )

    path.write_text("\n".join(lines), encoding="utf-8")


def _frame_to_markdown(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "_No data available._"
    header = "| " + " | ".join(frame.columns) + " |"
    separator = "| " + " | ".join(["---"] * len(frame.columns)) + " |"
    rows = []
    for _, row in frame.iterrows():
        rows.append("| " + " | ".join(str(value) for value in row.tolist()) + " |")
    return "\n".join([header, separator, *rows])
