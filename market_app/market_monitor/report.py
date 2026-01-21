from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


def write_report(
    path: Path,
    summary: dict[str, int],
    scored: pd.DataFrame,
    *,
    run_id: str | None = None,
    run_timestamp: str | None = None,
    data_usage: dict[str, str] | None = None,
    prediction_panel: pd.DataFrame | None = None,
    prediction_metrics: dict[str, float] | None = None,
    context_summary: dict[str, object] | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Market Monitor Report",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat().replace('+00:00','Z')}",
        f"Run ID: {run_id or 'unknown'}",
        f"Run timestamp: {run_timestamp or 'unknown'}",
        "",
        "## Stage Summary",
        "",
        f"- Universe size: {summary.get('universe', 0)}",
        f"- Stage 1 survivors: {summary.get('stage1', 0)}",
        f"- Stage 2 eligible prelim: {summary.get('stage2', 0)}",
        f"- Stage 3 scored: {summary.get('stage3', 0)}",
        "",
    ]

    if data_usage:
        lines.extend(
            [
                "## Data Source Usage",
                "",
                f"- Offline mode: {data_usage.get('offline_mode')}",
                f"- Provider: {data_usage.get('provider_name')}",
                f"- MARKET_APP_NASDAQ_DAILY_DIR: {data_usage.get('nasdaq_daily_dir')}",
                f"- NASDAQ data found: {data_usage.get('nasdaq_daily_found')}",
                f"- MARKET_APP_SILVER_PRICES_DIR: {data_usage.get('silver_prices_dir')}",
                f"- Silver data found: {data_usage.get('silver_prices_found')}",
                f"- MARKET_APP_GDELT_CONFLICT_DIR: {data_usage.get('gdelt_conflict_dir')}",
                f"- GDELT corpus found: {data_usage.get('gdelt_conflict_found')}",
                "",
                "## Missing Field Handling",
                "",
                "- Volume-missing symbols are scored with explicit penalties and liquidity overrides.",
                "- Volume-dependent features are set to NA when volume is unavailable.",
                "",
            ]
        )

    lines.extend(
        [
            "## Top Monitor Priority",
            "",
        ]
    )
    if not scored.empty:
        top = scored.sort_values("monitor_priority_1_10", ascending=False).head(15)
        lines.append("| Symbol | Name | Monitor Score | Risk | Confidence | Notes |")
        lines.append("| --- | --- | --- | --- | --- | --- |")
        for _, row in top.iterrows():
            confidence = row.get("confidence_score")
            confidence_fmt = f"{confidence:.2f}" if confidence is not None else "NA"
            lines.append(
                f"| {row.get('symbol')} | {row.get('name')} | {row.get('monitor_score_1_10')} | {row.get('risk_level')} | {confidence_fmt} | {row.get('notes')} |"
            )
    else:
        lines.append("No scored symbols available.")

    if prediction_panel is not None and not prediction_panel.empty:
        lines.extend(_prediction_diagnostics(prediction_panel, prediction_metrics or {}))

    if context_summary:
        lines.extend(_context_section(context_summary))

    lines.append("")
    lines.append("## Notes")
    lines.append("This report is monitoring-only and contains no trading recommendations.")

    path.write_text("\n".join(lines), encoding="utf-8")


def _prediction_diagnostics(panel: pd.DataFrame, metrics: dict[str, float]) -> list[str]:
    lines = [
        "",
        "## Prediction Diagnostics",
        "",
        f"- Brier score (walk-forward): {metrics.get('brier_score', float('nan')):.4f}",
        f"- Brier score (full fit): {metrics.get('brier_score_full', float('nan')):.4f}",
        "",
    ]

    if {"pred_forward_return_20d", "forward_return_20d"}.issubset(panel.columns):
        panel = panel.copy()
        panel["decile"] = pd.qcut(panel["pred_forward_return_20d"], 10, labels=False, duplicates="drop")
        top = panel[panel["decile"] == panel["decile"].max()]
        bottom = panel[panel["decile"] == panel["decile"].min()]
        lines.extend(
            [
                "### Rank Efficacy (Forward Return)",
                "",
                f"- Top decile mean forward return: {np.nanmean(top['forward_return_20d']):.4f}",
                f"- Bottom decile mean forward return: {np.nanmean(bottom['forward_return_20d']):.4f}",
                "",
            ]
        )

    if {"date", "symbol", "pred_forward_return_20d"}.issubset(panel.columns):
        panel = panel.sort_values("date")
        panel["decile"] = pd.qcut(panel["pred_forward_return_20d"], 10, labels=False, duplicates="drop")
        top_membership = (
            panel.groupby("date").apply(lambda df: set(df[df["decile"] == df["decile"].max()]["symbol"]))
        )
        stability = []
        prev = None
        for members in top_membership:
            if prev is not None and members:
                stability.append(len(prev & members) / len(members))
            prev = members
        if stability:
            lines.extend(
                [
                    "### Rank Stability",
                    "",
                    f"- Avg top-decile overlap (day-over-day): {np.mean(stability):.2f}",
                    "",
                ]
            )

    if "vol60_ann" in panel.columns:
        median_vol = np.nanmedian(panel["vol60_ann"])
        high_vol = panel[panel["vol60_ann"] >= median_vol]
        low_vol = panel[panel["vol60_ann"] < median_vol]
        if not high_vol.empty and not low_vol.empty:
            lines.extend(
                [
                    "### Regime Slicing (Volatility)",
                    "",
                    f"- High-vol mean forward return: {np.nanmean(high_vol['forward_return_20d']):.4f}",
                    f"- Low-vol mean forward return: {np.nanmean(low_vol['forward_return_20d']):.4f}",
                    "",
                ]
            )

    return lines


def _context_section(context_summary: dict[str, object]) -> list[str]:
    latest_date = context_summary.get("latest_date", "unknown")
    metrics = context_summary.get("latest_metrics", {}) or {}
    analogs = context_summary.get("analogs", []) or []
    analog_outcomes = context_summary.get("analog_outcomes", []) or []
    event_impact_rows = context_summary.get("event_impact_rows", 0)
    lines = [
        "",
        "## Context & Analogs",
        "",
        f"- Latest context date: {latest_date}",
        f"- Event impact rows: {event_impact_rows}",
        "",
        "### Latest Context Snapshot",
        "",
    ]
    if metrics:
        for key, value in metrics.items():
            lines.append(f"- {key}: {value}")
    else:
        lines.append("- No context metrics available.")

    lines.extend(
        [
            "",
            "### Nearest Historical Analogs",
            "",
        ]
    )
    if analogs:
        lines.append("| Rank | Date | Similarity |")
        lines.append("| --- | --- | --- |")
        for entry in analogs:
            lines.append(
                f"| {entry.get('rank')} | {entry.get('date')} | {entry.get('similarity'):.4f} |"
            )
    else:
        lines.append("No analogs available.")

    lines.extend(["", "### Analog Outcome Distributions (Baseline)", ""])
    if analog_outcomes:
        lines.append("| Symbol | Forward Days | Mean Forward Return |")
        lines.append("| --- | --- | --- |")
        for row in analog_outcomes:
            lines.append(
                f"| {row.get('symbol')} | {row.get('forward_days')} | {row.get('forward_return'):.4f} |"
            )
    else:
        lines.append("No analog outcome summaries available.")
    return lines
