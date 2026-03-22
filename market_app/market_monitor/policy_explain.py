from __future__ import annotations

from pathlib import Path

import pandas as pd

from market_monitor.timebase import utcnow


def write_policy_report(
    path: Path,
    *,
    scenario_name: str,
    scenario_description: str,
    regime_snapshot: dict[str, object],
    rankings: pd.DataFrame,
    analogs: pd.DataFrame,
    event_study: pd.DataFrame,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Policy Simulator Report",
        "",
        f"Generated: {utcnow().isoformat()}",
        f"Scenario: {scenario_name}",
        "",
        scenario_description,
        "",
        "## Regime Snapshot",
        "",
        f"- Macro regime: {regime_snapshot.get('macro_regime', 'unknown')}",
        f"- Inflation regime: {regime_snapshot.get('inflation_regime', 'unknown')}",
        f"- Policy stance: {regime_snapshot.get('policy_stance', 'unknown')}",
        f"- Conflict regime: {regime_snapshot.get('conflict_regime', 'unknown')}",
        f"- Tags: {', '.join(regime_snapshot.get('tags', []) or ['none'])}",
        "",
        "## Top Impacted Symbols",
        "",
    ]

    if rankings.empty:
        lines.append("No policy impact rankings were produced.")
    else:
        lines.append(
            "| Rank | Symbol | Impact Score | Median 20D | Q10 20D | Q90 20D | Confidence |"
        )
        lines.append("| --- | --- | --- | --- | --- | --- | --- |")
        for _, row in rankings.head(10).iterrows():
            lines.append(
                "| {rank} | {symbol} | {score:.3f} | {med:.4f} | {q10:.4f} | {q90:.4f} | {conf:.3f} |".format(
                    rank=int(row.get("rank", 0)),
                    symbol=row.get("symbol", ""),
                    score=float(row.get("scenario_impact_score", 0.0)),
                    med=float(row.get("median_return_20d", 0.0)),
                    q10=float(row.get("q10_return_20d", 0.0)),
                    q90=float(row.get("q90_return_20d", 0.0)),
                    conf=float(row.get("confidence_score", 0.0)),
                )
            )

    lines.extend(["", "## Historical Analogs", ""])
    if analogs.empty:
        lines.append("No historical analogs were found.")
    else:
        lines.append("| Rank | Event Date | Event Type | Similarity | Title |")
        lines.append("| --- | --- | --- | --- | --- |")
        for _, row in analogs.iterrows():
            lines.append(
                "| {rank} | {event_date} | {event_type} | {similarity:.4f} | {title} |".format(
                    rank=int(row.get("rank", 0)),
                    event_date=row.get("event_date", ""),
                    event_type=row.get("event_type", ""),
                    similarity=float(row.get("similarity", 0.0)),
                    title=row.get("title", ""),
                )
            )

    lines.extend(["", "## Event Study Highlights", ""])
    if event_study.empty:
        lines.append("No event-study rows were produced.")
    else:
        highlights = (
            event_study[event_study["horizon_days"].isin([1, 5, 20])]
            .sort_values(["symbol", "horizon_days", "event_date"])
            .head(18)
        )
        lines.append("| Symbol | Event Date | Horizon | CAR | Z-Score Proxy |")
        lines.append("| --- | --- | --- | --- | --- |")
        for _, row in highlights.iterrows():
            lines.append(
                "| {symbol} | {event_date} | {horizon}D | {car:.4f} | {z:.3f} |".format(
                    symbol=row.get("symbol", ""),
                    event_date=row.get("event_date", ""),
                    horizon=int(row.get("horizon_days", 0)),
                    car=float(row.get("cumulative_abnormal_return", 0.0)),
                    z=float(row.get("abnormal_zscore", 0.0)),
                )
            )

    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- Outputs are probabilistic and intended for monitoring/simulation only.",
            "- Rankings reflect modeled impact, not trade recommendations.",
            "- Confidence decreases when analog breadth is thin or simulated dispersion is wide.",
        ]
    )

    path.write_text("\n".join(lines), encoding="utf-8")
