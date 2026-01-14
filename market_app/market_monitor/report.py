from datetime import datetime
from pathlib import Path
from typing import Dict

import pandas as pd


def write_report(path: Path, summary: Dict[str, int], scored: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Market Monitor Report",
        "",
        f"Generated: {datetime.utcnow().isoformat()}Z",
        "",
        "## Stage Summary",
        "",
        f"- Universe size: {summary.get('universe', 0)}",
        f"- Stage 1 survivors: {summary.get('stage1', 0)}",
        f"- Stage 2 eligible prelim: {summary.get('stage2', 0)}",
        f"- Stage 3 scored: {summary.get('stage3', 0)}",
        "",
        "## Top Monitor Priority",
        "",
    ]
    if not scored.empty:
        top = scored.sort_values("monitor_priority_1_10", ascending=False).head(15)
        lines.append("| Symbol | Name | Monitor Priority | Notes |")
        lines.append("| --- | --- | --- | --- |")
        for _, row in top.iterrows():
            lines.append(
                f"| {row.get('symbol')} | {row.get('name')} | {row.get('monitor_priority_1_10')} | {row.get('notes')} |"
            )
    else:
        lines.append("No scored symbols available.")
    lines.append("")
    lines.append("## Notes")
    lines.append("This report is monitoring-only and contains no trading recommendations.")

    path.write_text("\n".join(lines), encoding="utf-8")
