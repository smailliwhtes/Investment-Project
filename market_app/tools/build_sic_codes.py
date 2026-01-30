from __future__ import annotations

import argparse
import csv
import html
from html.parser import HTMLParser
from pathlib import Path


class SicTableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._in_td = False
        self._current_row: list[str] = []
        self.rows: list[list[str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() == "td":
            self._in_td = True

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "td":
            self._in_td = False
        if tag.lower() == "tr":
            if self._current_row:
                self.rows.append(self._current_row)
                self._current_row = []

    def handle_data(self, data: str) -> None:
        if self._in_td:
            cleaned = html.unescape(data).strip()
            if cleaned:
                self._current_row.append(cleaned)


def parse_sic_html(path: Path) -> list[tuple[str, str, str]]:
    parser = SicTableParser()
    parser.feed(path.read_text(encoding="utf-8", errors="replace"))
    rows = []
    for row in parser.rows:
        if len(row) >= 3 and row[0].isdigit():
            rows.append((row[0].strip(), row[1].strip(), row[2].strip()))
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description="Parse SEC SIC HTML into CSV.")
    ap.add_argument("--html", required=True, help="Path to siccodes.html downloaded from SEC.")
    ap.add_argument("--output", required=True, help="Output CSV path.")
    args = ap.parse_args()

    html_path = Path(args.html)
    output_path = Path(args.output)
    rows = parse_sic_html(html_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["sic", "office", "industry_title"])
        writer.writerows(rows)


if __name__ == "__main__":
    main()
