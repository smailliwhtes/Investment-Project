# tools/convert_us_txt_to_ohlcv_csv.py
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def infer_delim(sample: str) -> str:
    # Typical: comma or semicolon; occasionally tab.
    if ";" in sample and "," not in sample:
        return ";"
    if "\t" in sample and "," not in sample and ";" not in sample:
        return "\t"
    return ","


def ticker_from_filename(p: Path) -> str:
    # aapl.us.txt -> AAPL ; brk.b.us.txt -> BRK.B ; aam-ws.us.txt -> AAM-WS
    stem = p.name
    if stem.lower().endswith(".txt"):
        stem = stem[:-4]
    # remove trailing ".us" ONLY
    if stem.lower().endswith(".us"):
        stem = stem[:-3]
    return stem.upper()


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower().strip(): c for c in df.columns}

    # Common variants
    date_c = cols.get("date") or cols.get("datetime") or cols.get("time") or None
    open_c = cols.get("open")
    high_c = cols.get("high")
    low_c = cols.get("low")
    close_c = cols.get("close") or cols.get("closing") or cols.get("adj close") or cols.get("adj_close")
    vol_c = cols.get("volume") or cols.get("vol")

    missing = [k for k, v in {
        "date": date_c, "open": open_c, "high": high_c, "low": low_c, "close": close_c
    }.items() if v is None]
    if missing:
        raise ValueError(f"Missing required columns: {missing}. Found: {list(df.columns)}")

    out = df[[date_c, open_c, high_c, low_c, close_c] + ([vol_c] if vol_c else [])].copy()
    out.columns = ["date", "open", "high", "low", "close"] + (["volume"] if vol_c else [])

    # Parse/clean
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date.astype(str)
    for c in ["open", "high", "low", "close"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    if "volume" in out.columns:
        out["volume"] = pd.to_numeric(out["volume"], errors="coerce")

    out = out.dropna(subset=["date", "open", "high", "low", "close"]).sort_values("date").reset_index(drop=True)
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-dir", required=True, help="Folder containing *.us.txt files (can be recursive).")
    ap.add_argument("--out-dir", required=True, help="Output folder for normalized per-ticker CSVs.")
    ap.add_argument("--recursive", action="store_true", help="Recurse into subfolders.")
    ap.add_argument("--limit", type=int, default=None, help="Limit number of tickers processed (debug).")
    args = ap.parse_args()

    in_dir = Path(args.in_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    globber = in_dir.rglob("*.us.txt") if args.recursive else in_dir.glob("*.us.txt")
    files = sorted([p for p in globber if p.is_file()])

    # Deduplicate by ticker: choose file with most rows; if tie, newest mtime.
    best: dict[str, Path] = {}
    meta: dict[str, tuple[int, float]] = {}  # rows, mtime
    for p in files:
        t = ticker_from_filename(p)
        # quick row-count estimate (cheap-ish): count lines
        try:
            n_lines = sum(1 for _ in p.open("r", encoding="utf-8", errors="ignore"))
        except Exception:
            continue
        mtime = p.stat().st_mtime
        if t not in best or (n_lines, mtime) > meta[t]:
            best[t] = p
            meta[t] = (n_lines, mtime)

    tickers = sorted(best.keys())
    if args.limit:
        tickers = tickers[: args.limit]

    manifest = {"input_root": str(in_dir), "output_root": str(out_dir), "tickers": []}
    processed = 0

    for t in tickers:
        src = best[t]
        try:
            with src.open("r", encoding="utf-8", errors="ignore") as f:
                head = f.readline()
            delim = infer_delim(head)

            df = pd.read_csv(src, sep=delim, engine="python")
            norm = normalize_columns(df)

            out_path = out_dir / f"{t}.csv"
            norm.to_csv(out_path, index=False)

            manifest["tickers"].append({
                "ticker": t,
                "source": str(src),
                "source_sha256": sha256_file(src),
                "rows": int(norm.shape[0]),
                "start_date": norm["date"].iloc[0] if len(norm) else None,
                "end_date": norm["date"].iloc[-1] if len(norm) else None,
                "output": str(out_path),
            })

            processed += 1
            if processed % 250 == 0:
                print(f"[ok] {processed}/{len(tickers)}")

        except Exception as e:
            manifest["tickers"].append({"ticker": t, "source": str(src), "error": str(e)})

    (out_dir / "conversion_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"[done] wrote {processed} csvs to: {out_dir}")
    print(f"[done] manifest: {out_dir / 'conversion_manifest.json'}")


if __name__ == "__main__":
    main()
