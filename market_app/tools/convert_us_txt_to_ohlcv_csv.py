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


def ticker_from_filename(p: Path) -> str:
    # aapl.us.txt -> AAPL ; brk.b.us.txt -> BRK.B ; aam-ws.us.txt -> AAM-WS
    name = p.name
    if name.lower().endswith(".txt"):
        name = name[:-4]
    if name.lower().endswith(".us"):
        name = name[:-3]
    return name.upper()


def clean_col(c: str) -> str:
    c = c.strip().strip('"').strip("'")
    # remove angle brackets like <DATE>
    c = c.replace("<", "").replace(">", "")
    return c.strip().lower()


def load_universe_symbols(universe_path: Path) -> set[str]:
    df = pd.read_csv(universe_path)
    # try common column names; fallback to first column
    for col in ["symbol", "ticker", "symbols", "Symbol", "Ticker"]:
        if col in df.columns:
            syms = df[col].astype(str).str.strip().str.upper()
            return set(syms[syms != ""])
    syms = df.iloc[:, 0].astype(str).str.strip().str.upper()
    return set(syms[syms != ""])


def normalize_artius_us_txt(df: pd.DataFrame) -> pd.DataFrame:
    # normalize columns
    df = df.rename(columns={c: clean_col(c) for c in df.columns})

    # required in your schema
    # ticker, per, date, time, open, high, low, close, vol, openint
    req = ["date", "open", "high", "low", "close"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}. Found: {list(df.columns)}")

    # Filter daily only if PER exists
    if "per" in df.columns:
        df = df[df["per"].astype(str).str.upper() == "D"].copy()

    out = pd.DataFrame()
    # DATE is YYYYMMDD; TIME exists but can be ignored
    out["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce").dt.date.astype(str)

    for c in ["open", "high", "low", "close"]:
        out[c] = pd.to_numeric(df[c], errors="coerce")

    if "vol" in df.columns:
        out["volume"] = pd.to_numeric(df["vol"], errors="coerce")
    elif "volume" in df.columns:
        out["volume"] = pd.to_numeric(df["volume"], errors="coerce")

    out = out.dropna(subset=["date", "open", "high", "low", "close"]).sort_values("date").reset_index(drop=True)
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-dir", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--recursive", action="store_true")
    ap.add_argument("--universe", default=None, help="Optional universe.csv; if provided, only convert those symbols.")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--progress-every", type=int, default=200)
    args = ap.parse_args()

    in_dir = Path(args.in_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    wanted: set[str] | None = None
    if args.universe:
        wanted = load_universe_symbols(Path(args.universe))

    # collect *.us.txt (ignore MASTER/DOP/etc by construction)
    globber = in_dir.rglob("*.us.txt") if args.recursive else in_dir.glob("*.us.txt")
    candidates = [p for p in globber if p.is_file()]

    # dedupe by ticker using (file size, mtime)
    best: dict[str, Path] = {}
    best_key: dict[str, tuple[int, float]] = {}
    for p in candidates:
        t = ticker_from_filename(p)
        if wanted is not None and t not in wanted:
            continue
        key = (p.stat().st_size, p.stat().st_mtime)
        if t not in best or key > best_key[t]:
            best[t] = p
            best_key[t] = key

    tickers = sorted(best.keys())
    if args.limit:
        tickers = tickers[: args.limit]

    manifest = {
        "input_root": str(in_dir),
        "output_root": str(out_dir),
        "universe_filter": str(args.universe) if args.universe else None,
        "converted": [],
        "errors": [],
    }

    total = len(tickers)
    for i, t in enumerate(tickers, start=1):
        src = best[t]
        try:
            # Your schema is comma-separated and consistent.
            df = pd.read_csv(src, sep=",", engine="python")
            norm = normalize_artius_us_txt(df)

            out_path = out_dir / f"{t}.csv"
            norm.to_csv(out_path, index=False)

            manifest["converted"].append({
                "ticker": t,
                "source": str(src),
                "source_sha256": sha256_file(src),
                "rows": int(norm.shape[0]),
                "start_date": norm["date"].iloc[0] if len(norm) else None,
                "end_date": norm["date"].iloc[-1] if len(norm) else None,
                "output": str(out_path),
            })

            if i % args.progress_every == 0 or i == total:
                print(f"[ok] {i}/{total} converted")

        except Exception as e:
            manifest["errors"].append({"ticker": t, "source": str(src), "error": str(e)})
            if i % args.progress_every == 0 or i == total:
                print(f"[warn] {i}/{total} (latest error on {t}: {e})")

    (out_dir / "conversion_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"[done] out_dir={out_dir}")
    print(f"[done] manifest={out_dir / 'conversion_manifest.json'}")
    if manifest["errors"]:
        print(f"[done] errors={len(manifest['errors'])} (see manifest)")


if __name__ == "__main__":
    main()
