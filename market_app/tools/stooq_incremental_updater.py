import argparse
import csv
import json
import os
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, List, Tuple

# -----------------------------
# Core helpers (fast + robust)
# -----------------------------

def _now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

def _safe_mkdir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def _tail_last_nonempty_line(path: Path, max_bytes: int = 65536) -> Optional[str]:
    """
    Read the last non-empty line from a text file efficiently.
    Returns None if file is missing/empty.
    """
    if not path.exists():
        return None
    try:
        size = path.stat().st_size
        if size == 0:
            return None
        with path.open("rb") as f:
            # Read from the end in chunks
            chunk_size = 8192
            read_bytes = 0
            data = b""
            while read_bytes < max_bytes:
                step = min(chunk_size, size - read_bytes)
                if step <= 0:
                    break
                f.seek(size - read_bytes - step)
                data = f.read(step) + data
                read_bytes += step
                if b"\n" in data:
                    break
            # Split and find last non-empty
            lines = data.splitlines()
            for i in range(len(lines) - 1, -1, -1):
                line = lines[i].decode("utf-8", errors="ignore").strip()
                if line:
                    return line
    except Exception:
        return None
    return None

def _read_last_date_from_ohlcv_csv(csv_path: Path) -> Optional[str]:
    """
    Expects OHLCV csv in format:
      date,open,high,low,close,volume
    Returns last date 'YYYY-MM-DD' or None.
    """
    last = _tail_last_nonempty_line(csv_path)
    if not last:
        return None
    # Skip if last line is header
    if last.lower().startswith("date,"):
        return None
    parts = [p.strip() for p in last.split(",")]
    if not parts or len(parts[0]) < 8:
        return None
    # date should already be YYYY-MM-DD
    return parts[0]

def _stooq_date_yyyymmdd_to_iso(d: str) -> Optional[str]:
    d = (d or "").strip()
    if len(d) != 8 or not d.isdigit():
        return None
    return f"{d[0:4]}-{d[4:6]}-{d[6:8]}"

def _ticker_from_stooq_symbol(sym: str) -> str:
    # e.g. "AAPL.US" -> "AAPL"
    s = (sym or "").strip().upper()
    if "." in s:
        s = s.split(".", 1)[0]
    return s

def _iter_stooq_txt_files(root: Path, recursive: bool) -> List[Path]:
    if recursive:
        return [p for p in root.rglob("*.us.txt") if p.is_file()]
    return [p for p in root.glob("*.us.txt") if p.is_file()]

@dataclass
class UpdateResult:
    ticker: str
    source: str
    out_csv: str
    last_existing_date: Optional[str]
    rows_appended: int
    status: str
    error: Optional[str] = None

def update_from_stooq_dump(
    existing_csv_dir: Path,
    new_stooq_dir: Path,
    out_csv_dir: Path,
    recursive: bool = True,
    dry_run: bool = False,
    global_cutoff_date: Optional[str] = None,  # 'YYYY-MM-DD' (optional fast mode)
) -> Dict:
    """
    Incrementally update OHLCV CSVs from a Stooq dump folder containing *.us.txt files.
    For each ticker, append only dates strictly greater than last existing date.
    """
    t0 = time.time()
    _safe_mkdir(out_csv_dir)

    files = _iter_stooq_txt_files(new_stooq_dir, recursive=recursive)

    results: List[UpdateResult] = []
    errors: List[Dict] = []

    processed = 0
    updated = 0
    total_appended = 0
    skipped_empty = 0

    for src in files:
        processed += 1
        try:
            if src.stat().st_size == 0:
                skipped_empty += 1
                results.append(UpdateResult(
                    ticker="",
                    source=str(src),
                    out_csv="",
                    last_existing_date=None,
                    rows_appended=0,
                    status="skipped_empty",
                ))
                continue

            # Read as CSV (stooq format)
            # <TICKER>,<PER>,<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>,<OPENINT>
            with src.open("r", encoding="utf-8", errors="ignore", newline="") as f:
                reader = csv.reader(f)
                header = next(reader, None)
                first_data = None
                for row in reader:
                    if row and any(cell.strip() for cell in row):
                        first_data = row
                        break

                if not first_data or len(first_data) < 9:
                    results.append(UpdateResult(
                        ticker="",
                        source=str(src),
                        out_csv="",
                        last_existing_date=None,
                        rows_appended=0,
                        status="skipped_no_data",
                    ))
                    continue

                ticker = _ticker_from_stooq_symbol(first_data[0])
                if not ticker:
                    results.append(UpdateResult(
                        ticker="",
                        source=str(src),
                        out_csv="",
                        last_existing_date=None,
                        rows_appended=0,
                        status="skipped_bad_ticker",
                    ))
                    continue

                out_csv = out_csv_dir / f"{ticker}.csv"

                # Determine cutoff date
                last_existing = None
                if global_cutoff_date:
                    last_existing = global_cutoff_date
                else:
                    # Prefer reading from out dir (in-place) if exists; else from existing dir
                    if out_csv.exists():
                        last_existing = _read_last_date_from_ohlcv_csv(out_csv)
                    else:
                        last_existing = _read_last_date_from_ohlcv_csv(existing_csv_dir / f"{ticker}.csv")

                # If we are not writing in-place and file exists in existing_csv_dir, we may need to copy baseline
                baseline_path = existing_csv_dir / f"{ticker}.csv"
                if (not out_csv.exists()) and baseline_path.exists() and (out_csv_dir.resolve() != existing_csv_dir.resolve()):
                    if not dry_run:
                        _safe_mkdir(out_csv.parent)
                        out_csv.write_bytes(baseline_path.read_bytes())

                # Rewind and stream again from the top; append only new rows
                f.seek(0)
                reader2 = csv.reader(f)
                _ = next(reader2, None)  # header line

                appended_rows: List[Tuple[str, str, str, str, str, str]] = []
                # Include the first data row we already read
                pending_first = first_data

                def handle_row(r: List[str]) -> None:
                    nonlocal appended_rows
                    if not r or len(r) < 9:
                        return
                    sym, per, d, _tm, o, h, l, c, v = r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8]
                    if (per or "").strip().upper() != "D":
                        return
                    iso = _stooq_date_yyyymmdd_to_iso(d)
                    if not iso:
                        return
                    if last_existing and iso <= last_existing:
                        return
                    appended_rows.append((iso, o, h, l, c, v))

                handle_row(pending_first)
                for row in reader2:
                    handle_row(row)

                if appended_rows:
                    # Ensure output has header
                    if not dry_run:
                        _safe_mkdir(out_csv.parent)
                        need_header = (not out_csv.exists()) or (out_csv.stat().st_size == 0)
                        with out_csv.open("a", encoding="utf-8", newline="") as outf:
                            w = csv.writer(outf, lineterminator="\n")
                            if need_header:
                                w.writerow(["date", "open", "high", "low", "close", "volume"])
                            for r in appended_rows:
                                w.writerow(r)

                    updated += 1
                    total_appended += len(appended_rows)
                    results.append(UpdateResult(
                        ticker=ticker,
                        source=str(src),
                        out_csv=str(out_csv),
                        last_existing_date=last_existing,
                        rows_appended=len(appended_rows),
                        status="updated" if not dry_run else "would_update",
                    ))
                else:
                    results.append(UpdateResult(
                        ticker=ticker,
                        source=str(src),
                        out_csv=str(out_csv),
                        last_existing_date=last_existing,
                        rows_appended=0,
                        status="no_change",
                    ))

        except Exception as e:
            err = {"source": str(src), "error": repr(e)}
            errors.append(err)
            results.append(UpdateResult(
                ticker="",
                source=str(src),
                out_csv="",
                last_existing_date=None,
                rows_appended=0,
                status="error",
                error=repr(e),
            ))

    manifest = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "existing_csv_dir": str(existing_csv_dir),
        "new_stooq_dir": str(new_stooq_dir),
        "out_csv_dir": str(out_csv_dir),
        "recursive": recursive,
        "dry_run": dry_run,
        "global_cutoff_date": global_cutoff_date,
        "counts": {
            "files_found": len(files),
            "processed": processed,
            "updated_tickers": updated,
            "total_rows_appended": total_appended,
            "skipped_empty": skipped_empty,
            "errors": len(errors),
        },
        "results": [r.__dict__ for r in results],
        "errors": errors,
        "elapsed_seconds": round(time.time() - t0, 3),
    }
    return manifest

# -----------------------------
# Tkinter GUI
# -----------------------------

def _run_gui() -> None:
    import tkinter as tk
    from tkinter import ttk, filedialog, messagebox

    root = tk.Tk()
    root.title("Stooq Incremental OHLCV Updater")

    # Fields
    existing_var = tk.StringVar()
    new_var = tk.StringVar()
    out_var = tk.StringVar()
    recursive_var = tk.BooleanVar(value=True)
    dryrun_var = tk.BooleanVar(value=False)
    cutoff_var = tk.StringVar()

    # Status
    status_var = tk.StringVar(value="Idle.")
    progress_var = tk.DoubleVar(value=0.0)

    def browse_dir(var: tk.StringVar) -> None:
        p = filedialog.askdirectory()
        if p:
            var.set(p)

    def append_log(msg: str) -> None:
        log.configure(state="normal")
        log.insert("end", msg + "\n")
        log.see("end")
        log.configure(state="disabled")

    def set_status(msg: str) -> None:
        status_var.set(msg)
        root.update_idletasks()

    def run_clicked() -> None:
        ex = existing_var.get().strip()
        nw = new_var.get().strip()
        out = out_var.get().strip() or ex
        if not ex or not nw:
            messagebox.showerror("Missing paths", "Select Existing CSV dir and New Stooq dir.")
            return

        # Validate cutoff format if provided
        cutoff = cutoff_var.get().strip() or None
        if cutoff and (len(cutoff) != 10 or cutoff[4] != "-" or cutoff[7] != "-"):
            messagebox.showerror("Bad cutoff", "Cutoff must be YYYY-MM-DD (or blank).")
            return

        btn_run.configure(state="disabled")
        progress.configure(mode="indeterminate")
        progress.start(10)
        log.configure(state="normal")
        log.delete("1.0", "end")
        log.configure(state="disabled")

        def worker():
            try:
                set_status("Running update...")
                append_log(f"Existing CSV dir: {ex}")
                append_log(f"New Stooq dir:     {nw}")
                append_log(f"Output CSV dir:    {out}")
                append_log(f"Recursive:         {recursive_var.get()}")
                append_log(f"Dry run:           {dryrun_var.get()}")
                append_log(f"Global cutoff:     {cutoff or '(per-ticker last date)'}")
                append_log("")

                manifest = update_from_stooq_dump(
                    existing_csv_dir=Path(ex),
                    new_stooq_dir=Path(nw),
                    out_csv_dir=Path(out),
                    recursive=recursive_var.get(),
                    dry_run=dryrun_var.get(),
                    global_cutoff_date=cutoff,
                )

                # Write manifest beside out dir
                out_dir = Path(out)
                _safe_mkdir(out_dir)
                man_path = out_dir / f"stooq_incremental_update_manifest_{_now_stamp()}.json"
                with man_path.open("w", encoding="utf-8") as mf:
                    json.dump(manifest, mf, indent=2)

                c = manifest["counts"]
                append_log("")
                append_log("Done.")
                append_log(f"Files found:       {c['files_found']}")
                append_log(f"Processed:         {c['processed']}")
                append_log(f"Updated tickers:   {c['updated_tickers']}")
                append_log(f"Rows appended:     {c['total_rows_appended']}")
                append_log(f"Skipped empty:     {c['skipped_empty']}")
                append_log(f"Errors:            {c['errors']}")
                append_log(f"Manifest:          {man_path}")
                set_status("Complete.")

            except Exception as e:
                append_log(f"ERROR: {repr(e)}")
                set_status("Failed.")
            finally:
                progress.stop()
                progress.configure(mode="determinate")
                btn_run.configure(state="normal")

        threading.Thread(target=worker, daemon=True).start()

    frm = ttk.Frame(root, padding=12)
    frm.grid(row=0, column=0, sticky="nsew")

    root.columnconfigure(0, weight=1)
    root.rowconfigure(0, weight=1)
    frm.columnconfigure(1, weight=1)

    ttk.Label(frm, text="Existing OHLCV CSV folder (your current main data):").grid(row=0, column=0, sticky="w")
    ttk.Entry(frm, textvariable=existing_var, width=80).grid(row=0, column=1, sticky="ew")
    ttk.Button(frm, text="Browse", command=lambda: browse_dir(existing_var)).grid(row=0, column=2, padx=(8, 0))

    ttk.Label(frm, text="New Stooq dump folder (contains *.us.txt):").grid(row=1, column=0, sticky="w", pady=(8, 0))
    ttk.Entry(frm, textvariable=new_var, width=80).grid(row=1, column=1, sticky="ew", pady=(8, 0))
    ttk.Button(frm, text="Browse", command=lambda: browse_dir(new_var)).grid(row=1, column=2, padx=(8, 0), pady=(8, 0))

    ttk.Label(frm, text="Output CSV folder (blank = update in place):").grid(row=2, column=0, sticky="w", pady=(8, 0))
    ttk.Entry(frm, textvariable=out_var, width=80).grid(row=2, column=1, sticky="ew", pady=(8, 0))
    ttk.Button(frm, text="Browse", command=lambda: browse_dir(out_var)).grid(row=2, column=2, padx=(8, 0), pady=(8, 0))

    opts = ttk.Frame(frm)
    opts.grid(row=3, column=0, columnspan=3, sticky="w", pady=(10, 0))
    ttk.Checkbutton(opts, text="Recursive scan", variable=recursive_var).grid(row=0, column=0, padx=(0, 12))
    ttk.Checkbutton(opts, text="Dry run (no writes)", variable=dryrun_var).grid(row=0, column=1, padx=(0, 12))
    ttk.Label(opts, text="Optional global cutoff YYYY-MM-DD:").grid(row=0, column=2, padx=(0, 6))
    ttk.Entry(opts, textvariable=cutoff_var, width=12).grid(row=0, column=3)

    btn_run = ttk.Button(frm, text="Run update", command=run_clicked)
    btn_run.grid(row=4, column=0, sticky="w", pady=(12, 0))

    progress = ttk.Progressbar(frm)
    progress.grid(row=4, column=1, columnspan=2, sticky="ew", pady=(12, 0))

    ttk.Label(frm, textvariable=status_var).grid(row=5, column=0, columnspan=3, sticky="w", pady=(8, 0))

    log = tk.Text(frm, height=18, width=110, state="disabled")
    log.grid(row=6, column=0, columnspan=3, sticky="nsew", pady=(10, 0))
    frm.rowconfigure(6, weight=1)

    # Prefill with your current paths (edit if desired)
    existing_var.set(r"C:\Users\micha\OneDrive\Desktop\Market_Files\ohlcv_daily_csv")
    new_var.set(r"C:\Users\micha\OneDrive\Desktop\Market_Files\Stooq_11FEB2026\ticker.us.txt_files\daily\us")
    out_var.set("")  # in place

    root.minsize(980, 520)
    root.mainloop()

# -----------------------------
# CLI entry
# -----------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Incrementally update OHLCV CSVs from Stooq *.us.txt dumps.")
    ap.add_argument("--gui", action="store_true", help="Launch GUI.")
    ap.add_argument("--existing-csv-dir", help="Folder with existing OHLCV CSVs (date,open,high,low,close,volume).")
    ap.add_argument("--new-stooq-dir", help="Folder containing new Stooq *.us.txt files.")
    ap.add_argument("--out-csv-dir", help="Output folder for updated CSVs (blank = update in place).")
    ap.add_argument("--recursive", action="store_true", help="Recurse into subfolders for *.us.txt.")
    ap.add_argument("--dry-run", action="store_true", help="Scan and report only; do not write.")
    ap.add_argument("--cutoff", default="", help="Optional global cutoff date YYYY-MM-DD (skip reading per-ticker last date).")
    args = ap.parse_args()

    if args.gui:
        _run_gui()
        return 0

    if not args.existing_csv_dir or not args.new_stooq_dir:
        ap.error("CLI mode requires --existing-csv-dir and --new-stooq-dir (or use --gui).")

    existing = Path(args.existing_csv_dir)
    newdir = Path(args.new_stooq_dir)
    outdir = Path(args.out_csv_dir) if args.out_csv_dir else existing

    cutoff = args.cutoff.strip() or None

    manifest = update_from_stooq_dump(
        existing_csv_dir=existing,
        new_stooq_dir=newdir,
        out_csv_dir=outdir,
        recursive=bool(args.recursive),
        dry_run=bool(args.dry_run),
        global_cutoff_date=cutoff,
    )

    man_path = outdir / f"stooq_incremental_update_manifest_{_now_stamp()}.json"
    with man_path.open("w", encoding="utf-8") as mf:
        json.dump(manifest, mf, indent=2)

    c = manifest["counts"]
    print(f"[done] files_found={c['files_found']} processed={c['processed']} updated_tickers={c['updated_tickers']}"
          f" rows_appended={c['total_rows_appended']} skipped_empty={c['skipped_empty']} errors={c['errors']}")
    print(f"[done] manifest={man_path}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
