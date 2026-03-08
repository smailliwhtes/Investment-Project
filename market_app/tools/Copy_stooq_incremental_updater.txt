from __future__ import annotations

import argparse
import csv
import json
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


def _now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _safe_mkdir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _tail_last_nonempty_line(path: Path, max_bytes: int = 65536) -> Optional[str]:
    if not path.exists() or path.stat().st_size == 0:
        return None
    try:
        size = path.stat().st_size
        with path.open("rb") as f:
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
            for line in reversed(data.splitlines()):
                s = line.decode("utf-8", errors="ignore").strip()
                if s:
                    return s
    except Exception:
        return None
    return None


def _read_last_date_from_ohlcv_csv(csv_path: Path) -> Optional[str]:
    last = _tail_last_nonempty_line(csv_path)
    if not last or last.lower().startswith("date,"):
        return None
    parts = [p.strip() for p in last.split(",")]
    return parts[0] if parts else None


def _stooq_date_yyyymmdd_to_iso(d: str) -> Optional[str]:
    d = (d or "").strip()
    if len(d) != 8 or not d.isdigit():
        return None
    return f"{d[0:4]}-{d[4:6]}-{d[6:8]}"


def _ticker_from_stooq_symbol(sym: str) -> str:
    s = (sym or "").strip().upper()
    return s.split(".", 1)[0] if "." in s else s


def _iter_stooq_txt_files(root: Path, recursive: bool) -> list[Path]:
    source = root.rglob("*.us.txt") if recursive else root.glob("*.us.txt")
    return sorted([p for p in source if p.is_file()])


def _read_existing_rows(csv_path: Path) -> list[tuple[str, str, str, str, str, str]]:
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        return []
    rows: list[tuple[str, str, str, str, str, str]] = []
    with csv_path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        for row in csv.DictReader(f):
            date = (row.get("date") or "").strip()
            if not date:
                continue
            rows.append((
                date,
                str(row.get("open", "")).strip(),
                str(row.get("high", "")).strip(),
                str(row.get("low", "")).strip(),
                str(row.get("close", "")).strip(),
                str(row.get("volume", "")).strip(),
            ))
    return rows


def _atomic_write_rows(csv_path: Path, rows: list[tuple[str, str, str, str, str, str]]) -> None:
    tmp_path = csv_path.with_suffix(f"{csv_path.suffix}.tmp")
    with tmp_path.open("w", encoding="utf-8", newline="") as out:
        writer = csv.writer(out, lineterminator="\n")
        writer.writerow(["date", "open", "high", "low", "close", "volume"])
        for row in rows:
            writer.writerow(row)
    tmp_path.replace(csv_path)


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
    global_cutoff_date: Optional[str] = None,
) -> dict:
    t0 = time.time()
    _safe_mkdir(out_csv_dir)
    files = _iter_stooq_txt_files(new_stooq_dir, recursive=recursive)

    results: list[UpdateResult] = []
    errors: list[dict] = []
    processed = 0
    updated = 0
    total_appended = 0
    skipped_empty = 0

    for src in files:
        processed += 1
        try:
            if src.stat().st_size == 0:
                skipped_empty += 1
                results.append(UpdateResult("", str(src), "", None, 0, "skipped_empty"))
                continue

            with src.open("r", encoding="utf-8", errors="ignore", newline="") as f:
                reader = csv.reader(f)
                _ = next(reader, None)
                rows = [r for r in reader if r and any(cell.strip() for cell in r) and len(r) >= 9]

            if not rows:
                results.append(UpdateResult("", str(src), "", None, 0, "skipped_no_data"))
                continue

            ticker = _ticker_from_stooq_symbol(rows[0][0])
            if not ticker:
                results.append(UpdateResult("", str(src), "", None, 0, "skipped_bad_ticker"))
                continue

            out_csv = out_csv_dir / f"{ticker}.csv"
            if global_cutoff_date:
                last_existing = global_cutoff_date
            elif out_csv.exists():
                last_existing = _read_last_date_from_ohlcv_csv(out_csv)
            else:
                last_existing = _read_last_date_from_ohlcv_csv(existing_csv_dir / f"{ticker}.csv")

            appended_rows: list[tuple[str, str, str, str, str, str]] = []
            for r in rows:
                per = (r[1] or "").strip().upper()
                if per != "D":
                    continue
                iso = _stooq_date_yyyymmdd_to_iso(r[2])
                if not iso:
                    continue
                if last_existing and iso <= last_existing:
                    continue
                appended_rows.append((iso, r[4], r[5], r[6], r[7], r[8]))

            if appended_rows:
                if not dry_run:
                    baseline = existing_csv_dir / f"{ticker}.csv"
                    existing_rows = _read_existing_rows(out_csv if out_csv.exists() else baseline)
                    merged = existing_rows + appended_rows
                    deduped = {row[0]: row for row in merged}
                    ordered = [deduped[d] for d in sorted(deduped)]
                    _safe_mkdir(out_csv.parent)
                    _atomic_write_rows(out_csv, ordered)
                updated += 1
                total_appended += len(appended_rows)
                status = "would_update" if dry_run else "updated"
                results.append(UpdateResult(ticker, str(src), str(out_csv), last_existing, len(appended_rows), status))
            else:
                results.append(UpdateResult(ticker, str(src), str(out_csv), last_existing, 0, "no_change"))
        except Exception as e:
            errors.append({"source": str(src), "error": repr(e)})
            results.append(UpdateResult("", str(src), "", None, 0, "error", repr(e)))

    return {
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


def _run_gui() -> None:
    import tkinter as tk
    from tkinter import filedialog, messagebox, ttk

    root = tk.Tk()
    root.title("Stooq Incremental OHLCV Updater")

    existing_var = tk.StringVar()
    new_var = tk.StringVar()
    out_var = tk.StringVar()
    recursive_var = tk.BooleanVar(value=True)
    dryrun_var = tk.BooleanVar(value=False)
    cutoff_var = tk.StringVar()
    status_var = tk.StringVar(value="Idle.")

    def browse_dir(var: tk.StringVar) -> None:
        p = filedialog.askdirectory()
        if p:
            var.set(p)

    def append_log(msg: str) -> None:
        log.configure(state="normal")
        log.insert("end", msg + "\n")
        log.see("end")
        log.configure(state="disabled")

    def run_clicked() -> None:
        ex = existing_var.get().strip()
        nw = new_var.get().strip()
        out = out_var.get().strip() or ex
        if not ex or not nw:
            messagebox.showerror("Missing paths", "Select Existing CSV dir and New Stooq dir.")
            return

        btn_run.configure(state="disabled")
        progress.start(10)

        def worker() -> None:
            try:
                manifest = update_from_stooq_dump(
                    existing_csv_dir=Path(ex),
                    new_stooq_dir=Path(nw),
                    out_csv_dir=Path(out),
                    recursive=recursive_var.get(),
                    dry_run=dryrun_var.get(),
                    global_cutoff_date=cutoff_var.get().strip() or None,
                )
                man_path = Path(out) / f"stooq_incremental_update_manifest_{_now_stamp()}.json"
                with man_path.open("w", encoding="utf-8") as mf:
                    json.dump(manifest, mf, indent=2)
                append_log(f"Done. Manifest: {man_path}")
                status_var.set("Complete")
            except Exception as e:
                append_log(f"ERROR: {e!r}")
                status_var.set("Failed")
            finally:
                progress.stop()
                btn_run.configure(state="normal")

        threading.Thread(target=worker, daemon=True).start()

    frm = ttk.Frame(root, padding=12)
    frm.grid(row=0, column=0, sticky="nsew")
    frm.columnconfigure(1, weight=1)
    frm.rowconfigure(6, weight=1)

    ttk.Label(frm, text="Existing OHLCV CSV folder:").grid(row=0, column=0, sticky="w")
    ttk.Entry(frm, textvariable=existing_var, width=80).grid(row=0, column=1, sticky="ew")
    ttk.Button(frm, text="Browse", command=lambda: browse_dir(existing_var)).grid(row=0, column=2)

    ttk.Label(frm, text="New Stooq dump folder:").grid(row=1, column=0, sticky="w")
    ttk.Entry(frm, textvariable=new_var, width=80).grid(row=1, column=1, sticky="ew")
    ttk.Button(frm, text="Browse", command=lambda: browse_dir(new_var)).grid(row=1, column=2)

    ttk.Label(frm, text="Output CSV folder (blank=in-place):").grid(row=2, column=0, sticky="w")
    ttk.Entry(frm, textvariable=out_var, width=80).grid(row=2, column=1, sticky="ew")
    ttk.Button(frm, text="Browse", command=lambda: browse_dir(out_var)).grid(row=2, column=2)

    opts = ttk.Frame(frm)
    opts.grid(row=3, column=0, columnspan=3, sticky="w")
    ttk.Checkbutton(opts, text="Recursive", variable=recursive_var).grid(row=0, column=0)
    ttk.Checkbutton(opts, text="Dry run", variable=dryrun_var).grid(row=0, column=1)
    ttk.Label(opts, text="Cutoff YYYY-MM-DD:").grid(row=0, column=2)
    ttk.Entry(opts, textvariable=cutoff_var, width=12).grid(row=0, column=3)

    btn_run = ttk.Button(frm, text="Run update", command=run_clicked)
    btn_run.grid(row=4, column=0, sticky="w")
    progress = ttk.Progressbar(frm)
    progress.grid(row=4, column=1, columnspan=2, sticky="ew")
    ttk.Label(frm, textvariable=status_var).grid(row=5, column=0, columnspan=3, sticky="w")

    log = tk.Text(frm, height=16, width=100, state="disabled")
    log.grid(row=6, column=0, columnspan=3, sticky="nsew")
    root.mainloop()


def main() -> int:
    parser = argparse.ArgumentParser(description="Incrementally update OHLCV CSVs from Stooq dumps")
    parser.add_argument("--gui", action="store_true")
    parser.add_argument("--existing-csv-dir")
    parser.add_argument("--new-stooq-dir")
    parser.add_argument("--out-csv-dir")
    parser.add_argument("--recursive", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--cutoff", default="")
    args = parser.parse_args()

    if args.gui:
        _run_gui()
        return 0
    if not args.existing_csv_dir or not args.new_stooq_dir:
        parser.error("CLI mode requires --existing-csv-dir and --new-stooq-dir (or use --gui)")

    existing = Path(args.existing_csv_dir)
    outdir = Path(args.out_csv_dir) if args.out_csv_dir else existing
    manifest = update_from_stooq_dump(
        existing_csv_dir=existing,
        new_stooq_dir=Path(args.new_stooq_dir),
        out_csv_dir=outdir,
        recursive=bool(args.recursive),
        dry_run=bool(args.dry_run),
        global_cutoff_date=args.cutoff.strip() or None,
    )
    man_path = outdir / f"stooq_incremental_update_manifest_{_now_stamp()}.json"
    with man_path.open("w", encoding="utf-8") as mf:
        json.dump(manifest, mf, indent=2)
    c = manifest["counts"]
    print(
        f"[done] files_found={c['files_found']} processed={c['processed']} updated_tickers={c['updated_tickers']} "
        f"rows_appended={c['total_rows_appended']} skipped_empty={c['skipped_empty']} errors={c['errors']}"
    )
    print(f"[done] manifest={man_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
