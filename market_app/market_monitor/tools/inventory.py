from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from market_monitor.time_utils import utc_now_iso
from pathlib import Path

from market_monitor.config.discovery import (
    ensure_required_symbols_file,
    find_repo_root,
)
from market_monitor.metadata.security_master import SECURITY_MASTER_COLUMNS


@dataclass(frozen=True)
class InventoryConfig:
    repo_root: Path
    stooq_root: Path | None
    metastock_root: Path | None
    corpus_root: Path | None
    output_dir: Path


def build_inventory(config: InventoryConfig) -> dict:
    required_symbols_path = ensure_required_symbols_file(config.repo_root)
    security_master_path = config.repo_root / "out" / "security_master.csv"

    stooq_summary = _summarize_stooq(config.stooq_root)
    metastock_summary = _summarize_metastock(config.metastock_root)
    corpus_summary = _summarize_corpus(config.corpus_root)
    security_master_summary = _summarize_security_master(security_master_path)

    payload = {
        "generated_at": utc_now_iso(),
        "paths": {
            "repo_root": str(config.repo_root),
            "stooq_root": str(config.stooq_root) if config.stooq_root else None,
            "metastock_root": str(config.metastock_root) if config.metastock_root else None,
            "corpus_root": str(config.corpus_root) if config.corpus_root else None,
            "required_symbols_file": str(required_symbols_path),
            "security_master_path": str(security_master_path),
        },
        "stooq_txt": stooq_summary,
        "metastock": metastock_summary,
        "corpus": corpus_summary,
        "security_master": security_master_summary,
    }
    return payload


def write_inventory(config: InventoryConfig) -> dict:
    payload = build_inventory(config)
    config.output_dir.mkdir(parents=True, exist_ok=True)
    json_path = config.output_dir / "inventory.json"
    md_path = config.output_dir / "inventory.md"
    with json_path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(payload, indent=2))
    with md_path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(_render_inventory_md(payload))
    return payload


def _summarize_stooq(root: Path | None) -> dict:
    if root is None:
        return {"exists": False, "total_files": 0, "buckets": {}}
    if not root.exists():
        return {"exists": False, "total_files": 0, "buckets": {}, "path": str(root)}
    buckets: dict[str, int] = {}
    total = 0
    for path in root.rglob("*.us.txt"):
        bucket = _stooq_bucket(root, path)
        buckets[bucket] = buckets.get(bucket, 0) + 1
        total += 1
    return {"exists": True, "total_files": total, "buckets": dict(sorted(buckets.items()))}


def _summarize_metastock(root: Path | None) -> dict:
    if root is None:
        return {"exists": False}
    if not root.exists():
        return {"exists": False, "path": str(root)}
    xmaster_count = len(list(root.rglob("XMASTER")))
    dop_count = len(list(root.rglob("F*.DOP")))
    master_files = [path.name for path in root.glob("*MASTER") if path.is_file()]
    return {
        "exists": True,
        "xmaster_count": xmaster_count,
        "dop_count": dop_count,
        "master_files": sorted(master_files),
    }


def _summarize_corpus(root: Path | None) -> dict:
    if root is None:
        return {"exists": False, "file_count": 0, "files": []}
    if not root.exists():
        return {"exists": False, "file_count": 0, "files": [], "path": str(root)}
    files = []
    for path in sorted(root.iterdir()):
        if path.is_file():
            files.append({"name": path.name, "bytes": path.stat().st_size})
    return {"exists": True, "file_count": len(files), "files": files}


def _summarize_security_master(path: Path) -> dict:
    if not path.exists():
        return {"exists": False, "schema_ok": False}
    with path.open("r", encoding="utf-8") as handle:
        header = handle.readline().strip().split(",")
        rows = sum(1 for _ in handle)
    schema_ok = header == SECURITY_MASTER_COLUMNS
    return {"exists": True, "schema_ok": schema_ok, "row_count": rows, "columns": header}


def _stooq_bucket(root: Path, path: Path) -> str:
    try:
        relative = path.relative_to(root)
    except ValueError:
        return "unknown"
    parts = list(relative.parts)
    if not parts:
        return "unknown"
    bucket_parts = parts[:-1]
    if len(bucket_parts) >= 2 and bucket_parts[-1].isdigit():
        bucket = "/".join(bucket_parts[-2:])
    elif bucket_parts:
        bucket = "/".join(bucket_parts)
    else:
        bucket = "root"
    return bucket


def _render_inventory_md(payload: dict) -> str:
    lines = [
        "# Offline Asset Inventory",
        "",
        f"Generated: {payload.get('generated_at')}",
        "",
        "## Paths",
    ]
    paths = payload.get("paths", {})
    for key in (
        "repo_root",
        "stooq_root",
        "metastock_root",
        "corpus_root",
        "required_symbols_file",
        "security_master_path",
    ):
        lines.append(f"- **{key}**: {paths.get(key)}")

    stooq = payload.get("stooq_txt", {})
    lines.extend(["", "## Stooq TXT", f"- Exists: {stooq.get('exists')}"])
    lines.append(f"- Total files: {stooq.get('total_files', 0)}")
    buckets = stooq.get("buckets", {})
    if buckets:
        lines.append("- Buckets:")
        for bucket, count in buckets.items():
            lines.append(f"  - {bucket}: {count}")

    metastock = payload.get("metastock", {})
    lines.extend(["", "## MetaStock", f"- Exists: {metastock.get('exists')}"])
    if metastock.get("exists"):
        lines.append(f"- XMASTER count: {metastock.get('xmaster_count')}")
        lines.append(f"- F*.DOP count: {metastock.get('dop_count')}")
        lines.append(f"- MASTER files: {', '.join(metastock.get('master_files', []))}")

    corpus = payload.get("corpus", {})
    lines.extend(["", "## NLP Corpus", f"- Exists: {corpus.get('exists')}"])
    lines.append(f"- File count: {corpus.get('file_count', 0)}")
    if corpus.get("files"):
        lines.append("- Top-level files:")
        for item in corpus["files"]:
            lines.append(f"  - {item['name']} ({item['bytes']} bytes)")

    sec_master = payload.get("security_master", {})
    lines.extend(
        [
            "",
            "## Security Master",
            f"- Exists: {sec_master.get('exists')}",
            f"- Schema OK: {sec_master.get('schema_ok')}",
        ]
    )
    if sec_master.get("exists"):
        lines.append(f"- Row count: {sec_master.get('row_count')}")

    lines.append("")
    return "\n".join(lines)


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Inventory offline assets and config paths.")
    ap.add_argument("--stooq-root", help="Root directory for Stooq TXT pack.")
    ap.add_argument("--metastock-root", help="Root directory for MetaStock pack.")
    ap.add_argument("--corpus-root", help="Root directory for NLP corpus.")
    ap.add_argument(
        "--write-out",
        default="out",
        help="Output directory for inventory.json/markdown (default: out).",
    )
    return ap.parse_args()


def main() -> None:
    args = _parse_args()
    repo_root = find_repo_root(Path.cwd()) or Path.cwd()
    stooq_root = Path(args.stooq_root).expanduser() if args.stooq_root else None
    metastock_root = Path(args.metastock_root).expanduser() if args.metastock_root else None
    corpus_root = Path(args.corpus_root).expanduser() if args.corpus_root else None
    output_dir = Path(args.write_out).expanduser()

    config = InventoryConfig(
        repo_root=repo_root,
        stooq_root=stooq_root,
        metastock_root=metastock_root,
        corpus_root=corpus_root,
        output_dir=output_dir,
    )
    write_inventory(config)


if __name__ == "__main__":
    main()
