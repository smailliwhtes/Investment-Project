from __future__ import annotations

import argparse
import hashlib
import json
import socket
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from market_app.offline_guard import OfflineNetworkError, enforce_offline_network_block


NETWORK_LIB_HINTS = [
    "requests",
    "httpx",
    "urllib",
    "aiohttp",
    "websockets",
    "yfinance",
    "socket",
]

REQUIRED_RELATIVE_PATHS = [
    "README.md",
    "requirements.txt",
    "pyproject.toml",
    "scripts/run.ps1",
    "src/market_app/cli.py",
    "src/market_app/offline_pipeline.py",
    "src/market_app/features_local.py",
    "src/market_app/scoring_local.py",
    "src/market_app/reporting_local.py",
    "src/market_app/ohlcv_local.py",
    "src/market_app/manifest_local.py",
    "src/market_app/symbols_local.py",
    "src/market_app/offline_guard.py",
    "config/config.yaml",
]

OPTIONAL_RELATIVE_PATHS = [
    "scripts/provision_data.ps1",
    "tools/run_watchlist.py",
    "market_monitor/corpus",
    "data/text_corpus",
    "data/index",
]

RUNTIME_ARTIFACTS = [
    "universe.csv",
    "features.csv",
    "eligible.csv",
    "scored.csv",
]


@dataclass(frozen=True)
class SmokeResult:
    run_a: Path
    run_b: Path
    deterministic: bool
    digests_a: dict[str, str]
    digests_b: dict[str, str]


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _inventory(repo_root: Path) -> dict[str, Any]:
    files = sorted(
        [p.relative_to(repo_root).as_posix() for p in repo_root.rglob("*") if p.is_file() and ".git/" not in p.as_posix()]
    )
    required = []
    optional = []
    unknown = []

    required_set = set(REQUIRED_RELATIVE_PATHS)
    optional_set = set(OPTIONAL_RELATIVE_PATHS)

    for rel in REQUIRED_RELATIVE_PATHS:
        required.append({"path": rel, "exists": (repo_root / rel).exists()})
    for rel in OPTIONAL_RELATIVE_PATHS:
        optional.append({"path": rel, "exists": (repo_root / rel).exists()})

    for rel in files:
        if rel in required_set or rel in optional_set:
            continue
        unknown.append(rel)

    return {
        "required": required,
        "optional": optional,
        "unknown": unknown,
        "counts": {
            "required_present": sum(1 for x in required if x["exists"]),
            "required_total": len(required),
            "optional_present": sum(1 for x in optional if x["exists"]),
            "optional_total": len(optional),
            "unknown_total": len(unknown),
        },
    }


def _validate_config(repo_root: Path, config_path: Path) -> dict[str, Any]:
    required_keys = ["schema_version", "offline", "paths", "gates", "scoring"]
    required_path_keys = ["symbols_dir", "ohlcv_dir", "output_dir", "logging_config"]
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}

    key_checks = {k: (k in payload) for k in required_keys}
    path_checks = {}
    for key in required_path_keys:
        raw = payload.get("paths", {}).get(key, "")
        exists = False
        resolved = ""
        if raw:
            p = Path(raw)
            if not p.is_absolute():
                p = (config_path.parent / p).resolve()
            exists = p.exists()
            resolved = str(p)
        path_checks[key] = {"value": raw, "resolved": resolved, "exists": exists}

    issues: list[str] = []
    if not all(key_checks.values()):
        issues.append("missing required top-level config keys")
    if not path_checks["logging_config"]["exists"]:
        issues.append("logging_config path does not exist")
    out_dir = Path(path_checks["output_dir"]["resolved"]) if path_checks["output_dir"]["resolved"] else None
    if out_dir is not None and not out_dir.exists():
        out_dir.mkdir(parents=True, exist_ok=True)
        path_checks["output_dir"]["exists"] = True
    # symbols_dir and ohlcv_dir may be empty by design and supplied by env overrides.

    return {
        "config_path": str(config_path),
        "required_keys": key_checks,
        "path_checks": path_checks,
        "issues": issues,
        "valid": len(issues) == 0,
    }


def _static_network_scan(repo_root: Path) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for py in repo_root.rglob("*.py"):
        rel = py.relative_to(repo_root).as_posix()
        if ".git/" in rel or "/_backup" in rel:
            continue
        text = py.read_text(encoding="utf-8", errors="ignore")
        for idx, line in enumerate(text.splitlines(), start=1):
            low = line.lower()
            if any(f"import {hint}" in low or f"from {hint}" in low for hint in NETWORK_LIB_HINTS):
                findings.append({"file": rel, "line": idx, "text": line.strip()})
    return findings


def _runtime_offline_proof() -> dict[str, Any]:
    with enforce_offline_network_block(True):
        try:
            socket.create_connection(("example.com", 80), timeout=0.5)
        except OfflineNetworkError as exc:
            return {"blocked": True, "message": str(exc)}
    return {"blocked": False, "message": "offline guard failed to block socket"}


def _run_smoke(repo_root: Path, run_base: Path) -> SmokeResult:
    config = repo_root / "tests" / "data" / "mini_dataset" / "config.yaml"
    run_a = run_base / "audit_smoke_1"
    run_b = run_base / "audit_smoke_2"

    cmd_base = [
        sys.executable,
        "-m",
        "market_app.cli",
        "--config",
        str(config),
        "--offline",
    ]
    subprocess.check_call(cmd_base + ["--run-id", run_a.name], cwd=repo_root)
    subprocess.check_call(cmd_base + ["--run-id", run_b.name], cwd=repo_root)

    digests_a = {name: _sha256(run_a / name) for name in RUNTIME_ARTIFACTS}
    digests_b = {name: _sha256(run_b / name) for name in RUNTIME_ARTIFACTS}
    deterministic = digests_a == digests_b
    return SmokeResult(run_a=run_a, run_b=run_b, deterministic=deterministic, digests_a=digests_a, digests_b=digests_b)


def run_audit(repo_root: Path) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_dir = repo_root / "outputs" / "audit" / ts
    out_dir.mkdir(parents=True, exist_ok=True)

    inventory = _inventory(repo_root)
    (out_dir / "inventory.json").write_text(json.dumps(inventory, indent=2, sort_keys=True), encoding="utf-8")

    config_validation = _validate_config(repo_root, repo_root / "config" / "config.yaml")
    (out_dir / "config_validation.json").write_text(
        json.dumps(config_validation, indent=2, sort_keys=True), encoding="utf-8"
    )

    static_findings = _static_network_scan(repo_root)
    runtime_proof = _runtime_offline_proof()
    offline_md = [
        "# Offline Safety Report",
        "",
        "## Runtime enforcement proof",
        f"- Blocked network call in offline mode: `{runtime_proof['blocked']}`",
        f"- Message: `{runtime_proof['message']}`",
        "",
        "## Static network capability scan",
        f"- Findings: `{len(static_findings)}`",
        "",
    ]
    for item in static_findings[:100]:
        offline_md.append(f"- `{item['file']}:{item['line']}` — `{item['text']}`")
    (out_dir / "offline_safety_report.md").write_text("\n".join(offline_md) + "\n", encoding="utf-8")

    smoke = _run_smoke(repo_root, repo_root / "tests" / "data" / "mini_dataset" / "outputs" / "runs")
    smoke_md = [
        "# Pipeline Smoke Report",
        "",
        "## Runs",
        f"- Run A: `{smoke.run_a}`",
        f"- Run B: `{smoke.run_b}`",
        f"- Deterministic: `{smoke.deterministic}`",
        "",
        "## Stable output digests",
    ]
    for name in RUNTIME_ARTIFACTS:
        smoke_md.append(f"- `{name}`: `{smoke.digests_a[name]}`")
    (out_dir / "pipeline_smoke_report.md").write_text("\n".join(smoke_md) + "\n", encoding="utf-8")

    required_ok = all(x["exists"] for x in inventory["required"]) and config_validation["valid"]
    verdict = "READY" if (required_ok and runtime_proof["blocked"] and smoke.deterministic) else "NOT READY"
    readiness = [
        "# READINESS REPORT",
        "",
        f"**Verdict:** {verdict}",
        "",
        "## Evidence",
        f"- Inventory: `{out_dir / 'inventory.json'}`",
        f"- Config validation: `{out_dir / 'config_validation.json'}`",
        f"- Offline safety: `{out_dir / 'offline_safety_report.md'}`",
        f"- Smoke report: `{out_dir / 'pipeline_smoke_report.md'}`",
        "",
        "## Commands executed",
        f"- `python -m market_app.cli --config {repo_root / 'tests' / 'data' / 'mini_dataset' / 'config.yaml'} --offline --run-id audit_smoke_1`",
        f"- `python -m market_app.cli --config {repo_root / 'tests' / 'data' / 'mini_dataset' / 'config.yaml'} --offline --run-id audit_smoke_2`",
        "",
        "## Priority punch list",
    ]
    if verdict == "READY":
        readiness.append("- None.")
    else:
        readiness.extend(
            [
                "- P0: Fix failing required inventory/config checks.",
                "- P0: Ensure offline guard blocks all network attempts.",
                "- P1: Resolve deterministic output mismatches.",
                "- P2: Reduce optional warnings and clean legacy files.",
            ]
        )

    (out_dir / "READINESS_REPORT.md").write_text("\n".join(readiness) + "\n", encoding="utf-8")
    return out_dir


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit market_app for offline deterministic readiness")
    parser.add_argument("--repo-root", default=".")
    args = parser.parse_args(argv)

    repo_root = Path(args.repo_root).resolve()
    out = run_audit(repo_root)
    print(f"[audit] wrote reports to {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
