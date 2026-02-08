from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from market_monitor.hash_utils import hash_text

STABLE_ARTIFACTS = (
    "eligible.csv",
    "scored.csv",
    "features.csv",
    "classified.csv",
    "universe.csv",
    "manifest.json",
    "digest.json",
    "report.md",
    "report.html",
)


@dataclass(frozen=True)
class AllowlistConfig:
    global_allowlist: set[str]
    per_file: dict[str, set[str]]


@dataclass(frozen=True)
class DiffFileResult:
    status: str
    diff_keys: list[str]
    disallowed_keys: list[str]
    diff_rows: int | None
    max_abs_delta: float | None
    example_keys: list[str]
    diff_examples_path: str | None


@dataclass(frozen=True)
class DeterminismReport:
    ok: bool
    diff_dir: Path | None
    summary: dict[str, Any]
    disallowed: dict[str, list[str]]


def resolve_allowlist(allowed_vary_columns: dict[str, Any] | None) -> AllowlistConfig:
    allowed_vary_columns = allowed_vary_columns or {}
    global_allow = set(allowed_vary_columns.get("global", []))
    per_file = {}
    for key, value in allowed_vary_columns.items():
        if key == "global":
            continue
        per_file[key] = set(value or [])
    return AllowlistConfig(global_allowlist=global_allow, per_file=per_file)


def stable_output_digests(
    run_dir: Path,
    allowlist: AllowlistConfig,
) -> dict[str, dict[str, Any]]:
    outputs: dict[str, dict[str, Any]] = {}
    for name in STABLE_ARTIFACTS:
        path = run_dir / name
        if not path.exists():
            continue
        drop_keys = allowlist.global_allowlist | allowlist.per_file.get(name, set())
        payload = canonical_bytes(path, drop_keys=drop_keys)
        outputs[name] = {"sha256": hash_text(payload.decode("utf-8")), "bytes": len(payload)}
    return outputs


def canonical_bytes(path: Path, *, drop_keys: Iterable[str] | None = None) -> bytes:
    if path.suffix.lower() == ".csv":
        return _canonical_csv_bytes(path, drop_keys=drop_keys)
    if path.suffix.lower() == ".json":
        return _canonical_json_bytes(path, drop_keys=drop_keys)
    return _canonical_text_bytes(path, drop_keys=drop_keys)


def compare_runs(
    run_dir_a: Path,
    run_dir_b: Path,
    *,
    allowlist: AllowlistConfig,
    diff_dir: Path,
    as_of_date: str | None,
) -> DeterminismReport:
    diff_dir.mkdir(parents=True, exist_ok=True)
    summary: dict[str, Any] = {
        "as_of_date": as_of_date,
        "run_dir_a": str(run_dir_a),
        "run_dir_b": str(run_dir_b),
        "files": {},
        "allowlist": {
            "global": sorted(allowlist.global_allowlist),
            "per_file": {k: sorted(v) for k, v in allowlist.per_file.items()},
        },
    }
    disallowed: dict[str, list[str]] = {}
    diffs: dict[str, DiffFileResult] = {}

    for name in STABLE_ARTIFACTS:
        path_a = run_dir_a / name
        path_b = run_dir_b / name
        if not path_a.exists() and not path_b.exists():
            diffs[name] = DiffFileResult(
                status="absent",
                diff_keys=[],
                disallowed_keys=[],
                diff_rows=None,
                max_abs_delta=None,
                example_keys=[],
                diff_examples_path=None,
            )
            continue
        if not path_a.exists() or not path_b.exists():
            diff = DiffFileResult(
                status="missing",
                diff_keys=[],
                disallowed_keys=["__missing__"],
                diff_rows=None,
                max_abs_delta=None,
                example_keys=[],
                diff_examples_path=None,
            )
            diffs[name] = diff
            disallowed[name] = diff.disallowed_keys
            continue

        allowed = allowlist.global_allowlist | allowlist.per_file.get(name, set())
        if path_a.suffix.lower() == ".csv":
            diff = _compare_csv(path_a, path_b, allowed_keys=allowed, diff_dir=diff_dir)
        elif path_a.suffix.lower() == ".json":
            diff = _compare_json(path_a, path_b, allowed_keys=allowed)
        else:
            diff = _compare_text(path_a, path_b, allowed_keys=allowed)

        diffs[name] = diff
        if diff.disallowed_keys:
            disallowed[name] = diff.disallowed_keys

    summary["files"] = {
        name: {
            "status": diff.status,
            "diff_keys": diff.diff_keys,
            "disallowed_keys": diff.disallowed_keys,
            "diff_rows": diff.diff_rows,
            "max_abs_delta": diff.max_abs_delta,
            "example_keys": diff.example_keys,
            "diff_examples_path": diff.diff_examples_path,
        }
        for name, diff in diffs.items()
    }

    summary_path = diff_dir / "diff_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    _write_diff_report(diff_dir / "diff_report.md", summary, disallowed)

    ok = not disallowed
    return DeterminismReport(ok=ok, diff_dir=diff_dir, summary=summary, disallowed=disallowed)


def _canonical_csv_bytes(path: Path, *, drop_keys: Iterable[str] | None = None) -> bytes:
    df = pd.read_csv(path)
    drop_keys = set(drop_keys or [])
    if drop_keys:
        df = df.drop(columns=[col for col in drop_keys if col in df.columns], errors="ignore")
    df = _canonicalize_frame(df)
    content = df.to_csv(index=False, lineterminator="\n", float_format="%.6f", na_rep="")
    return content.encode("utf-8")


def _canonical_json_bytes(path: Path, *, drop_keys: Iterable[str] | None = None) -> bytes:
    payload = json.loads(path.read_text(encoding="utf-8"))
    drop_keys = set(drop_keys or [])
    for key in drop_keys:
        _delete_json_path(payload, key)
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return serialized.encode("utf-8")


def _canonical_text_bytes(path: Path, *, drop_keys: Iterable[str] | None = None) -> bytes:
    text = path.read_text(encoding="utf-8").replace("\r\n", "\n").replace("\r", "\n")
    drop_keys = set(drop_keys or [])
    if drop_keys:
        lines = text.split("\n")
        filtered = []
        for line in lines:
            if "run_id" in drop_keys and line.strip().lower().startswith("run id:"):
                continue
            if "run_timestamp" in drop_keys and line.strip().lower().startswith("run timestamp:"):
                continue
            filtered.append(line)
        text = "\n".join(filtered)
    return text.encode("utf-8")


def _canonicalize_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.reindex(sorted(df.columns), axis=1)
    columns = list(df.columns)
    sort_keys = [key for key in ["symbol", "ticker", "id", "name"] if key in columns]
    if sort_keys:
        remainder = [col for col in sorted(columns) if col not in sort_keys]
        sort_keys = sort_keys + remainder
    else:
        sort_keys = sorted(columns)
    ordered = df.reindex(sorted(columns), axis=1)
    ordered = ordered.sort_values(sort_keys, kind="mergesort").reset_index(drop=True)
    return ordered


def _compare_csv(
    path_a: Path,
    path_b: Path,
    *,
    allowed_keys: set[str],
    diff_dir: Path,
) -> DiffFileResult:
    df_a = pd.read_csv(path_a)
    df_b = pd.read_csv(path_b)
    df_a = _canonicalize_frame(df_a)
    df_b = _canonicalize_frame(df_b)

    diff_keys = _diff_frame_columns(df_a, df_b)
    disallowed = sorted([key for key in diff_keys if key not in allowed_keys])
    diff_rows, max_abs_delta, example_keys, diff_examples_path = _frame_diff_examples(
        path_a.name,
        df_a,
        df_b,
        diff_keys,
        diff_dir=diff_dir,
    )
    status = "match" if not diff_keys else "diff"
    return DiffFileResult(
        status=status,
        diff_keys=diff_keys,
        disallowed_keys=disallowed,
        diff_rows=diff_rows,
        max_abs_delta=max_abs_delta,
        example_keys=example_keys,
        diff_examples_path=diff_examples_path,
    )


def _compare_json(path_a: Path, path_b: Path, *, allowed_keys: set[str]) -> DiffFileResult:
    payload_a = json.loads(path_a.read_text(encoding="utf-8"))
    payload_b = json.loads(path_b.read_text(encoding="utf-8"))
    flat_a = _flatten_json(payload_a)
    flat_b = _flatten_json(payload_b)
    diff_keys = sorted({key for key in flat_a.keys() | flat_b.keys() if flat_a.get(key) != flat_b.get(key)})
    disallowed = sorted([key for key in diff_keys if key not in allowed_keys])
    status = "match" if not diff_keys else "diff"
    return DiffFileResult(
        status=status,
        diff_keys=diff_keys,
        disallowed_keys=disallowed,
        diff_rows=None,
        max_abs_delta=None,
        example_keys=diff_keys[:5],
        diff_examples_path=None,
    )


def _compare_text(path_a: Path, path_b: Path, *, allowed_keys: set[str]) -> DiffFileResult:
    text_a = _canonical_text_bytes(path_a, drop_keys=allowed_keys)
    text_b = _canonical_text_bytes(path_b, drop_keys=allowed_keys)
    diff_keys = []
    if text_a != text_b:
        diff_keys = ["__content__"]
    disallowed = sorted([key for key in diff_keys if key not in allowed_keys])
    status = "match" if not diff_keys else "diff"
    return DiffFileResult(
        status=status,
        diff_keys=diff_keys,
        disallowed_keys=disallowed,
        diff_rows=None,
        max_abs_delta=None,
        example_keys=diff_keys,
        diff_examples_path=None,
    )


def _diff_frame_columns(df_a: pd.DataFrame, df_b: pd.DataFrame) -> list[str]:
    columns = sorted(set(df_a.columns) | set(df_b.columns))
    diff_keys: list[str] = []
    for col in columns:
        if col not in df_a.columns or col not in df_b.columns:
            diff_keys.append(col)
            continue
        if not _series_equal(df_a[col], df_b[col]):
            diff_keys.append(col)
    return diff_keys


def _series_equal(left: pd.Series, right: pd.Series) -> bool:
    sentinel = object()
    left_values = left.where(~left.isna(), sentinel)
    right_values = right.where(~right.isna(), sentinel)
    return left_values.equals(right_values)


def _frame_diff_examples(
    name: str,
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    diff_keys: list[str],
    *,
    diff_dir: Path,
) -> tuple[int | None, float | None, list[str], str | None]:
    if not diff_keys:
        return 0, None, [], None

    common_cols = [col for col in df_a.columns if col in df_b.columns]
    if not common_cols:
        return None, None, [], None

    key_cols = [col for col in ["symbol", "ticker", "id", "name"] if col in df_a.columns]
    if not key_cols:
        key_cols = ["row_index"]
        df_a = df_a.copy()
        df_b = df_b.copy()
        df_a["row_index"] = df_a.index
        df_b["row_index"] = df_b.index

    diff_mask = pd.Series(False, index=df_a.index)
    for col in diff_keys:
        if col not in df_a.columns or col not in df_b.columns:
            diff_mask = diff_mask | True
            continue
        left = df_a[col].where(~df_a[col].isna(), "__nan__")
        right = df_b[col].where(~df_b[col].isna(), "__nan__")
        diff_mask = diff_mask | (left != right)

    diff_rows = int(diff_mask.sum())
    max_abs_delta = _max_abs_delta(df_a, df_b, diff_keys)
    example_keys = df_a.loc[diff_mask, key_cols].head(5).astype(str).agg("|".join, axis=1).tolist()

    examples = []
    for col in diff_keys[:5]:
        if col not in df_a.columns or col not in df_b.columns:
            continue
        col_diff_mask = df_a[col].where(~df_a[col].isna(), "__nan__") != df_b[col].where(
            ~df_b[col].isna(), "__nan__"
        )
        sample = df_a.loc[col_diff_mask, key_cols].head(5)
        for idx, row in sample.iterrows():
            entry = {key: row[key] for key in key_cols}
            entry.update(
                {
                    "column": col,
                    "run_a": df_a.loc[idx, col],
                    "run_b": df_b.loc[idx, col],
                }
            )
            examples.append(entry)

    diff_examples_path = None
    if examples:
        diff_examples_path = str(diff_dir / f"diff_examples_{name}")
        pd.DataFrame(examples).to_csv(diff_examples_path, index=False, lineterminator="\n")

    return diff_rows, max_abs_delta, example_keys, diff_examples_path


def _max_abs_delta(df_a: pd.DataFrame, df_b: pd.DataFrame, diff_keys: list[str]) -> float | None:
    max_delta = None
    for col in diff_keys:
        if col not in df_a.columns or col not in df_b.columns:
            continue
        left = pd.to_numeric(df_a[col], errors="coerce")
        right = pd.to_numeric(df_b[col], errors="coerce")
        if left.isna().all() or right.isna().all():
            continue
        delta = (left - right).abs().max()
        if pd.isna(delta):
            continue
        max_delta = float(delta) if max_delta is None else max(max_delta, float(delta))
    return max_delta


def _flatten_json(payload: Any, prefix: str = "") -> dict[str, Any]:
    items: dict[str, Any] = {}
    if isinstance(payload, dict):
        for key, value in payload.items():
            new_prefix = f"{prefix}.{key}" if prefix else str(key)
            items.update(_flatten_json(value, new_prefix))
    elif isinstance(payload, list):
        for idx, value in enumerate(payload):
            new_prefix = f"{prefix}[{idx}]"
            items.update(_flatten_json(value, new_prefix))
    else:
        items[prefix] = payload
    return items


def _delete_json_path(payload: Any, path: str) -> None:
    if not path:
        return
    current = payload
    parts = path.replace("]", "").split(".")
    for part in parts[:-1]:
        if "[" in part:
            key, index = part.split("[", 1)
            if isinstance(current, dict):
                current = current.get(key)
            else:
                return
            if isinstance(current, list):
                try:
                    current = current[int(index)]
                except (ValueError, IndexError):
                    return
        else:
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return
        if current is None:
            return
    final = parts[-1]
    if "[" in final:
        key, index = final.split("[", 1)
        if isinstance(current, dict):
            current = current.get(key)
        if isinstance(current, list):
            try:
                del current[int(index)]
            except (ValueError, IndexError):
                return
        return
    if isinstance(current, dict):
        current.pop(final, None)


def _write_diff_report(path: Path, summary: dict[str, Any], disallowed: dict[str, list[str]]) -> None:
    lines = [
        "# Determinism Check Report",
        "",
        f"As-of date: {summary.get('as_of_date') or 'unset'}",
        f"Run A: {summary.get('run_dir_a')}",
        f"Run B: {summary.get('run_dir_b')}",
        "",
    ]
    if disallowed:
        lines.append("## Result: FAIL")
        lines.append("")
        lines.append("Disallowed diffs detected:")
        for name, keys in disallowed.items():
            lines.append(f"- {name}: {', '.join(keys)}")
    else:
        lines.append("## Result: PASS")
        lines.append("")
        lines.append("No disallowed diffs detected.")

    lines.append("")
    lines.append("## Per-file Summary")
    for name, info in summary.get("files", {}).items():
        lines.append(f"- {name}: {info.get('status')} (diff_keys={info.get('diff_keys')})")
        diff_examples = info.get("diff_examples_path")
        if diff_examples:
            lines.append(f"  - examples: {diff_examples}")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
