from __future__ import annotations

import json
from dataclasses import dataclass
from difflib import unified_diff
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from market_monitor.hash_utils import hash_text
from market_monitor.paths import find_repo_root

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

_SKIP_OUTPUTS_FOR_DIGEST = {"manifest.json", "digest.json"}
_VOLATILE_JSON_FIELDS = {
    "run_id",
    "run_dir",
    "run_directory",
    "generated_at",
    "timestamp",
    "timestamps",
    "start_timestamp_utc",
    "end_timestamp_utc",
    "run_timestamp",
    "run_timestamp_utc",
}
_VOLATILE_CSV_COLUMNS = {"run_id", "run_timestamp", "run_timestamp_utc"}
_VOLATILE_TEXT_KEYS = {"run_id", "run_timestamp", "run_timestamp_utc"}
_PATH_KEYS = {"path", "config_path", "watchlist_file", "watchlist_path", "run_dir", "run_dir_a", "run_dir_b"}


@dataclass(frozen=True)
class AllowlistConfig:
    global_allowlist: set[str]
    per_file: dict[str, set[str]]


@dataclass(frozen=True)
class AllowlistBundle:
    csv: AllowlistConfig
    json: AllowlistConfig
    text: AllowlistConfig


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


def _resolve_allowlist(allowed_vary: dict[str, Any] | None) -> AllowlistConfig:
    allowed_vary = allowed_vary or {}
    global_allow = set(allowed_vary.get("global", []))
    per_file = {}
    for key, value in allowed_vary.items():
        if key == "global":
            continue
        per_file[key] = set(value or [])
    return AllowlistConfig(global_allowlist=global_allow, per_file=per_file)


def resolve_allowlists(
    *,
    allowed_vary_columns: dict[str, Any] | None,
    allowed_vary_json_keys: dict[str, Any] | None,
) -> AllowlistBundle:
    return AllowlistBundle(
        csv=_resolve_allowlist(allowed_vary_columns),
        json=_resolve_allowlist(allowed_vary_json_keys),
        text=_resolve_allowlist({}),
    )


def stable_output_digests(
    run_dir: Path,
    allowlist: AllowlistBundle,
) -> dict[str, dict[str, Any]]:
    outputs: dict[str, dict[str, Any]] = {}
    for name in STABLE_ARTIFACTS:
        if name in _SKIP_OUTPUTS_FOR_DIGEST:
            continue
        path = run_dir / name
        if not path.exists():
            continue
        allow = _select_allowlist(path, allowlist)
        drop_keys = allow.global_allowlist | allow.per_file.get(name, set())
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
    allowlist: AllowlistBundle,
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
            "csv": {
                "global": sorted(allowlist.csv.global_allowlist),
                "per_file": {k: sorted(v) for k, v in allowlist.csv.per_file.items()},
            },
            "json": {
                "global": sorted(allowlist.json.global_allowlist),
                "per_file": {k: sorted(v) for k, v in allowlist.json.per_file.items()},
            },
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

        allow = _select_allowlist(path_a, allowlist)
        allowed_keys = allow.global_allowlist | allow.per_file.get(name, set())
        if path_a.suffix.lower() == ".csv":
            diff = _compare_csv(path_a, path_b, allowed_keys=allowed_keys, diff_dir=diff_dir)
        elif path_a.suffix.lower() == ".json":
            diff = _compare_json(path_a, path_b, allowed_keys=allowed_keys, diff_dir=diff_dir)
        else:
            diff = _compare_text(path_a, path_b, allowed_keys=allowed_keys, diff_dir=diff_dir)

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
    _write_diff_report_html(diff_dir / "diff_report.html", summary, disallowed)

    ok = not disallowed
    return DeterminismReport(ok=ok, diff_dir=diff_dir, summary=summary, disallowed=disallowed)


def _canonical_csv_bytes(path: Path, *, drop_keys: Iterable[str] | None = None) -> bytes:
    df = pd.read_csv(path)
    drop_keys = set(drop_keys or []) | _VOLATILE_CSV_COLUMNS
    if drop_keys:
        df = df.drop(columns=[col for col in drop_keys if col in df.columns], errors="ignore")
    df = _canonicalize_frame(df)
    content = df.to_csv(index=False, lineterminator="\n", float_format="%.6f", na_rep="")
    return content.encode("utf-8")


def _canonical_json_bytes(path: Path, *, drop_keys: Iterable[str] | None = None) -> bytes:
    payload = json.loads(path.read_text(encoding="utf-8"))
    drop_keys = set(drop_keys or [])
    repo_root = find_repo_root(path.parent)
    payload = _canonicalize_json_payload(payload, drop_keys=drop_keys, root=repo_root)
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return serialized.encode("utf-8")


def _canonical_text_bytes(path: Path, *, drop_keys: Iterable[str] | None = None) -> bytes:
    text = path.read_text(encoding="utf-8").replace("\r\n", "\n").replace("\r", "\n")
    drop_keys = set(drop_keys or []) | _VOLATILE_TEXT_KEYS
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
    df_a = df_a.drop(columns=[col for col in _VOLATILE_CSV_COLUMNS if col in df_a.columns], errors="ignore")
    df_b = df_b.drop(columns=[col for col in _VOLATILE_CSV_COLUMNS if col in df_b.columns], errors="ignore")
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


def _compare_json(
    path_a: Path, path_b: Path, *, allowed_keys: set[str], diff_dir: Path
) -> DiffFileResult:
    repo_root = find_repo_root(path_a.parent)
    payload_a = _canonicalize_json_payload(
        json.loads(path_a.read_text(encoding="utf-8")),
        drop_keys=_VOLATILE_JSON_FIELDS,
        root=repo_root,
    )
    payload_b = _canonicalize_json_payload(
        json.loads(path_b.read_text(encoding="utf-8")),
        drop_keys=_VOLATILE_JSON_FIELDS,
        root=repo_root,
    )
    flat_a = _flatten_json(payload_a)
    flat_b = _flatten_json(payload_b)
    diff_keys = sorted({key for key in flat_a.keys() | flat_b.keys() if flat_a.get(key) != flat_b.get(key)})
    disallowed = sorted([key for key in diff_keys if key not in allowed_keys])
    diff_path = _write_text_diff(path_a, path_b, diff_dir=diff_dir) if diff_keys else None
    status = "match" if not diff_keys else "diff"
    return DiffFileResult(
        status=status,
        diff_keys=diff_keys,
        disallowed_keys=disallowed,
        diff_rows=None,
        max_abs_delta=None,
        example_keys=diff_keys[:5],
        diff_examples_path=diff_path,
    )


def _compare_text(
    path_a: Path, path_b: Path, *, allowed_keys: set[str], diff_dir: Path
) -> DiffFileResult:
    text_a = _canonical_text_bytes(path_a, drop_keys=allowed_keys)
    text_b = _canonical_text_bytes(path_b, drop_keys=allowed_keys)
    diff_keys = []
    if text_a != text_b:
        diff_keys = ["__content__"]
    disallowed = sorted([key for key in diff_keys if key not in allowed_keys])
    diff_path = _write_text_diff(path_a, path_b, diff_dir=diff_dir) if diff_keys else None
    status = "match" if not diff_keys else "diff"
    return DiffFileResult(
        status=status,
        diff_keys=diff_keys,
        disallowed_keys=disallowed,
        diff_rows=None,
        max_abs_delta=None,
        example_keys=diff_keys,
        diff_examples_path=diff_path,
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


def _canonicalize_json_payload(
    payload: Any, *, drop_keys: Iterable[str], root: Path
) -> Any:
    drop_set = set(drop_keys) | _VOLATILE_JSON_FIELDS
    for key in drop_keys:
        _delete_json_path(payload, key)

    def normalize(value: Any, parent_key: str | None = None) -> Any:
        if isinstance(value, dict):
            normalized: dict[str, Any] = {}
            for key, item in value.items():
                if key in drop_set:
                    continue
                normalized[key] = normalize(item, key)
            return normalized
        if isinstance(value, list):
            return [normalize(item, parent_key) for item in value]
        if isinstance(value, str) and parent_key in _PATH_KEYS:
            return _normalize_path_value(value, root=root)
        return value

    return normalize(payload)


def _normalize_path_value(value: str, *, root: Path) -> str:
    if not value:
        return value
    normalized = value
    if _is_windows_absolute_path(value):
        parts = value.replace("\\", "/").split(":", 1)
        normalized = parts[-1].lstrip("/\\")
    elif Path(value).is_absolute():
        try:
            normalized = str(Path(value).relative_to(root))
        except ValueError:
            normalized = str(Path(value))
            normalized = str(Path(normalized).as_posix())
            normalized = str(Path(*Path(normalized).parts[1:]))
    normalized = normalized.replace("\\", "/")
    return normalized


def _is_windows_absolute_path(value: str) -> bool:
    if len(value) < 3:
        return False
    return value[1:3] in {":\\", ":/"}


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


def _write_text_diff(path_a: Path, path_b: Path, *, diff_dir: Path) -> str | None:
    try:
        text_a = path_a.read_text(encoding="utf-8").splitlines()
        text_b = path_b.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None
    diff_lines = list(
        unified_diff(
            text_a,
            text_b,
            fromfile=path_a.name,
            tofile=path_b.name,
            lineterm="",
        )
    )
    if not diff_lines:
        return None
    diff_path = diff_dir / f"diff_{path_a.name}.txt"
    diff_path.write_text("\n".join(diff_lines) + "\n", encoding="utf-8")
    return str(diff_path)


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


def _write_diff_report_html(path: Path, summary: dict[str, Any], disallowed: dict[str, list[str]]) -> None:
    status = "FAIL" if disallowed else "PASS"
    rows = []
    for name, info in summary.get("files", {}).items():
        diff_examples = info.get("diff_examples_path") or ""
        rows.append(
            f"<tr><td>{name}</td><td>{info.get('status')}</td>"
            f"<td>{', '.join(info.get('diff_keys') or [])}</td>"
            f"<td>{diff_examples}</td></tr>"
        )
    html = [
        "<!doctype html>",
        "<html><head><meta charset='utf-8'><title>Determinism Check Report</title></head>",
        "<body>",
        "<h1>Determinism Check Report</h1>",
        f"<p><strong>Result:</strong> {status}</p>",
        f"<p><strong>As-of date:</strong> {summary.get('as_of_date') or 'unset'}</p>",
        f"<p><strong>Run A:</strong> {summary.get('run_dir_a')}</p>",
        f"<p><strong>Run B:</strong> {summary.get('run_dir_b')}</p>",
        "<h2>Per-file Summary</h2>",
        "<table border='1' cellpadding='4' cellspacing='0'>",
        "<tr><th>File</th><th>Status</th><th>Diff Keys</th><th>Diff Artifacts</th></tr>",
        *rows,
        "</table>",
        "</body></html>",
    ]
    path.write_text("\n".join(html) + "\n", encoding="utf-8")


def _select_allowlist(path: Path, allowlist: AllowlistBundle) -> AllowlistConfig:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return allowlist.csv
    if suffix == ".json":
        return allowlist.json
    return allowlist.text
