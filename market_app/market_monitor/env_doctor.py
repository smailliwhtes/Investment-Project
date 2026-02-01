from __future__ import annotations

import argparse
import importlib
import platform
import sys
import sysconfig
from dataclasses import dataclass
from typing import Callable

SUPPORTED_MIN = (3, 10)
SUPPORTED_MAX = (3, 14)


@dataclass(frozen=True)
class SupportStatus:
    ok: bool
    issues: list[str]
    hints: list[str]


@dataclass(frozen=True)
class ModuleStatus:
    name: str
    ok: bool
    error: str | None
    hints: list[str]


def _format_version(version_info: tuple[int, int, int]) -> str:
    return f"{version_info[0]}.{version_info[1]}.{version_info[2]}"


def _gil_disabled() -> bool:
    try:
        return bool(sysconfig.get_config_var("Py_GIL_DISABLED"))
    except Exception:
        return False


def check_python_support(
    *,
    version_info: tuple[int, int, int] | None = None,
    gil_disabled: bool | None = None,
) -> SupportStatus:
    if version_info is None:
        version_info = (sys.version_info.major, sys.version_info.minor, sys.version_info.micro)
    if gil_disabled is None:
        gil_disabled = _gil_disabled()

    issues: list[str] = []
    hints: list[str] = []

    if version_info < SUPPORTED_MIN:
        issues.append(
            f"Python {_format_version(version_info)} is too old; requires >= "
            f"{SUPPORTED_MIN[0]}.{SUPPORTED_MIN[1]}."
        )
    if version_info >= SUPPORTED_MAX:
        issues.append(
            f"Python {_format_version(version_info)} is too new; NumPy wheels target < "
            f"{SUPPORTED_MAX[0]}.{SUPPORTED_MAX[1]}."
        )
    if gil_disabled:
        issues.append("Free-threaded (no-GIL) Python builds are not supported for this stack.")

    if issues:
        hints.extend(
            [
                "Install Python 3.12 or 3.13 and recreate the virtual environment.",
                "Reinstall with wheels-only to avoid source builds: pip install --only-binary=:all: -e \".[dev]\"",
                "On Windows, prefer: py -3.13 -m venv .venv",
            ]
        )

    return SupportStatus(ok=not issues, issues=issues, hints=hints)


def _diagnose_numpy_error(error_text: str) -> list[str]:
    error_lower = error_text.lower()
    hints = []
    if "_multiarray_umath" in error_lower:
        hints.append("Detected missing numpy._core._multiarray_umath (wheel mismatch).")
    if "importing numpy c-extensions failed" in error_lower or "multiarray failed to import" in error_lower:
        hints.append("NumPy C-extension import failed (wheel mismatch or broken install).")
    if "import numpy from its source directory" in error_lower:
        hints.append("NumPy thinks you are importing from its source tree; use a clean venv.")
    return hints


def _diagnose_import_error(module: str, error_text: str) -> list[str]:
    hints: list[str] = []
    error_lower = error_text.lower()
    if module == "numpy" or "numpy" in error_lower:
        hints.extend(_diagnose_numpy_error(error_text))
        hints.extend(
            [
                "Delete .venv and recreate it with Python 3.12/3.13.",
                "Reinstall using wheels-only: pip install --only-binary=:all: -e \".[dev]\"",
            ]
        )
    if module == "pandas" and ("numpy" in error_lower or "c-extension" in error_lower):
        hints.append("Pandas depends on NumPy; fix NumPy first, then reinstall pandas.")
    if module == "pandas" and not hints:
        hints.append("Reinstall pandas with wheels-only: pip install --only-binary=:all: pandas==2.2.3")
    return hints


def check_module_import(
    module: str, *, import_func: Callable[[str], object] | None = None
) -> ModuleStatus:
    importer = import_func or importlib.import_module
    try:
        importer(module)
        return ModuleStatus(name=module, ok=True, error=None, hints=[])
    except Exception as exc:
        error_text = f"{type(exc).__name__}: {exc}".strip()
        hints = _diagnose_import_error(module, error_text)
        return ModuleStatus(name=module, ok=False, error=error_text, hints=hints)


def _print_header() -> None:
    print("[env] python executable:", sys.executable)
    print("[env] python version:", sys.version.splitlines()[0])
    print("[env] platform:", platform.platform())
    print("[env] architecture:", platform.machine())


def _print_support_status(status: SupportStatus) -> None:
    if status.ok:
        print("[env] python support: OK")
        return
    print("[env] python support: ERROR")
    for issue in status.issues:
        print(f"  - {issue}")
    for hint in status.hints:
        print(f"  * {hint}")


def _print_module_status(status: ModuleStatus) -> None:
    if status.ok:
        print(f"[env] {status.name}: OK")
        return
    print(f"[env] {status.name}: ERROR")
    if status.error:
        print(f"  - {status.error}")
    for hint in status.hints:
        print(f"  * {hint}")


def run(self_test: bool) -> int:
    _print_header()
    support_status = check_python_support()
    _print_support_status(support_status)

    module_statuses = [check_module_import("numpy"), check_module_import("pandas")]
    for status in module_statuses:
        _print_module_status(status)

    errors = [status for status in module_statuses if not status.ok]
    if not support_status.ok:
        errors.append(ModuleStatus("python", False, None, []))

    if self_test and errors:
        return 2
    return 0


def ensure_supported_python() -> int | None:
    status = check_python_support()
    if status.ok:
        return None
    _print_header()
    _print_support_status(status)
    return 2


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Market Monitor environment doctor.")
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Exit nonzero if numpy/pandas cannot import or Python is unsupported.",
    )
    args = parser.parse_args(argv)
    return run(self_test=args.self_test)


if __name__ == "__main__":
    raise SystemExit(main())
