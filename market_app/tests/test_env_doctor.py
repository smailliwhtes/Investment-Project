from __future__ import annotations

from market_monitor.env_doctor import check_module_import, check_python_support


def test_check_python_support_rejects_too_new() -> None:
    status = check_python_support(version_info=(3, 14, 0), gil_disabled=False)
    assert not status.ok
    assert any("too new" in issue for issue in status.issues)


def test_check_python_support_rejects_free_threaded() -> None:
    status = check_python_support(version_info=(3, 13, 0), gil_disabled=True)
    assert not status.ok
    assert any("Free-threaded" in issue for issue in status.issues)


def test_check_module_import_detects_numpy_multiarray() -> None:
    def _fake_import(_name: str) -> object:
        raise ImportError("No module named 'numpy._core._multiarray_umath'")

    status = check_module_import("numpy", import_func=_fake_import)
    assert not status.ok
    assert any("_multiarray_umath" in hint for hint in status.hints)
