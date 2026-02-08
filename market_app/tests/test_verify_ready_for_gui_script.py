from __future__ import annotations

from pathlib import Path


def test_verify_ready_for_gui_invokes_determinism_check() -> None:
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "verify_ready_for_gui.ps1"
    content = script_path.read_text(encoding="utf-8")
    assert "determinism-check" in content
    assert "market_app.cli" in content
    assert "doctor" not in content
