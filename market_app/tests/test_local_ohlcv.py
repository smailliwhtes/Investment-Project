from __future__ import annotations

from pathlib import Path

from market_app.ohlcv_local import load_ohlcv


def test_ohlcv_loader_missing_volume(tmp_path: Path) -> None:
    ohlcv_dir = tmp_path / "ohlcv"
    ohlcv_dir.mkdir()
    content = "\n".join(
        [
            "date,open,high,low,close",
            "2024-01-02,10,11,9,10.5",
            "2024-01-03,10.5,12,10,11.5",
        ]
    )
    (ohlcv_dir / "TEST.csv").write_text(content, encoding="utf-8")
    result = load_ohlcv("TEST", ohlcv_dir)
    assert result.missing_volume is True
    assert "volume" in result.frame.columns


def test_ohlcv_loader_duplicate_dates(tmp_path: Path) -> None:
    ohlcv_dir = tmp_path / "ohlcv"
    ohlcv_dir.mkdir()
    content = "\n".join(
        [
            "date,open,high,low,close,volume",
            "2024-01-02,10,11,9,10.5,100",
            "2024-01-02,10.5,12,10,11.5,120",
            "2024-01-03,11.5,13,11,12.5,130",
        ]
    )
    (ohlcv_dir / "DUP.csv").write_text(content, encoding="utf-8")
    result = load_ohlcv("DUP", ohlcv_dir)
    assert result.duplicate_dates == 1


def test_ohlcv_loader_malformed_dates(tmp_path: Path) -> None:
    ohlcv_dir = tmp_path / "ohlcv"
    ohlcv_dir.mkdir()
    content = "\n".join(
        [
            "date,open,high,low,close,volume",
            "bad-date,10,11,9,10.5,100",
            "2024-01-03,11.5,13,11,12.5,130",
        ]
    )
    (ohlcv_dir / "BAD.csv").write_text(content, encoding="utf-8")
    result = load_ohlcv("BAD", ohlcv_dir)
    assert result.missing_date_rows == 1
