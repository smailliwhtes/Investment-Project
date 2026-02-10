from __future__ import annotations

from pathlib import Path
import logging

from market_app.symbols_local import load_symbols


def test_symbol_loader_filters_and_headers(tmp_path: Path) -> None:
    symbols_dir = tmp_path / "symbols"
    symbols_dir.mkdir()
    pipe_text = "\n".join(
        [
            "Symbol|Security Name|Exchange|ETF|Test Issue",
            "GOOD|Good Company|NASDAQ|N|N",
            "UNIT|Unit Holdings Unit|NYSE|N|N",
            "File Creation Time: 20250101",
        ]
    )
    (symbols_dir / "nasdaqlisted.txt").write_text(pipe_text, encoding="utf-8")
    csv_text = "Ticker,Company Name,Exchange\nALT,Alternate Corp,NYSE\n"
    (symbols_dir / "other.csv").write_text(csv_text, encoding="utf-8")

    config = {"filters": {"include_units": False}}
    logger = logging.getLogger("test_symbols")
    result = load_symbols(symbols_dir, config, logger)

    assert "UNIT" not in result.symbols["symbol"].tolist()
    assert set(result.symbols["symbol"]) == {"GOOD", "ALT"}


def test_symbol_loader_accepts_csv_variants(tmp_path: Path) -> None:
    symbols_dir = tmp_path / "symbols"
    symbols_dir.mkdir()
    csv_text = "symbol,security name,exchange\nXYZ,Example Security,NASDAQ\n"
    (symbols_dir / "list.csv").write_text(csv_text, encoding="utf-8")

    config = {"filters": {}}
    logger = logging.getLogger("test_symbols_csv")
    result = load_symbols(symbols_dir, config, logger)
    assert result.symbols.loc[0, "symbol"] == "XYZ"
