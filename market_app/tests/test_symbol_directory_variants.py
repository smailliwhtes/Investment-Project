from __future__ import annotations

from pathlib import Path
import logging

from market_app.symbols_local import load_symbols


def test_symbol_directory_variants(tmp_path: Path) -> None:
    symbols_dir = tmp_path / "symbols"
    symbols_dir.mkdir()
    nasdaq_standard = "\n".join(
        [
            "Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares",
            "AAA|Alpha Corp|Q|N|N|100|N|N",
            "BBB|Bravo ETF|Q|N|N|100|Y|N",
            "File Creation Time: 20250101",
        ]
    )
    nasdaq_enhanced = "\n".join(
        [
            "Symbol|Security Name|CQS Symbol|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares|Country|Leveraged|Inverse|Leverage Ratio",
            "CCC|Charlie Leveraged|CCC|Q|N|N|100|N|N|USA|Y|N|2",
            "DDD|Delta Inverse|DDD|Q|N|N|100|N|N|USA|N|Y|-2",
            "File Creation Time: 20250102",
        ]
    )
    otherlisted = "\n".join(
        [
            "ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Test Issue",
            "EEE|Echo Corp|N|EEE|N|N",
            "FFF|Foxtrot ETF|P|FFF|Y|N",
            "File Creation Time: 20250103",
        ]
    )
    (symbols_dir / "nasdaqlisted.txt").write_text(nasdaq_standard, encoding="utf-8")
    (symbols_dir / "nasdaqlisted_enhanced.txt").write_text(nasdaq_enhanced, encoding="utf-8")
    (symbols_dir / "otherlisted.txt").write_text(otherlisted, encoding="utf-8")

    result = load_symbols(symbols_dir, {"filters": {}}, logging.getLogger("test_symbols_variants"))

    assert set(result.symbols["symbol"]) == {"AAA", "BBB", "CCC", "DDD", "EEE", "FFF"}
    leveraged = result.symbols.set_index("symbol").loc["CCC", "is_leveraged"]
    inverse = result.symbols.set_index("symbol").loc["DDD", "is_inverse"]
    assert leveraged
    assert inverse
