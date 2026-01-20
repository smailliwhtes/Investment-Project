from pathlib import Path

import pandas as pd

from market_monitor.io import write_csv


def test_write_csv_is_deterministic(tmp_path: Path) -> None:
    df = pd.DataFrame({"b": [0.12345678], "a": [1.98765432]})
    columns = ["a", "b"]
    path = tmp_path / "out.csv"
    write_csv(df, path, columns)
    content_first = path.read_text(encoding="utf-8")

    write_csv(df, path, columns)
    content_second = path.read_text(encoding="utf-8")

    assert content_first == content_second
    assert content_first.splitlines()[0] == "a,b"
    assert "1.987654" in content_first
    assert "0.123457" in content_first
