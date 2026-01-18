from pathlib import Path

import pandas as pd

from market_monitor.bulk.models import BulkSource
from market_monitor.bulk.planner import build_download_plan


def test_build_download_plan_symbol_tasks():
    sources = [
        BulkSource(
            name="stooq",
            base_url="https://stooq.pl/q/d/l",
            symbol_template="{symbol}.us",
        )
    ]
    symbols = pd.Series(["aapl", " msft "])
    tasks = build_download_plan(sources, symbols, Path("data/raw"))

    assert len(tasks) == 2
    assert tasks[0].source_name == "stooq"
    assert tasks[0].symbol == "AAPL"
    assert tasks[0].url.endswith("/AAPL.us.csv")
    assert tasks[0].destination == Path("data/raw/stooq/AAPL.csv")


def test_build_download_plan_archive_task():
    sources = [
        BulkSource(
            name="gov",
            base_url="https://example.gov/data",
            supports_bulk_archive=True,
            archive_path="archives/treasury.zip",
        )
    ]
    symbols = pd.Series(["AAPL"])
    tasks = build_download_plan(sources, symbols, Path("data/raw"), use_archives=True)

    assert len(tasks) == 1
    assert tasks[0].is_archive is True
    assert tasks[0].url == "https://example.gov/data/archives/treasury.zip"
    assert tasks[0].destination == Path("data/raw/gov/treasury.zip")
    assert tasks[0].kind == "archive"


def test_build_download_plan_static_task():
    sources = [
        BulkSource(
            name="treasury",
            base_url="https://example.gov",
            static_path="/rates/daily.csv",
        )
    ]
    symbols = pd.Series(["AAPL"])
    tasks = build_download_plan(sources, symbols, Path("data/raw"), use_archives=False)

    assert len(tasks) == 1
    assert tasks[0].kind == "static"
    assert tasks[0].url == "https://example.gov/rates/daily.csv"
    assert tasks[0].destination == Path("data/raw/treasury/daily.csv")
