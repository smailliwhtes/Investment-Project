from pathlib import Path

import pandas as pd

from market_monitor.corpus.pipeline import aggregate_daily_features, load_events


def _fixture_path() -> Path:
    return Path(__file__).resolve().parent / "fixtures" / "corpus" / "gdelt_conflict_sample.csv"


def test_corpus_load_events_dedup_and_future_dates() -> None:
    events, infos = load_events([_fixture_path()])
    assert len(infos) == 1
    assert len(events) == 3
    assert events["Date"].min() == "2020-01-01"
    assert events["Date"].max() == "2020-01-02"


def test_corpus_daily_aggregation() -> None:
    events, _ = load_events([_fixture_path()])
    daily = aggregate_daily_features(events, rootcode_top_n=1, country_top_k=1)
    daily = daily.set_index("Date")
    assert daily.loc["2020-01-01", "conflict_event_count_total"] == 2
    assert daily.loc["2020-01-02", "conflict_event_count_total"] == 1
    assert daily.loc["2020-01-01", "conflict_event_count_rootcode_19"] == 2
    assert daily.loc["2020-01-02", "conflict_event_count_rootcode_other"] == 1
    assert daily.loc["2020-01-01", "goldstein_mean"] == -5.0
    assert daily.loc["2020-01-02", "tone_mean"] == -0.5


def test_corpus_missing_quadclass_graceful() -> None:
    events = pd.DataFrame(
        {
            "Date": ["2020-01-01", "2020-01-01"],
            "EventRootCode": ["19", "19"],
            "GoldsteinScale": [-1.0, -2.0],
        }
    )
    daily = aggregate_daily_features(events, rootcode_top_n=1, country_top_k=1)
    assert not any(col.startswith("conflict_event_count_quadclass_") for col in daily.columns)
