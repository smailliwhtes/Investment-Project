from __future__ import annotations

from pathlib import Path

from market_monitor.policy_event_schema import PolicyEvent
from market_monitor.providers.nasdaq_daily import NasdaqDailyProvider, NasdaqDailySource
from market_monitor.scenario_engine import compute_policy_event_study


def test_compute_policy_event_study_generates_abnormal_return_rows(tmp_path: Path) -> None:
    fixtures_dir = Path(__file__).resolve().parent / "fixtures" / "ohlcv_daily"
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    provider = NasdaqDailyProvider(
        NasdaqDailySource(directory=fixtures_dir, cache_dir=cache_dir)
    )
    events = [
        PolicyEvent(
            event_id="evt_tariff_1",
            event_type="tariff",
            source="fixture",
            agency="ustr",
            event_date="2024-02-01",
            title="Tariff announcement",
            summary="Synthetic tariff event for deterministic testing.",
            sectors=("industrials",),
            tickers=("AAA", "BBB"),
            countries=("USA", "CHN"),
            severity=0.7,
        ),
        PolicyEvent(
            event_id="evt_tariff_2",
            event_type="tariff",
            source="fixture",
            agency="ustr",
            event_date="2024-03-01",
            title="Tariff follow-up",
            summary="Synthetic tariff follow-up event for deterministic testing.",
            sectors=("industrials",),
            tickers=("AAA", "BBB"),
            countries=("USA", "CHN"),
            severity=0.6,
        ),
    ]

    frame = compute_policy_event_study(
        events,
        symbols=["AAA", "BBB"],
        provider=provider,
        benchmark_symbol="SPY",
        horizons=[1, 5],
        estimation_lookback_days=20,
    )

    assert len(frame) == 8
    assert {"AAA", "BBB"} == set(frame["symbol"])
    assert {1, 5} == set(frame["horizon_days"])
    assert {
        "event_id",
        "event_type",
        "event_date",
        "symbol",
        "horizon_days",
        "asset_return",
        "benchmark_return",
        "cumulative_abnormal_return",
        "window_volatility",
        "abnormal_zscore",
        "event_volume_ratio",
    }.issubset(frame.columns)
