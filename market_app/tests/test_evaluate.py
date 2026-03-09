import pandas as pd
import pytest

from market_monitor.evaluate import EvaluationSplit, _build_market_panel, _evaluate_split, _sanitize_symbols, enforce_no_lookahead


def test_enforce_no_lookahead_flags_future_corpus() -> None:
    panel = pd.DataFrame(
        {
            "Date": pd.to_datetime(["2020-01-01", "2020-01-02"]),
            "corpus_date": pd.to_datetime(["2020-01-02", "2020-01-02"]),
            "forward_return": [0.01, -0.02],
            "label": [1, 0],
            "feature_a": [0.1, 0.2],
        }
    )
    with pytest.raises(ValueError, match="Future leakage detected"):
        enforce_no_lookahead(panel, cutoff=pd.Timestamp("2020-01-02"))


def test_evaluation_deterministic_metrics() -> None:
    panel = pd.DataFrame(
        {
            "Date": pd.to_datetime(
                [
                    "2020-01-01",
                    "2020-01-02",
                    "2020-01-03",
                    "2020-01-04",
                    "2020-01-05",
                    "2020-01-06",
                ]
            ),
            "forward_return": [0.01, 0.02, -0.01, 0.03, -0.02, 0.01],
            "label": [1, 1, 0, 1, 0, 1],
            "feature_a": [0.1, 0.2, 0.05, 0.3, 0.0, 0.15],
            "feature_b": [1.0, 0.9, 1.1, 0.8, 1.2, 0.95],
        }
    )
    split = EvaluationSplit(
        name="fold_1",
        train_end=pd.Timestamp("2020-01-04"),
        test_start=pd.Timestamp("2020-01-05"),
        test_end=pd.Timestamp("2020-01-06"),
    )
    first = _evaluate_split(panel, split, include_corpus=False, mode="both", risk_free_rate_annual=0.0)
    second = _evaluate_split(panel, split, include_corpus=False, mode="both", risk_free_rate_annual=0.0)
    assert first == second



def test_sanitize_symbols_handles_headers_and_comma_rows() -> None:
    symbols = _sanitize_symbols([
        "\ufeffSYMBOL",
        "A,,equity",
        " aapl ",
        "BRK.B",
        "",
        None,
        "A,,EQUITY",
    ])

    assert symbols == ["A", "AAPL", "BRK.B"]



def test_build_market_panel_respects_max_samples_per_symbol() -> None:
    history = pd.DataFrame(
        {
            "Date": pd.date_range("2024-01-01", periods=20, freq="D"),
            "Open": [100 + i for i in range(20)],
            "High": [101 + i for i in range(20)],
            "Low": [99 + i for i in range(20)],
            "Close": [100 + i for i in range(20)],
            "Volume": [1_000_000 for _ in range(20)],
        }
    )

    class _Provider:
        def get_history(self, symbol: str, days: int) -> pd.DataFrame:  # noqa: ARG002
            return history

    class _Logger:
        def warning(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
            return

        def info(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
            return

    panel = _build_market_panel(
        ["AAA"],
        _Provider(),
        lookback_days=5,
        forward_return_days=1,
        min_history_days=5,
        max_samples_per_symbol=4,
        logger=_Logger(),
    )

    assert len(panel) == 4
    assert panel["symbol"].nunique() == 1
