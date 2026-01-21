import pandas as pd
import pytest

from market_monitor.evaluate import EvaluationSplit, _evaluate_split, enforce_no_lookahead


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
    first = _evaluate_split(panel, split, include_corpus=False, mode="both")
    second = _evaluate_split(panel, split, include_corpus=False, mode="both")
    assert first == second
