"""ML training and prediction entrypoints for joined market + GDELT features."""

from market_monitor.ml.dataset import DatasetInfo, build_dataset, load_prediction_frame


def train_main() -> int:
    from market_monitor.ml import train_xgb

    return train_xgb.main()


def predict_main() -> int:
    from market_monitor.ml import predict

    return predict.main()


def benchmark_main() -> int:
    from market_monitor.ml import benchmark

    return benchmark.main()


__all__ = [
    "DatasetInfo",
    "benchmark_main",
    "build_dataset",
    "load_prediction_frame",
    "predict_main",
    "train_main",
]
