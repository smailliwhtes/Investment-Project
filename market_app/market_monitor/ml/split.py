from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class WalkForwardSplit:
    fold: int
    train_days: list[str]
    val_days: list[str]


def build_walk_forward_splits(days: Iterable[str], folds: int) -> list[WalkForwardSplit]:
    days_sorted = sorted(set(days))
    if folds < 1:
        raise ValueError("folds must be >= 1")
    if len(days_sorted) < folds + 1:
        raise ValueError("Not enough unique days to build walk-forward splits")

    total_days = len(days_sorted)
    boundaries = [int(np.floor(total_days * (idx + 1) / (folds + 1))) for idx in range(folds + 1)]
    boundaries[-1] = total_days

    splits: list[WalkForwardSplit] = []
    for fold_idx in range(folds):
        train_end = boundaries[fold_idx]
        val_end = boundaries[fold_idx + 1]
        train_days = days_sorted[:train_end]
        val_days = days_sorted[train_end:val_end]
        if not train_days or not val_days:
            raise ValueError("Walk-forward split produced empty train or validation window")
        splits.append(WalkForwardSplit(fold=fold_idx + 1, train_days=train_days, val_days=val_days))
    return splits


def filter_frame_by_days(df: pd.DataFrame, day_column: str, days: Iterable[str]) -> pd.DataFrame:
    day_set = set(days)
    return df[df[day_column].isin(day_set)].copy()
