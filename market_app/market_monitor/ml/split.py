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


def build_walk_forward_splits(
    days: Iterable[str],
    folds: int,
    gap: int = 0,
) -> list[WalkForwardSplit]:
    days_sorted = sorted(set(days))
    if folds < 1:
        raise ValueError("folds must be >= 1")
    if gap < 0:
        raise ValueError("gap must be >= 0")
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
        val_start = train_end + gap
        val_days = days_sorted[val_start:val_end]
        if not train_days or not val_days:
            raise ValueError("Walk-forward split produced empty train or validation window")
        splits.append(WalkForwardSplit(fold=fold_idx + 1, train_days=train_days, val_days=val_days))
    return splits


def filter_frame_by_days(df: pd.DataFrame, day_column: str, days: Iterable[str]) -> pd.DataFrame:
    day_set = set(days)
    return df[df[day_column].isin(day_set)].copy()


def split_frame_for_walk_forward(
    df: pd.DataFrame,
    split: WalkForwardSplit,
    *,
    day_column: str,
    label_end_column: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    train_df = filter_frame_by_days(df, day_column, split.train_days)
    val_df = filter_frame_by_days(df, day_column, split.val_days)
    if train_df.empty or val_df.empty:
        return train_df, val_df

    val_start = pd.to_datetime(split.val_days[0], errors="coerce")
    if pd.isna(val_start):
        raise ValueError(f"Validation start day could not be parsed: {split.val_days[0]}")

    label_end = pd.to_datetime(train_df[label_end_column], errors="coerce")
    purged_train = train_df[label_end < val_start].copy()
    return purged_train, val_df
