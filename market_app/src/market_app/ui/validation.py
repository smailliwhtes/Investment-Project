"""
Validation helpers for GUI inputs.

Provides validation functions for config paths, runs directories, and run IDs
to ensure proper input before launching commands from the UI.
"""

from __future__ import annotations

import os
from pathlib import Path


class ValidationError(Exception):
    """Raised when validation fails with a user-friendly message."""

    pass


def validate_config_path(config_path: str) -> Path:
    """
    Validate and resolve config file path.

    Args:
        config_path: The config file path to validate (can be relative or absolute)

    Returns:
        Absolute Path to the config file

    Raises:
        ValidationError: If config_path is empty or file doesn't exist
    """
    if not config_path or not config_path.strip():
        raise ValidationError("Config path cannot be empty.")

    path = Path(config_path.strip()).expanduser().resolve()

    if not path.exists():
        raise ValidationError(f"Config file does not exist: {path}")

    if not path.is_file():
        raise ValidationError(f"Config path is not a file: {path}")

    return path


def validate_runs_directory(runs_dir: str) -> Path:
    """
    Validate and resolve runs directory path, creating it if needed.

    Args:
        runs_dir: The runs directory path to validate (can be relative or absolute)

    Returns:
        Absolute Path to the runs directory

    Raises:
        ValidationError: If runs_dir is empty or cannot be created
    """
    if not runs_dir or not runs_dir.strip():
        raise ValidationError("Runs directory path cannot be empty.")

    path = Path(runs_dir.strip()).expanduser().resolve()

    # Create directory if it doesn't exist
    if not path.exists():
        try:
            path.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise ValidationError(f"Cannot create runs directory {path}: {exc}") from exc

    # Verify it's a directory
    if not path.is_dir():
        raise ValidationError(f"Runs path exists but is not a directory: {path}")

    return path


def validate_run_id(run_id: str) -> str:
    """
    Validate run ID to ensure it's safe for filesystem use.

    Args:
        run_id: The run ID string to validate

    Returns:
        The validated run ID (stripped of whitespace)

    Raises:
        ValidationError: If run_id is empty or contains path separators
    """
    if not run_id or not run_id.strip():
        raise ValidationError("Run ID cannot be empty.")

    validated_id = run_id.strip()

    # Check for path separators (both forward and back slashes)
    if os.sep in validated_id or "/" in validated_id or "\\" in validated_id:
        raise ValidationError(
            f"Run ID cannot contain path separators (/, \\). Got: {validated_id}"
        )

    return validated_id


__all__ = [
    "ValidationError",
    "validate_config_path",
    "validate_runs_directory",
    "validate_run_id",
]
