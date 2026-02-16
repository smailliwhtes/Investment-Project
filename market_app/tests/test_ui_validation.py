"""
Tests for UI validation helpers.

These tests validate the input validation logic without requiring
Tkinter GUI instantiation. All tests are hermetic and offline.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

# Import validation module directly by file path to avoid importing tkinter
_validation_module_path = Path(__file__).parents[1] / "src" / "market_app" / "ui" / "validation.py"
_spec = importlib.util.spec_from_file_location("market_app.ui.validation", _validation_module_path)
_validation_module = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["market_app.ui.validation"] = _validation_module
_spec.loader.exec_module(_validation_module)  # type: ignore[union-attr]

ValidationError = _validation_module.ValidationError
validate_config_path = _validation_module.validate_config_path
validate_runs_directory = _validation_module.validate_runs_directory
validate_run_id = _validation_module.validate_run_id


class TestValidateConfigPath:
    """Tests for config path validation."""

    def test_empty_string_raises_error(self) -> None:
        with pytest.raises(ValidationError, match="Config path cannot be empty"):
            validate_config_path("")

    def test_whitespace_only_raises_error(self) -> None:
        with pytest.raises(ValidationError, match="Config path cannot be empty"):
            validate_config_path("   ")

    def test_missing_file_raises_error(self, tmp_path: Path) -> None:
        missing_file = tmp_path / "missing.yaml"
        with pytest.raises(ValidationError, match="Config file does not exist"):
            validate_config_path(str(missing_file))

    def test_directory_instead_of_file_raises_error(self, tmp_path: Path) -> None:
        directory = tmp_path / "config_dir"
        directory.mkdir()
        with pytest.raises(ValidationError, match="Config path is not a file"):
            validate_config_path(str(directory))

    def test_valid_absolute_path(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.yaml"
        config_file.write_text("# Test config\n")
        result = validate_config_path(str(config_file))
        assert result == config_file.resolve()
        assert result.is_absolute()

    def test_valid_relative_path_resolved_to_absolute(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        # Create a config file in tmp_path
        config_file = tmp_path / "config.yaml"
        config_file.write_text("# Test config\n")
        
        # Change to tmp_path directory
        monkeypatch.chdir(tmp_path)
        
        # Use relative path
        result = validate_config_path("config.yaml")
        assert result.is_absolute()
        assert result == config_file.resolve()

    def test_strips_whitespace(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.yaml"
        config_file.write_text("# Test config\n")
        result = validate_config_path(f"  {config_file}  ")
        assert result == config_file.resolve()

    def test_expands_user_home(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        # Mock home directory
        fake_home = tmp_path / "home"
        fake_home.mkdir()
        config_file = fake_home / "config.yaml"
        config_file.write_text("# Test config\n")
        
        monkeypatch.setenv("HOME", str(fake_home))
        # Note: Path.expanduser() uses the HOME env var
        result = validate_config_path("~/config.yaml")
        assert result.is_absolute()
        assert result.exists()


class TestValidateRunsDirectory:
    """Tests for runs directory validation."""

    def test_empty_string_raises_error(self) -> None:
        with pytest.raises(ValidationError, match="Runs directory path cannot be empty"):
            validate_runs_directory("")

    def test_whitespace_only_raises_error(self) -> None:
        with pytest.raises(ValidationError, match="Runs directory path cannot be empty"):
            validate_runs_directory("   ")

    def test_creates_missing_directory(self, tmp_path: Path) -> None:
        new_dir = tmp_path / "new_runs_dir"
        assert not new_dir.exists()
        
        result = validate_runs_directory(str(new_dir))
        
        assert result == new_dir.resolve()
        assert result.exists()
        assert result.is_dir()
        assert result.is_absolute()

    def test_creates_nested_directory(self, tmp_path: Path) -> None:
        nested_dir = tmp_path / "level1" / "level2" / "runs"
        assert not nested_dir.exists()
        
        result = validate_runs_directory(str(nested_dir))
        
        assert result == nested_dir.resolve()
        assert result.exists()
        assert result.is_dir()

    def test_existing_directory_returns_absolute_path(self, tmp_path: Path) -> None:
        existing_dir = tmp_path / "existing_runs"
        existing_dir.mkdir()
        
        result = validate_runs_directory(str(existing_dir))
        
        assert result == existing_dir.resolve()
        assert result.is_absolute()

    def test_file_instead_of_directory_raises_error(self, tmp_path: Path) -> None:
        file_path = tmp_path / "runs.txt"
        file_path.write_text("not a directory")
        
        with pytest.raises(ValidationError, match="Runs path exists but is not a directory"):
            validate_runs_directory(str(file_path))

    def test_relative_path_resolved_to_absolute(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.chdir(tmp_path)
        
        result = validate_runs_directory("relative_runs")
        
        assert result.is_absolute()
        assert result == (tmp_path / "relative_runs").resolve()
        assert result.exists()

    def test_strips_whitespace(self, tmp_path: Path) -> None:
        runs_dir = tmp_path / "runs"
        runs_dir.mkdir()
        
        result = validate_runs_directory(f"  {runs_dir}  ")
        
        assert result == runs_dir.resolve()

    def test_expands_user_home(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        fake_home = tmp_path / "home"
        fake_home.mkdir()
        runs_dir = fake_home / "runs"
        
        monkeypatch.setenv("HOME", str(fake_home))
        
        result = validate_runs_directory("~/runs")
        
        assert result.is_absolute()
        assert result.exists()
        assert result == runs_dir.resolve()


class TestValidateRunId:
    """Tests for run ID validation."""

    def test_empty_string_raises_error(self) -> None:
        with pytest.raises(ValidationError, match="Run ID cannot be empty"):
            validate_run_id("")

    def test_whitespace_only_raises_error(self) -> None:
        with pytest.raises(ValidationError, match="Run ID cannot be empty"):
            validate_run_id("   ")

    def test_forward_slash_raises_error(self) -> None:
        with pytest.raises(ValidationError, match="Run ID cannot contain path separators"):
            validate_run_id("run/id")

    def test_backslash_raises_error(self) -> None:
        with pytest.raises(ValidationError, match="Run ID cannot contain path separators"):
            validate_run_id("run\\id")

    def test_multiple_separators_raises_error(self) -> None:
        with pytest.raises(ValidationError, match="Run ID cannot contain path separators"):
            validate_run_id("run/id\\test")

    def test_valid_simple_id(self) -> None:
        result = validate_run_id("my_run")
        assert result == "my_run"

    def test_valid_id_with_hyphens(self) -> None:
        result = validate_run_id("my-run-123")
        assert result == "my-run-123"

    def test_valid_id_with_underscores(self) -> None:
        result = validate_run_id("my_run_123")
        assert result == "my_run_123"

    def test_valid_id_with_dots(self) -> None:
        result = validate_run_id("run.2024.01.01")
        assert result == "run.2024.01.01"

    def test_strips_whitespace(self) -> None:
        result = validate_run_id("  my_run  ")
        assert result == "my_run"

    def test_valid_alphanumeric_id(self) -> None:
        result = validate_run_id("run123abc")
        assert result == "run123abc"

    def test_unicode_characters_allowed(self) -> None:
        # Unicode characters should be allowed as long as no path separators
        result = validate_run_id("run_α_β")
        assert result == "run_α_β"
