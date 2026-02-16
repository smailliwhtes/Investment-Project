import json
from pathlib import Path

from market_monitor import cli
from market_monitor.run_watchlist import ProgressEmitter


def test_validate_config_json_success(capsys):
    config_path = Path("config/config.yaml")
    exit_code = cli.main(["validate-config", "--config", str(config_path), "--format", "json"])
    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["valid"] is True


def test_progress_emitter_outputs_json(capsys):
    emitter = ProgressEmitter(enabled=True)
    emitter.emit("features", "start", "symbols=10")
    line = capsys.readouterr().out.strip()
    payload = json.loads(line)
    assert payload["stage"] == "features"
    assert payload["pct"] == 40
