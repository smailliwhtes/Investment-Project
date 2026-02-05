from __future__ import annotations

import json
import subprocess
import sys
import zipfile
from pathlib import Path


def test_provision_imports_from_zip(tmp_path: Path) -> None:
    fixtures_dir = Path(__file__).resolve().parent / "fixtures" / "provision"
    ohlcv_zip = tmp_path / "ohlcv_raw.zip"
    exogenous_zip = tmp_path / "exogenous_raw.zip"

    def _build_zip(source_dir: Path, zip_path: Path) -> None:
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for path in sorted(source_dir.rglob("*")):
                if path.is_file():
                    archive.write(path, path.relative_to(source_dir))

    _build_zip(fixtures_dir / "ohlcv_raw", ohlcv_zip)
    _build_zip(fixtures_dir / "exogenous_raw", exogenous_zip)

    ohlcv_dest = tmp_path / "ohlcv_raw"
    exogenous_dest = tmp_path / "exogenous_raw"

    cmd_ohlcv = [
        sys.executable,
        "-m",
        "market_monitor",
        "provision",
        "import-ohlcv",
        "--src",
        str(ohlcv_zip),
        "--dest",
        str(ohlcv_dest),
    ]
    result_ohlcv = subprocess.run(cmd_ohlcv, check=False, capture_output=True, text=True)
    assert result_ohlcv.returncode == 0, result_ohlcv.stderr or result_ohlcv.stdout
    payload = json.loads(result_ohlcv.stdout)
    inventory_path = Path(payload["inventory_path"])
    assert inventory_path.exists()

    cmd_exogenous = [
        sys.executable,
        "-m",
        "market_monitor",
        "provision",
        "import-exogenous",
        "--src",
        str(exogenous_zip),
        "--dest",
        str(exogenous_dest),
    ]
    result_exog = subprocess.run(cmd_exogenous, check=False, capture_output=True, text=True)
    assert result_exog.returncode == 0, result_exog.stderr or result_exog.stdout
    payload_exog = json.loads(result_exog.stdout)
    inventory_path_exog = Path(payload_exog["inventory_path"])
    assert inventory_path_exog.exists()
