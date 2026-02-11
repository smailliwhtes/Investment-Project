from __future__ import annotations

import argparse
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from market_app.tools.build_universe_available import build_universe_available


def main() -> int:
    parser = argparse.ArgumentParser(description="Build universe_available from cached OHLCV symbols")
    parser.add_argument("--ohlcv-dir", required=True)
    parser.add_argument("--universe-in", required=True)
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args()

    available_path, missing_path = build_universe_available(
        ohlcv_dir=Path(args.ohlcv_dir).expanduser().resolve(),
        universe_in=Path(args.universe_in).expanduser().resolve(),
        out_dir=Path(args.out_dir).expanduser().resolve(),
    )
    print(f"[done] wrote {available_path}")
    print(f"[done] wrote {missing_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
