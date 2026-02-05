# Release Packaging

## Windows PyInstaller build

From the `market_app` directory:

```powershell
.\scripts\build_exe.ps1
```

The build outputs `dist\market-monitor.exe`. You can run it against fixture data:

```powershell
.\dist\market-monitor.exe run --watchlist .\watchlists\watchlist_smoke.csv --asof 2025-01-31 --run-id smoke --ohlcv-daily-dir .\tests\fixtures\ohlcv_daily --exogenous-daily-dir .\tests\fixtures\exogenous\daily_features
```

Build artifacts are produced locally and are intentionally not committed to the repository.
