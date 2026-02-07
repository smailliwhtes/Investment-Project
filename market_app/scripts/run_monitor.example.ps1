# Example usage for the day-to-day wrapper

# Run with the default config.yaml in repo root
.\scripts\run_monitor.ps1 -Offline

# Run with an explicit config path and output directory
.\scripts\run_monitor.ps1 -Config .\config.yaml -RunsDir .\runs -Offline -Verbose

# Run and open the latest report automatically
.\scripts\run_monitor.ps1 -Config .\configs\acceptance.yaml -Offline -OpenReport
