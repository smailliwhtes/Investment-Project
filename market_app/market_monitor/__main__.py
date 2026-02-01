from market_monitor.env_doctor import ensure_supported_python

def main() -> int:
    status = ensure_supported_python()
    if status is not None:
        return status
    from market_monitor.cli import main as cli_main

    return cli_main()


if __name__ == "__main__":
    raise SystemExit(main())
