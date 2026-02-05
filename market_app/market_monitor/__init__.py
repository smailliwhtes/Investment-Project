"""Market Monitor package."""

from market_monitor.version import __version__

__all__ = ["main", "__version__"]


def main(argv=None):
    from .cli import main as _main
    return _main(argv)
