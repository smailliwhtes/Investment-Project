import json
import logging
import sys
from dataclasses import dataclass
from market_monitor.timebase import utcnow
from pathlib import Path
from typing import Any


class JsonlLogger:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def log(self, event_type: str, payload: dict[str, Any]) -> None:
        record = {
            "ts": utcnow().isoformat(),
            "event": event_type,
            **payload,
        }
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record) + "\n")


@dataclass(frozen=True)
class LogPaths:
    console_level: str
    file_path: Path | None = None


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": utcnow().isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
        }
        if record.args and isinstance(record.args, dict):
            payload["data"] = record.args
        return json.dumps(payload, ensure_ascii=False)


def configure_logging(log_paths: LogPaths) -> logging.Logger:
    logger = logging.getLogger("market_monitor")
    logger.setLevel(log_paths.console_level)
    logger.handlers.clear()
    logger.propagate = False

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_paths.console_level)
    console_formatter = logging.Formatter("[%(levelname)s] %(message)s")
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    if log_paths.file_path:
        log_paths.file_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_paths.file_path, encoding="utf-8")
        file_handler.setLevel(log_paths.console_level)
        file_handler.setFormatter(JsonFormatter())
        logger.addHandler(file_handler)

    return logger


def get_console_logger(level: str) -> logging.Logger:
    logger = logging.getLogger("market_monitor")
    if logger.handlers:
        return logger
    logger.setLevel(level)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("[%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger
