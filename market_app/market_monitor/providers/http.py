from __future__ import annotations

import random
import time
from collections.abc import Iterable
from dataclasses import dataclass

import requests


@dataclass(frozen=True)
class RetryConfig:
    max_retries: int
    base_delay_s: float
    jitter_s: float = 0.2
    retry_statuses: Iterable[int] = (429, 500, 502, 503, 504)


def request_with_backoff(
    url: str,
    *,
    session: requests.Session | None = None,
    retry: RetryConfig | None = None,
    timeout: float = 30,
    **kwargs,
) -> requests.Response:
    session = session or requests.Session()
    retry_cfg = retry or RetryConfig(max_retries=0, base_delay_s=0)
    attempt = 0

    while True:
        try:
            response = session.get(url, timeout=timeout, **kwargs)
            if response.status_code in retry_cfg.retry_statuses and attempt < retry_cfg.max_retries:
                _sleep_backoff(attempt, retry_cfg)
                attempt += 1
                continue
            return response
        except requests.RequestException:
            if attempt >= retry_cfg.max_retries:
                raise
            _sleep_backoff(attempt, retry_cfg)
            attempt += 1


def _sleep_backoff(attempt: int, retry_cfg: RetryConfig) -> None:
    delay = retry_cfg.base_delay_s * (2**attempt)
    delay += random.random() * retry_cfg.jitter_s
    time.sleep(delay)
