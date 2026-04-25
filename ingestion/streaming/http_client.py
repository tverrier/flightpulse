"""HTTP client wrapper enforcing the lessons of INCIDENTS.md § Incident 3.

Every outbound HTTP call MUST go through `make_request`. It enforces:
  * Explicit (connect_timeout, read_timeout) — never None, never 0.
  * Bounded retry with full jitter on transient errors only.
  * 4xx (except 429) raises immediately — no point retrying client errors.
  * Optional auth tuple, JSON helper.

A unit test in `tests/unit/test_http_client.py` asserts the wrapper raises
within 20s when pointed at a TCP blackhole.
"""

from __future__ import annotations

import logging
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from tenacity import (
    RetryError,
    Retrying,
    before_sleep_log,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)

LOGGER = logging.getLogger(__name__)

DEFAULT_CONNECT_TIMEOUT = 5.0
DEFAULT_READ_TIMEOUT = 15.0
DEFAULT_MAX_ATTEMPTS = 4
DEFAULT_BACKOFF_MAX_S = 30.0


class TransientHTTPError(Exception):
    """5xx or 429 — safe to retry."""


class FatalHTTPError(Exception):
    """4xx (non-429) — do not retry."""


_RETRYABLE = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,
    TransientHTTPError,
)


def _build_session(pool_connections: int = 10) -> requests.Session:
    sess = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=pool_connections,
        pool_maxsize=pool_connections,
        max_retries=0,  # we manage retries ourselves
    )
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    return sess


_SESSION = _build_session()


def make_request(
    method: str,
    url: str,
    *,
    auth: tuple[str, str] | None = None,
    params: dict[str, Any] | None = None,
    json_body: Any | None = None,
    headers: dict[str, str] | None = None,
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
    read_timeout: float = DEFAULT_READ_TIMEOUT,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    session: requests.Session | None = None,
) -> requests.Response:
    """Issue an HTTP request with explicit timeouts and bounded retry.

    Raises:
        FatalHTTPError on non-retryable 4xx.
        TransientHTTPError if every retry attempt failed.
    """
    if connect_timeout <= 0 or read_timeout <= 0:
        raise ValueError("timeouts must be positive — see Incident #3")

    sess = session or _SESSION

    def _attempt() -> requests.Response:
        resp = sess.request(
            method=method.upper(),
            url=url,
            auth=auth,
            params=params,
            json=json_body,
            headers=headers,
            timeout=(connect_timeout, read_timeout),
        )
        if resp.status_code >= 500 or resp.status_code == 429:
            raise TransientHTTPError(f"{resp.status_code} from {url}: {resp.text[:200]}")
        if 400 <= resp.status_code < 500:
            raise FatalHTTPError(f"{resp.status_code} from {url}: {resp.text[:200]}")
        return resp

    try:
        for attempt in Retrying(
            stop=stop_after_attempt(max_attempts),
            wait=wait_random_exponential(multiplier=1, max=DEFAULT_BACKOFF_MAX_S),
            retry=retry_if_exception_type(_RETRYABLE),
            reraise=True,
            before_sleep=before_sleep_log(LOGGER, logging.WARNING),
        ):
            with attempt:
                return _attempt()
    except RetryError as e:  # pragma: no cover — defensive
        raise TransientHTTPError(str(e)) from e
    raise TransientHTTPError(f"unreachable: {url}")
