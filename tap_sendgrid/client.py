"""HTTP client for tap-sendgrid.

Provides a ``Client`` class with authentication, retry/backoff, rate-limit
handling, and an optional mock-data mode for local testing.
"""
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional
from urllib.parse import urljoin

import backoff
import requests
from requests import Session
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import Timeout
from singer import get_logger, metrics

from tap_sendgrid.exceptions import (
    ERROR_CODE_EXCEPTION_MAPPING,
    SendgridError,
    SendgridRateLimitError,
    SendgridServerError,
)

LOGGER = get_logger()
DEFAULT_TIMEOUT = 300
DEFAULT_BASE_URL = "https://api.sendgrid.com"


def _parse_retry_after(headers: Mapping[str, str]) -> Optional[int]:
    """Extract and return the ``Retry-After`` header value as an integer, or None."""
    retry_after = headers.get("Retry-After")
    if not retry_after:
        return None
    try:
        return max(int(retry_after), 0)
    except ValueError:
        return None


def _raise_for_error(response: requests.Response) -> None:
    """Raise the appropriate ``SendgridError`` subclass for non-2xx responses."""
    if response.status_code in (200, 201, 202, 204):
        return

    exc = ERROR_CODE_EXCEPTION_MAPPING.get(response.status_code)
    if response.status_code >= 500:
        exc = SendgridServerError
    if not exc:
        exc = SendgridError

    response_text = response.text[:500]
    if response.status_code == 429:
        raise SendgridRateLimitError(response_text)

    raise exc(f"HTTP {response.status_code}: {response_text}")


class Client:
    """HTTP client with auth, retry, and optional mock mode."""

    def __init__(self, config: Mapping[str, Any]) -> None:
        self.config = config
        self.base_url = config.get("api_base_url", DEFAULT_BASE_URL)
        self.request_timeout = int(config.get("request_timeout", DEFAULT_TIMEOUT))
        self.api_key = config.get("api_key")
        self.use_mock_data = bool(config.get("use_mock_data", False))
        self.mock_data_path = config.get("mock_data_path")
        self._session = Session()

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self._session.close()

    def _headers(self, extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        """Build and return the request headers, merging any *extra* entries."""
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "tap-sendgrid",
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        if extra:
            headers.update(extra)
        return headers

    def _mock_response(self, stream_name: str) -> Dict[str, Any]:
        """Load and return mock response data for *stream_name* from disk."""
        if not self.mock_data_path:
            raise SendgridError("mock_data_path is required when use_mock_data=true")
        with open(f"{self.mock_data_path}/{stream_name}.json", "r", encoding="utf-8") as handle:
            return json.load(handle)

    @backoff.on_exception(
        backoff.expo,
        (SendgridRateLimitError, SendgridServerError, RequestsConnectionError, Timeout),
        max_tries=7,
        factor=2,
    )
    def request(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        stream_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Execute an HTTP request with retry/backoff and return the parsed JSON body."""
        if self.use_mock_data and stream_name:
            return self._mock_response(stream_name)

        url = endpoint if endpoint.startswith("http") else urljoin(self.base_url, endpoint)
        request_headers = self._headers(headers)

        with metrics.http_request_timer(url):
            response = self._session.request(
                method,
                url,
                params=params,
                json=body,
                headers=request_headers,
                timeout=self.request_timeout,
            )

        if response.status_code == 429:
            retry_after = _parse_retry_after(response.headers)
            if retry_after is not None and retry_after > 0:
                LOGGER.warning("Rate limited by SendGrid. Sleeping for %s seconds", retry_after)
                time.sleep(retry_after)
            raise SendgridRateLimitError("Rate limit reached")

        _raise_for_error(response)

        if response.status_code == 204:
            return {}
        return response.json()

    def get(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        stream_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Perform a GET request and return the parsed JSON response."""
        return self.request(
            method="GET",
            endpoint=endpoint,
            params=params,
            headers=headers,
            stream_name=stream_name,
        )


def to_unix_timestamp(value: str) -> int:
    """Convert an RFC-3339 / ISO-8601 datetime string to a UTC Unix timestamp integer."""
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return int(dt.replace(tzinfo=timezone.utc).timestamp())
