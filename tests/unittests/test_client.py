from unittest.mock import patch

import pytest

from tap_sendgrid.client import Client, _parse_retry_after


def test_parse_retry_after_int():
    assert _parse_retry_after({"Retry-After": "5"}) == 5


def test_parse_retry_after_invalid():
    assert _parse_retry_after({"Retry-After": "abc"}) is None


@patch("tap_sendgrid.client.Session.request")
def test_client_get_success(mock_request):
    mock_request.return_value.status_code = 200
    mock_request.return_value.json.return_value = {"result": []}

    client = Client({"api_key": "abc", "start_date": "2024-01-01T00:00:00Z"})
    result = client.get("/v3/marketing/lists")

    assert result == {"result": []}


@patch("tap_sendgrid.client.Session.request")
def test_client_rate_limit_retry_after(mock_request):
    mock_request.return_value.status_code = 429
    mock_request.return_value.headers = {"Retry-After": "0"}
    mock_request.return_value.text = "rate limit"

    client = Client({"api_key": "abc", "start_date": "2024-01-01T00:00:00Z"})

    with pytest.raises(Exception):
        client.get("/v3/marketing/lists")
