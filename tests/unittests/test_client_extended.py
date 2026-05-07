from unittest.mock import patch

import pytest

from tap_sendgrid.client import Client, _raise_for_error, to_unix_timestamp
from tap_sendgrid.exceptions import SendgridBadRequestError, SendgridServerError


class DummyResponse:
    def __init__(self, status_code, body=None, headers=None):
        self.status_code = status_code
        self._body = body or {}
        self.headers = headers or {}
        self.text = "body"

    def json(self):
        return self._body


def test_raise_for_error_bad_request_and_server_error():
    with pytest.raises(SendgridBadRequestError):
        _raise_for_error(DummyResponse(400, {"error": "bad"}))

    with pytest.raises(SendgridServerError):
        _raise_for_error(DummyResponse(500, {"error": "server"}))


def test_client_headers_enter_exit_and_unix_conversion():
    client = Client({"api_key": "abc", "start_date": "2024-01-01T00:00:00Z"})
    headers = client._headers()  # pylint: disable=protected-access
    assert headers["Authorization"] == "Bearer abc"

    entered = client.__enter__()
    assert entered is client
    with patch.object(client._session, "close") as close_mock:  # pylint: disable=protected-access
        client.__exit__(None, None, None)
        close_mock.assert_called_once()

    assert isinstance(to_unix_timestamp("2024-01-01T00:00:00Z"), int)


@patch("tap_sendgrid.client.Session.request")
def test_client_request_204_and_mock_path(mock_request):
    mock_request.return_value = DummyResponse(204, {})
    client = Client({"api_key": "abc", "start_date": "2024-01-01T00:00:00Z"})
    assert client.get("/v3/path") == {}

    mock_client = Client(
        {
            "start_date": "2024-01-01T00:00:00Z",
            "use_mock_data": True,
            "mock_data_path": "/tmp",
        }
    )

    with patch.object(mock_client, "_mock_response", return_value={"ok": True}):  # pylint: disable=protected-access
        assert mock_client.get("/v3/path", stream_name="blocks") == {"ok": True}
