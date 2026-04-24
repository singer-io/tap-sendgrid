import types
from unittest.mock import MagicMock, patch

import tap_sendgrid


def _parsed_args(discover=False, catalog=None):
    return types.SimpleNamespace(
        state={},
        config={"start_date": "2024-01-01T00:00:00Z"},
        discover=discover,
        catalog=catalog,
    )


def test_do_discover_calls_discover_and_dumps_json():
    fake_catalog = MagicMock()
    fake_catalog.to_dict.return_value = {"streams": []}
    with patch("tap_sendgrid.discover", return_value=fake_catalog):
        tap_sendgrid.do_discover()


@patch("tap_sendgrid.Client")
@patch("tap_sendgrid.singer.utils.parse_args")
@patch("tap_sendgrid.do_discover")
def test_main_discover_path(do_discover_mock, parse_args_mock, client_mock):
    parse_args_mock.return_value = _parsed_args(discover=True, catalog=None)
    client_mock.return_value.__enter__.return_value = MagicMock()

    tap_sendgrid.main()

    do_discover_mock.assert_called_once()


@patch("tap_sendgrid.Client")
@patch("tap_sendgrid.singer.utils.parse_args")
@patch("tap_sendgrid.sync")
def test_main_sync_path(sync_mock, parse_args_mock, client_mock):
    parse_args_mock.return_value = _parsed_args(discover=False, catalog=MagicMock())
    client_mock.return_value.__enter__.return_value = MagicMock()

    tap_sendgrid.main()

    sync_mock.assert_called_once()
