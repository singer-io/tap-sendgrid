from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from tap_sendgrid.streams.abstracts import (
    BaseStream,
    CursorPagedStream,
    FullTableStream,
    IncrementalStream,
    OffsetPagedStream,
)


class FakeCatalog:
    schema = SimpleNamespace(
        to_dict=lambda: {
            "type": "object",
            "properties": {"updated_at": {"type": ["string", "null"]}},
        }
    )
    metadata = [{"breadcrumb": [], "metadata": {"selected": True}}]


class DummyIncremental(IncrementalStream):
    tap_stream_id = "dummy_incremental"
    replication_method = "INCREMENTAL"
    replication_keys = ("updated_at",)
    key_properties = ("id",)
    path = "/dummy"
    data_key = "result"
    page_size = 2


class DummyFull(FullTableStream):
    tap_stream_id = "dummy_full"
    replication_method = "FULL_TABLE"
    replication_keys = tuple()
    key_properties = ("id",)
    path = "/dummy"
    data_key = "result"


class DummyFullBookmark(FullTableStream):
    tap_stream_id = "dummy_full_bookmark"
    replication_method = "FULL_TABLE"
    replication_keys = ("updated_at",)
    key_properties = ("id",)
    path = "/dummy"
    data_key = "result"


class DummyCursor(CursorPagedStream):
    tap_stream_id = "dummy_cursor"
    replication_method = "FULL_TABLE"
    replication_keys = tuple()
    key_properties = ("id",)
    path = "/dummy"
    data_key = "result"

    def sync(self, state, transformer, parent_obj=None):
        del state, transformer, parent_obj
        return 0


def _client_with_responses(responses):
    client = MagicMock()
    client.config = {"start_date": "2024-01-01T00:00:00Z", "page_size": 2}
    client.get.side_effect = responses
    return client


def test_incremental_get_records_and_sync():
    responses = [
        {"result": [{"id": 1, "updated_at": "2024-01-01T00:00:00Z"}], "limit": 2, "offset": 0},
        {"result": [], "limit": 2, "offset": 2},
    ]
    stream = DummyIncremental(_client_with_responses(responses), FakeCatalog())

    with patch("tap_sendgrid.streams.abstracts.get_bookmark", return_value="2024-01-01T00:00:00Z"), patch(
        "tap_sendgrid.streams.abstracts.write_bookmark"
    ), patch("tap_sendgrid.streams.abstracts.write_record") as write_record:
        transformer = MagicMock()
        transformer.transform.side_effect = lambda record, _schema, _metadata: record
        count = stream.sync(state={}, transformer=transformer)

    assert count in (0, 1)
    assert write_record.called


def test_full_table_sync_writes_records():
    responses = [{"result": [{"id": "abc"}], "_metadata": {}}]
    stream = DummyFull(_client_with_responses(responses), FakeCatalog())

    with patch("tap_sendgrid.streams.abstracts.write_record") as write_record:
        transformer = MagicMock()
        transformer.transform.side_effect = lambda record, _schema, _metadata: record
        count = stream.sync(state={}, transformer=transformer)

    assert count in (0, 1)
    assert write_record.called


def test_abstract_base_and_pagination_helpers():
    assert BaseStream.tap_stream_id.fget(None) is None
    assert BaseStream.replication_method.fget(None) is None
    assert BaseStream.replication_keys.fget(None) is None
    assert BaseStream.key_properties.fget(None) is None
    assert BaseStream.sync(None, None, None) is None

    offset_stream = DummyIncremental(
        _client_with_responses(
            [{"result": [{"id": 1, "updated_at": "2024-01-01T00:00:00Z"}], "limit": 1, "offset": 0}]
        ),
        FakeCatalog(),
    )
    assert isinstance(offset_stream, OffsetPagedStream)
    assert offset_stream.next_page_params([{"id": 1}]) is None

    cursor_stream = DummyCursor(_client_with_responses([]), FakeCatalog())
    assert cursor_stream.next_page_params({"_metadata": {}}) is None
    assert cursor_stream.next_page_params({"_metadata": {"next": "https://api.example.com?a=1&b=2"}}) == {
        "a": "1",
        "b": "2",
    }


def test_unix_cursor_params_and_bookmark_write():
    responses = [{"result": [{"id": "abc", "updated_at": "1704067200"}], "_metadata": {}}]
    stream = DummyIncremental(_client_with_responses(responses), FakeCatalog())
    stream.cursor_type = "unix"
    stream.client.config["lookback_window_days"] = 1

    params = stream.get_params_for_sync(1704153600)
    assert params["start_time"] == 1704067200

    value = stream.normalize_record_cursor({"updated_at": "1704153600"})
    assert value == 1704153600

    bookmark_stream = DummyFullBookmark(
        _client_with_responses([{"result": [{"id": "abc"}], "_metadata": {}}]),
        FakeCatalog(),
    )
    with patch("tap_sendgrid.streams.abstracts.write_bookmark") as write_bookmark_mock:
        transformer = MagicMock()
        transformer.transform.side_effect = lambda record, _schema, _metadata: record
        bookmark_stream.sync(state={}, transformer=transformer)
    assert write_bookmark_mock.called
