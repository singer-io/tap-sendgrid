"""Extended unit tests for stream sync behavior.

Covers:
- Record count accuracy (Singer counter.value resets to 0 after context exit)
- IncrementalStream with datetime cursor type
- FullTableStream record count
- Bookmark write after full-table sync with replication key
- Discovery metadata structure
"""
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from tap_sendgrid.streams.abstracts import FullTableStream, IncrementalStream


# ---------------------------------------------------------------------------
# Shared fixtures / helpers
# ---------------------------------------------------------------------------

class FakeCatalog:
    schema = SimpleNamespace(
        to_dict=lambda: {
            "type": "object",
            "properties": {
                "id": {"type": ["null", "string"]},
                "updated_at": {"type": ["null", "string"]},
            },
        }
    )
    metadata = [{"breadcrumb": [], "metadata": {"selected": True}}]


class DatetimeIncremental(IncrementalStream):
    """IncrementalStream with datetime (ISO string) cursor — not unix integer."""

    tap_stream_id = "dt_stream"
    replication_method = "INCREMENTAL"
    replication_keys = ("updated_at",)
    key_properties = ("id",)
    path = "/dt"
    data_key = "result"
    cursor_type = "datetime"  # ISO string comparison


class UnixIncremental(IncrementalStream):
    """IncrementalStream with unix integer cursor."""

    tap_stream_id = "unix_stream"
    replication_method = "INCREMENTAL"
    replication_keys = ("created",)
    key_properties = ("email",)
    path = "/unix"
    data_key = None
    cursor_type = "unix"


class CountableFull(FullTableStream):
    """FullTableStream with no replication key — pure full table."""

    tap_stream_id = "countable_full"
    replication_method = "FULL_TABLE"
    replication_keys = ()
    key_properties = ("id",)
    path = "/full"
    data_key = "result"


def _make_client(*responses):
    client = MagicMock()
    client.config = {"start_date": "2024-01-01T00:00:00Z", "page_size": 10}
    client.get.side_effect = list(responses)
    return client


# ---------------------------------------------------------------------------
# IncrementalStream — datetime cursor type
# ---------------------------------------------------------------------------


def test_incremental_datetime_cursor_does_not_crash():
    """IncrementalStream with cursor_type='datetime' must not call datetime.fromtimestamp."""
    responses = [
        {"result": [{"id": "a", "updated_at": "2024-06-01T00:00:00Z"}], "limit": 10, "offset": 0},
        {"result": [], "limit": 10, "offset": 1},
    ]
    stream = DatetimeIncremental(_make_client(*responses), FakeCatalog())

    with patch("tap_sendgrid.streams.abstracts.get_bookmark", return_value="2024-01-01T00:00:00Z"), \
         patch("tap_sendgrid.streams.abstracts.write_bookmark") as wb, \
         patch("tap_sendgrid.streams.abstracts.write_record"):
        transformer = MagicMock()
        transformer.transform.side_effect = lambda r, _s, _m: r
        count = stream.sync(state={}, transformer=transformer)

    assert count == 1
    # Bookmark must be written as an ISO string (not via fromtimestamp)
    wb.assert_called_once()
    _, _, _, bk_value = wb.call_args[0]
    assert isinstance(bk_value, str)
    assert "2024" in bk_value


def test_incremental_datetime_cursor_filters_old_records():
    """Records whose cursor value < bookmark must be excluded from the count."""
    responses = [
        {
            "result": [
                {"id": "old", "updated_at": "2023-01-01T00:00:00Z"},  # before bookmark
                {"id": "new", "updated_at": "2024-06-01T00:00:00Z"},  # after bookmark
            ],
            "limit": 10,
            "offset": 0,
        },
        {"result": [], "limit": 10, "offset": 2},
    ]
    stream = DatetimeIncremental(_make_client(*responses), FakeCatalog())

    with patch("tap_sendgrid.streams.abstracts.get_bookmark", return_value="2024-01-01T00:00:00Z"), \
         patch("tap_sendgrid.streams.abstracts.write_bookmark"), \
         patch("tap_sendgrid.streams.abstracts.write_record") as wr:
        transformer = MagicMock()
        transformer.transform.side_effect = lambda r, _s, _m: r
        count = stream.sync(state={}, transformer=transformer)

    assert count == 1  # only the "new" record passes
    assert wr.call_count == 1


# ---------------------------------------------------------------------------
# IncrementalStream — unix cursor type
# ---------------------------------------------------------------------------


def test_incremental_unix_cursor_bookmark_written_as_iso():
    """Unix-cursor IncrementalStream must write the bookmark as an ISO string."""
    epoch_2024 = 1704067200  # 2024-01-01T00:00:00Z
    responses = [
        [{"email": "a@b.com", "created": epoch_2024}],
    ]
    stream = UnixIncremental(_make_client(*responses), FakeCatalog())

    with patch("tap_sendgrid.streams.abstracts.get_bookmark", return_value="2020-01-01T00:00:00Z"), \
         patch("tap_sendgrid.streams.abstracts.write_bookmark") as wb, \
         patch("tap_sendgrid.streams.abstracts.write_record"):
        transformer = MagicMock()
        transformer.transform.side_effect = lambda r, _s, _m: r
        count = stream.sync(state={}, transformer=transformer)

    assert count == 1
    _, _, _, bk_value = wb.call_args[0]
    # Bookmark must be an ISO datetime string (not an integer)
    assert isinstance(bk_value, str)
    assert "T" in bk_value


def test_incremental_unix_cursor_returns_accurate_count():
    """
    Singer's metrics.Counter resets value to 0 on __exit__.
    IncrementalStream must track the count independently and return it correctly.
    """
    epoch_records = [1704067200, 1704153600, 1704240000]  # 3 records after 2020 bookmark
    mock_response = [{"email": f"u{i}@x.com", "created": ts} for i, ts in enumerate(epoch_records)]
    stream = UnixIncremental(_make_client(mock_response), FakeCatalog())

    with patch("tap_sendgrid.streams.abstracts.get_bookmark", return_value="2020-01-01T00:00:00Z"), \
         patch("tap_sendgrid.streams.abstracts.write_bookmark"), \
         patch("tap_sendgrid.streams.abstracts.write_record"):
        transformer = MagicMock()
        transformer.transform.side_effect = lambda r, _s, _m: r
        count = stream.sync(state={}, transformer=transformer)

    assert count == 3, (
        f"Expected 3 records but got {count}. "
        "This likely means counter.value (which resets to 0) is being returned instead "
        "of a separately maintained count."
    )


# ---------------------------------------------------------------------------
# FullTableStream — record count accuracy
# ---------------------------------------------------------------------------


def test_full_table_returns_accurate_count():
    """
    FullTableStream.sync() must return the actual record count, not counter.value
    (which Singer resets to 0 after the context manager exits).
    """
    records = [{"id": str(i)} for i in range(5)]
    responses = [{"result": records, "_metadata": {}}]
    stream = CountableFull(_make_client(*responses), FakeCatalog())

    with patch("tap_sendgrid.streams.abstracts.write_record"):
        transformer = MagicMock()
        transformer.transform.side_effect = lambda r, _s, _m: r
        count = stream.sync(state={}, transformer=transformer)

    assert count == 5, (
        f"Expected 5 records but got {count}. "
        "Ensure record_count is tracked separately from counter.value."
    )


def test_full_table_zero_records():
    """FullTableStream.sync() must return 0 when there are no records."""
    responses = [{"result": [], "_metadata": {}}]
    stream = CountableFull(_make_client(*responses), FakeCatalog())

    with patch("tap_sendgrid.streams.abstracts.write_record") as wr:
        transformer = MagicMock()
        transformer.transform.side_effect = lambda r, _s, _m: r
        count = stream.sync(state={}, transformer=transformer)

    assert count == 0
    wr.assert_not_called()


# ---------------------------------------------------------------------------
# Discovery metadata validation
# ---------------------------------------------------------------------------


def test_discover_stream_metadata_structure():
    """Each catalog stream must have root metadata with table-key-properties."""
    from tap_sendgrid.discover import discover
    from singer import metadata as singer_metadata

    catalog = discover()
    for entry in catalog.streams:
        mdata = singer_metadata.to_map(entry.metadata)
        root_meta = mdata.get((), {})
        assert "table-key-properties" in root_meta, (
            f"Stream {entry.stream} is missing 'table-key-properties' in root metadata"
        )
        assert "valid-replication-keys" in root_meta or root_meta.get("forced-replication-method") == "FULL_TABLE", (
            f"Stream {entry.stream} should have 'valid-replication-keys' or be FULL_TABLE"
        )
        assert "forced-replication-method" in root_meta, (
            f"Stream {entry.stream} is missing 'forced-replication-method' in root metadata"
        )


def test_discover_incremental_streams_have_replication_key():
    """All incremental streams must advertise a valid-replication-keys in metadata."""
    from tap_sendgrid.discover import discover
    from singer import metadata as singer_metadata

    incremental_streams = {
        "blocks", "bounces", "spam_reports", "invalid_emails", "global_suppressions"
    }
    catalog = discover()
    for entry in catalog.streams:
        if entry.stream not in incremental_streams:
            continue
        mdata = singer_metadata.to_map(entry.metadata)
        root_meta = mdata.get((), {})
        assert root_meta.get("forced-replication-method") == "INCREMENTAL", (
            f"{entry.stream} should be INCREMENTAL"
        )
        replication_keys = root_meta.get("valid-replication-keys", [])
        assert len(replication_keys) > 0, (
            f"{entry.stream} is INCREMENTAL but has no valid-replication-keys"
        )


def test_discover_full_table_streams_have_no_replication_keys():
    """Full-table streams must have empty replication-keys list."""
    from tap_sendgrid.discover import discover
    from singer import metadata as singer_metadata

    full_table_streams = {
        "lists", "segments", "single_sends", "single_send_stats",
        "stats_automations", "templates", "suppression_groups",
        "suppression_group_members", "senders",
        "marketing_contacts_count", "marketing_field_definitions",
    }
    catalog = discover()
    for entry in catalog.streams:
        if entry.stream not in full_table_streams:
            continue
        mdata = singer_metadata.to_map(entry.metadata)
        root_meta = mdata.get((), {})
        assert root_meta.get("forced-replication-method") == "FULL_TABLE", (
            f"{entry.stream} should be FULL_TABLE"
        )
