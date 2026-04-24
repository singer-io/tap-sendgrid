from unittest.mock import MagicMock

from tap_sendgrid.streams import STREAMS
from tap_sendgrid.streams.suppression_groups import SuppressionGroupMembers


def test_stream_registry_contains_expected_streams():
    expected = {
        "blocks",
        "bounces",
        "spam_reports",
        "invalid_emails",
        "global_suppressions",
        "lists",
        "segments",
        "single_sends",
        "single_send_stats",
        "stats_automations",
        "templates",
        "suppression_groups",
        "suppression_group_members",
        "senders",
        "marketing_contacts_count",
        "marketing_field_definitions",
    }
    assert expected == set(STREAMS.keys())


def _make_catalog():
    from types import SimpleNamespace
    return SimpleNamespace(
        schema=SimpleNamespace(to_dict=lambda: {"type": "object", "properties": {}}),
        metadata=[{"breadcrumb": [], "metadata": {"selected": True}}],
    )


def test_suppression_group_members_handles_email_string_list():
    """SuppressionGroupMembers.get_records must yield dicts from a list of email strings."""
    client = MagicMock()
    client.config = {"start_date": "2024-01-01T00:00:00Z", "page_size": 50}
    client.get.return_value = ["alice@example.com", "bob@example.com"]

    stream = SuppressionGroupMembers(client, _make_catalog())
    parent = {"id": 42, "name": "Group 1"}

    records = list(stream.get_records(parent_obj=parent))
    assert len(records) == 2
    assert records[0] == {"group_id": 42, "recipient_email": "alice@example.com"}
    assert records[1] == {"group_id": 42, "recipient_email": "bob@example.com"}


def test_suppression_group_members_handles_empty_response():
    """SuppressionGroupMembers.get_records must yield nothing for an empty list response."""
    client = MagicMock()
    client.config = {"start_date": "2024-01-01T00:00:00Z", "page_size": 50}
    client.get.return_value = []

    stream = SuppressionGroupMembers(client, _make_catalog())
    records = list(stream.get_records(parent_obj={"id": 99, "name": "Empty group"}))
    assert records == []


def test_suppression_group_members_handles_dict_response():
    """SuppressionGroupMembers.get_records must also handle dict items (fallback)."""
    client = MagicMock()
    client.config = {"start_date": "2024-01-01T00:00:00Z", "page_size": 50}
    client.get.return_value = [{"email": "carol@example.com"}]

    stream = SuppressionGroupMembers(client, _make_catalog())
    records = list(stream.get_records(parent_obj={"id": 7}))
    assert len(records) == 1
    assert records[0]["recipient_email"] == "carol@example.com"
    assert records[0]["group_id"] == 7

