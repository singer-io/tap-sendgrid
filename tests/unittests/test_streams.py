from tap_sendgrid.streams import STREAMS


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
