import os

from tap_tester.base_suite_tests.base_case import BaseCase


class SendgridBaseTest(BaseCase):
    start_date = "2024-01-01T00:00:00Z"

    @staticmethod
    def tap_name():
        return "tap-sendgrid"

    @staticmethod
    def get_type():
        return "platform.sendgrid"

    @classmethod
    def expected_metadata(cls):
        return {
            "blocks": {
                cls.PRIMARY_KEYS: {"email"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"created"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 1,
            },
            "bounces": {
                cls.PRIMARY_KEYS: {"email"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"created"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 1,
            },
            "spam_reports": {
                cls.PRIMARY_KEYS: {"email"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"created"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 1,
            },
            "invalid_emails": {
                cls.PRIMARY_KEYS: {"email"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"created"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 1,
            },
            "global_suppressions": {
                cls.PRIMARY_KEYS: {"email"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"created"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 1,
            },
            "lists": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 1,
            },
            "segments": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 1,
            },
            "single_sends": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 1,
            },
            "single_send_stats": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 1,
            },
            "stats_automations": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 1,
            },
            "templates": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 1,
            },
            "suppression_groups": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 1,
            },
            "suppression_group_members": {
                cls.PRIMARY_KEYS: {"group_id", "recipient_email"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 1,
            },
            "senders": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 1,
            },
            "marketing_contacts_count": {
                cls.PRIMARY_KEYS: set(),
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 1,
            },
            "marketing_field_definitions": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 1,
            },
        }

    @staticmethod
    def get_credentials():
        return {"api_key": os.getenv("TAP_SENDGRID_API_KEY")}

    def get_properties(self, original=True):
        return {
            "start_date": self.start_date,
            "lookback_window_days": 1,
            "request_timeout": 300,
        }
