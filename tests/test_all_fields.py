import pytest

pytest.importorskip("tap_tester")

from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest

from base import SendgridBaseTest


class SendgridAllFieldsTest(AllFieldsTest, SendgridBaseTest):
    MISSING_FIELDS = {"segments": {"contact_count"}}

    @staticmethod
    def name():
        return "tap_tester_sendgrid_all_fields_test"

    def streams_to_test(self):
        # Only include streams with data in this test SendGrid account
        return {
            "global_suppressions",
            "lists",
            "segments",
            "templates",
            "senders",
            "marketing_contacts_count",
            "marketing_field_definitions",
        }
