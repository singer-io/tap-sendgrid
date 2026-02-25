try:
    import tap_tester  # noqa: F401
except ImportError as exc:
    import unittest
    raise unittest.SkipTest("tap_tester not available") from exc

from tap_tester.base_suite_tests.automatic_fields_test import MinimumSelectionTest

from base import SendgridBaseTest


class SendgridAutomaticFieldsTest(MinimumSelectionTest, SendgridBaseTest):
    @staticmethod
    def name():
        return "tap_tester_sendgrid_automatic_fields_test"

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
