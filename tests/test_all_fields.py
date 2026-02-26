try:
    import tap_tester  # noqa: F401
except ImportError as exc:
    import unittest
    raise unittest.SkipTest("tap_tester not available") from exc

from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest

from base import SendgridBaseTest


class SendgridAllFieldsTest(AllFieldsTest, SendgridBaseTest):
    # All schema fields are expected to appear in API responses.
    # Previously listed {"segments": {"contact_count"}} but that field was
    # renamed to ``contacts_count`` in our schema and IS returned by the API.
    MISSING_FIELDS = {}

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
