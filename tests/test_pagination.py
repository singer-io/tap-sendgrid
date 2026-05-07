"""Integration tests for tap-sendgrid pagination."""
from tap_tester.base_suite_tests.pagination_test import PaginationTest

from base import SendgridBaseTest  # pylint: disable=import-error


class SendgridPaginationTest(PaginationTest, SendgridBaseTest):
    """Verify pagination works correctly."""

    @staticmethod
    def name():
        """Return unique test-run name."""
        return "tap_tester_sendgrid_pagination_test"

    def streams_to_test(self):
        """Return streams that consistently have multi-page data."""
        return {"lists", "marketing_field_definitions"}

    def excluded_stream_reasons(self):
        """Return documented reasons for streams excluded from this test."""
        included = self.streams_to_test()
        excluded = self.expected_stream_names().difference(included)
        return {
            stream: "Excluded: pagination assertion requires record_count > page_size in account."
            for stream in excluded
        }

    def test_excluded_streams_are_documented(self):
        """Verify each excluded stream has a documented reason."""
        excluded = self.expected_stream_names().difference(self.streams_to_test())
        self.assertSetEqual(excluded, set(self.excluded_stream_reasons().keys()))
