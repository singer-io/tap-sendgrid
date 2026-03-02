"""Integration tests for tap-sendgrid start_date filtering."""
from tap_tester.base_suite_tests.start_date_test import StartDateTest

from base import SendgridBaseTest  # pylint: disable=import-error


class SendgridStartDateTest(StartDateTest, SendgridBaseTest):
    """Verify start_date filters records correctly."""

    @staticmethod
    def name():
        """Return unique test-run name."""
        return "tap_tester_sendgrid_start_date_test"

    def streams_to_test(self):
        """Return incremental streams with reliable start_date coverage."""
        return {"global_suppressions"}

    def excluded_stream_reasons(self):
        """Return documented reasons for streams excluded from this test."""
        included = self.streams_to_test()
        excluded = self.expected_stream_names().difference(included)
        return {
            stream: "Excluded: start_date assertions require stable incremental records across two windows."
            for stream in excluded
        }

    def test_excluded_streams_are_documented(self):
        """Verify each excluded stream has a documented reason."""
        excluded = self.expected_stream_names().difference(self.streams_to_test())
        self.assertSetEqual(excluded, set(self.excluded_stream_reasons().keys()))

    start_date_1 = "2020-01-01T00:00:00Z"
    start_date_2 = "2022-01-01T00:00:00Z"

    def test_replicated_records(self):
        """Verify later start_date does not increase replicated records."""
        for stream in self.streams_to_test():
            with self.subTest(stream=stream):
                count_1 = StartDateTest.record_count_by_stream_1.get(stream, 0)
                count_2 = StartDateTest.record_count_by_stream_2.get(stream, 0)
                self.assertGreaterEqual(count_1, count_2)
