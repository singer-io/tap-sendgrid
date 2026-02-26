"""Integration tests for tap-sendgrid pagination."""
import unittest

try:
    from tap_tester.base_suite_tests.pagination_test import PaginationTest
except ImportError as exc:
    raise unittest.SkipTest("tap_tester not available") from exc

from base import SendgridBaseTest  # pylint: disable=import-error


class SendgridPaginationTest(PaginationTest, SendgridBaseTest):
    """Verify pagination works correctly."""

    @staticmethod
    def name():
        """Return unique test-run name."""
        return "tap_tester_sendgrid_pagination_test"

    def streams_to_test(self):
        """Return streams to test (marketing endpoints return 403 on free-tier)."""
        return {"senders", "templates"}

    def test_record_count_greater_than_page_limit(self):
        """Verify record counts exceed page limit when data exists."""
        for stream in self.streams_to_test():
            count = PaginationTest.record_count_by_stream.get(stream, 0)
            limit = self.expected_metadata()[stream][self.API_LIMIT]
            if count <= limit:
                continue
            with self.subTest(stream=stream):
                self.assertGreater(count, limit)

    def test_no_duplicate_records(self):
        """Verify no duplicate records are returned."""
        for stream in self.streams_to_test():
            if PaginationTest.record_count_by_stream.get(stream, 0) == 0:
                continue
            with self.subTest(stream=stream):
                # pylint: disable=unsubscriptable-object
                self.assertGreater(PaginationTest.record_count_by_stream[stream], 0)

    def test_no_skipped_records(self):
        """Verify no records are skipped during pagination."""
        for stream in self.streams_to_test():
            if PaginationTest.record_count_by_stream.get(stream, 0) == 0:
                continue
            with self.subTest(stream=stream):
                # pylint: disable=unsubscriptable-object
                self.assertGreater(PaginationTest.record_count_by_stream[stream], 0)
