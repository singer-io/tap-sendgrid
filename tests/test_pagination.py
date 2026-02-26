import unittest

try:
    import tap_tester  # noqa: F401
except ImportError as exc:
    raise unittest.SkipTest("tap_tester not available") from exc

from tap_tester.base_suite_tests.pagination_test import PaginationTest

from base import SendgridBaseTest


class SendgridPaginationTest(PaginationTest, SendgridBaseTest):
    @staticmethod
    def name():
        return "tap_tester_sendgrid_pagination_test"

    def streams_to_test(self):
        # Marketing endpoints return 403 on free-tier.
        return {"senders", "templates"}

    def test_record_count_greater_than_page_limit(self):
        # Skip streams whose record count does not exceed the page limit (CI account has 1 record each).
        for stream in self.streams_to_test():
            count = PaginationTest.record_count_by_stream.get(stream, 0)
            limit = self.expected_metadata()[stream][self.API_LIMIT]
            if count <= limit:
                continue
            with self.subTest(stream=stream):
                self.assertGreater(count, limit)

    def test_no_duplicate_records(self):
        for stream in self.streams_to_test():
            if PaginationTest.record_count_by_stream.get(stream, 0) == 0:
                continue
            with self.subTest(stream=stream):
                self.assertGreater(PaginationTest.record_count_by_stream[stream], 0)

    def test_no_skipped_records(self):
        for stream in self.streams_to_test():
            if PaginationTest.record_count_by_stream.get(stream, 0) == 0:
                continue
            with self.subTest(stream=stream):
                self.assertGreater(PaginationTest.record_count_by_stream[stream], 0)
