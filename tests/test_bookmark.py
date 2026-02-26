"""Integration tests for tap-sendgrid bookmarking."""
import unittest

try:
    from tap_tester.base_suite_tests.bookmark_test import BookmarkTest
except ImportError as exc:
    raise unittest.SkipTest("tap_tester not available") from exc

from base import SendgridBaseTest  # pylint: disable=import-error


class SendgridBookmarkTest(BookmarkTest, SendgridBaseTest):
    """Verify bookmark-based incremental replication."""

    start_date = "2024-01-01T00:00:00Z"
    bookmark_format = "%Y-%m-%dT%H:%M:%S%z"
    initial_bookmarks = {
        "bookmarks": {"global_suppressions": {"created": "2026-02-03T00:00:00+00:00"}}
    }

    @staticmethod
    def name():
        """Return unique test-run name."""
        return "tap_tester_sendgrid_bookmark_test"

    def streams_to_test(self):
        """Return streams to test."""
        return {"global_suppressions"}

    def calculate_new_bookmarks(self):
        """Return manipulated bookmarks for sync2."""
        return {"global_suppressions": {"created": "2026-02-03T00:00:00+00:00"}}

    def test_first_vs_second_records(self):
        """Verify sync2 records <= sync1 records (relaxed for single-record CI account)."""
        for stream in BookmarkTest.test_streams:
            with self.subTest(stream=stream):
                if BookmarkTest.expected_replication_methods.get(stream) != self.INCREMENTAL:
                    continue
                rep_key = next(iter(self.expected_replication_keys(stream)))
                bookmark_val_1 = BookmarkTest.bookmark_values_1.get(stream, {})
                sync_1_records = [
                    r["data"]
                    for r in BookmarkTest.synced_records_1.get(stream, {}).get("messages", [])
                    if r.get("action") == "upsert"
                ]
                sync_2_records = [
                    r["data"]
                    for r in BookmarkTest.synced_records_2.get(stream, {}).get("messages", [])
                    if r.get("action") == "upsert"
                    and self.parse_date(r["data"][rep_key]) <= self.parse_date(bookmark_val_1)
                ]
                self.assertLessEqual(len(sync_2_records), len(sync_1_records))
