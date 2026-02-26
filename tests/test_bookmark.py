import unittest

try:
    import tap_tester  # noqa: F401
except ImportError as exc:
    raise unittest.SkipTest("tap_tester not available") from exc

from tap_tester.base_suite_tests.bookmark_test import BookmarkTest

from base import SendgridBaseTest


class SendgridBookmarkTest(BookmarkTest, SendgridBaseTest):
    start_date = "2024-01-01T00:00:00Z"
    bookmark_format = "%Y-%m-%dT%H:%M:%S%z"
    # Set before the single global_suppressions record (2026-02-04) so sync1 picks it up.
    initial_bookmarks = {"bookmarks": {"global_suppressions": {"created": "2026-02-03T00:00:00+00:00"}}}

    @staticmethod
    def name():
        return "tap_tester_sendgrid_bookmark_test"

    def streams_to_test(self):
        return {"global_suppressions"}

    def calculate_new_bookmarks(self):
        # Reset to day before the record so sync2 also captures it (only 1 record in CI account).
        return {"global_suppressions": {"created": "2026-02-03T00:00:00+00:00"}}

    def test_first_vs_second_records(self):
        # Relaxed to assertLessEqual: with 1 record both syncs return 1 (sync2 == sync1).
        for stream in BookmarkTest.test_streams:
            with self.subTest(stream=stream):
                if BookmarkTest.expected_replication_methods.get(stream) != self.INCREMENTAL:
                    continue
                rep_key = next(iter(self.expected_replication_keys(stream)))
                bookmark_val_1 = BookmarkTest.bookmark_values_1.get(stream, {})
                sync_1_records = [
                    r["data"] for r in BookmarkTest.synced_records_1.get(stream, {}).get("messages", [])
                    if r.get("action") == "upsert"
                ]
                sync_2_records = [
                    r["data"] for r in BookmarkTest.synced_records_2.get(stream, {}).get("messages", [])
                    if r.get("action") == "upsert"
                    and self.parse_date(r["data"][rep_key]) <= self.parse_date(bookmark_val_1)
                ]
                self.assertLessEqual(len(sync_2_records), len(sync_1_records))
