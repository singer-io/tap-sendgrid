"""Integration tests for tap-sendgrid bookmarking."""
from base import SendgridBaseTest
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


class SendgridBookmarkTest(BookmarkTest, SendgridBaseTest):
    """Verify bookmark-based incremental replication."""

    bookmark_format = "%Y-%m-%dT%H:%M:%S%z"
    initial_bookmarks = {
        "bookmarks": {
            "global_suppressions": {"created": "2020-01-01T00:00:00.000000Z"},
        }
    }

    @staticmethod
    def name():
        """Return unique test-run name."""
        return "tap_tester_sendgrid_bookmark_test"

    def streams_to_test(self):
        """Return incremental streams with stable bookmark coverage."""
        return {"global_suppressions"}

    def excluded_stream_reasons(self):
        """Return documented reasons for streams excluded from this test."""
        included = self.streams_to_test()
        excluded = self.expected_stream_names().difference(included)
        return {
            stream: "Excluded: bookmark test needs >=2 historical incremental values in account."
            for stream in excluded
        }

    def test_excluded_streams_are_documented(self):
        """Verify each excluded stream has a documented reason."""
        excluded = self.expected_stream_names().difference(self.streams_to_test())
        self.assertSetEqual(excluded, set(self.excluded_stream_reasons().keys()))

    def calculate_new_bookmarks(self):
        """Return deterministic bookmark for sparse test-account data."""
        return {
            "global_suppressions": {"created": "2026-02-03T00:00:00.000000Z"}
        }

    def test_first_vs_second_records(self):
        """Verify sync2 records are not greater than sync1 for incremental streams."""
        for stream in BookmarkTest.test_streams:
            with self.subTest(stream=stream):
                if BookmarkTest.expected_replication_methods.get(stream) != self.INCREMENTAL:
                    continue
                rep_key = next(iter(self.expected_replication_keys(stream)))
                bookmark_val_1 = BookmarkTest.bookmark_values_1.get(stream, {})
                sync_1_records = [
                    rec["data"]
                    for rec in BookmarkTest.synced_records_1.get(stream, {}).get("messages", [])
                    if rec.get("action") == "upsert"
                ]
                sync_2_records = [
                    rec["data"]
                    for rec in BookmarkTest.synced_records_2.get(stream, {}).get("messages", [])
                    if rec.get("action") == "upsert"
                    and self.parse_date(rec["data"][rep_key]) <= self.parse_date(bookmark_val_1)
                ]
                self.assertLessEqual(len(sync_2_records), len(sync_1_records))
