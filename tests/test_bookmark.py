"""Integration tests for tap-sendgrid bookmarking."""
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest

from base import SendgridBaseTest  # pylint: disable=import-error


class SendgridBookmarkTest(BookmarkTest, SendgridBaseTest):
    """Verify bookmark-based incremental replication for all incremental streams.

    Full-table streams are excluded: they have no bookmark behavior.
    Zero-record incremental streams still validate bookmark state format.
    """

    bookmark_format = "%Y-%m-%dT%H:%M:%S%z"
    initial_bookmarks = {
        "bookmarks": {
            "blocks":             {"created": "2020-01-01T00:00:00Z"},
            "bounces":            {"created": "2020-01-01T00:00:00Z"},
            "spam_reports":       {"created": "2020-01-01T00:00:00Z"},
            "invalid_emails":     {"created": "2020-01-01T00:00:00Z"},
            "global_suppressions":{"created": "2020-01-01T00:00:00Z"},
        }
    }

    @staticmethod
    def name():
        """Return unique test-run name."""
        return "tap_tester_sendgrid_bookmark_test"

    def streams_to_test(self):
        """Return all 5 incremental streams."""
        return {s for s, m in self.expected_metadata().items()
                if m[self.REPLICATION_METHOD] == self.INCREMENTAL}

    def excluded_stream_reasons(self):
        """Document exclusion for full-table streams: no bookmark behavior."""
        return {s: "Full-table stream: bookmark state not applicable."
                for s, m in self.expected_metadata().items()
                if m[self.REPLICATION_METHOD] == self.FULL_TABLE}

    def test_excluded_streams_are_documented(self):
        """Verify each excluded stream has a documented reason."""
        excluded = self.expected_stream_names().difference(self.streams_to_test())
        self.assertSetEqual(excluded, set(self.excluded_stream_reasons().keys()))

    def calculate_new_bookmarks(self):
        """Return a deterministic future bookmark for all incremental streams."""
        return {s: {"created": "2026-02-03T00:00:00Z"} for s in self.streams_to_test()}

    def test_first_sync_bookmark(self):
        """Verify sync-1 bookmark equals max replication key. Skip streams with no records."""
        for stream in BookmarkTest.test_streams:
            records = [r["data"]
                       for r in BookmarkTest.synced_records_1.get(stream, {}).get("messages", [])
                       if r.get("action") == "upsert"]
            if not records:
                continue
            with self.subTest(stream=stream):
                rep_key = next(iter(self.expected_replication_keys(stream)))
                self.assertEqual(max(self.parse_date(r[rep_key]) for r in records),
                                 self.parse_date(BookmarkTest.bookmark_values_1.get(stream, {})))

    def test_second_sync_bookmark(self):
        """Verify sync-2 bookmark equals max replication key. Skip streams with no records."""
        for stream in BookmarkTest.test_streams:
            records = [r["data"]
                       for r in BookmarkTest.synced_records_2.get(stream, {}).get("messages", [])
                       if r.get("action") == "upsert"]
            if not records:
                continue
            with self.subTest(stream=stream):
                rep_key = next(iter(self.expected_replication_keys(stream)))
                self.assertEqual(max(self.parse_date(r[rep_key]) for r in records),
                                 self.parse_date(BookmarkTest.bookmark_values_2.get(stream, {})))

    def test_first_vs_second_records(self):
        """Sync-2 records (before bookmark) must be <= sync-1 records. Handles sparse data."""
        for stream in BookmarkTest.test_streams:
            with self.subTest(stream=stream):
                if BookmarkTest.expected_replication_methods.get(stream) != self.INCREMENTAL:
                    continue
                rep_key = next(iter(self.expected_replication_keys(stream)))
                bm_val_1 = BookmarkTest.bookmark_values_1.get(stream, {})
                sync_1 = [r["data"]
                          for r in BookmarkTest.synced_records_1.get(stream, {}).get("messages", [])
                          if r.get("action") == "upsert"]
                sync_2 = [r["data"]
                          for r in BookmarkTest.synced_records_2.get(stream, {}).get("messages", [])
                          if r.get("action") == "upsert"
                          and self.parse_date(r["data"][rep_key]) <= self.parse_date(bm_val_1)]
                self.assertLessEqual(len(sync_2), len(sync_1))
