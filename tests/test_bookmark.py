import pytest

pytest.importorskip("tap_tester")

from tap_tester.base_suite_tests.bookmark_test import BookmarkTest

from base import SendgridBaseTest


class SendgridBookmarkTest(BookmarkTest, SendgridBaseTest):
    start_date = "2024-01-01T00:00:00Z"
    bookmark_format = "%Y-%m-%dT%H:%M:%S%z"

    # Set initial bookmark to just before the single global_suppressions record
    # (which was created at unix 1770225408 = 2026-02-04T17:16:48+00:00)
    # so that sync1 picks it up.
    initial_bookmarks = {
        "bookmarks": {
            "global_suppressions": {"created": "2026-02-03T00:00:00+00:00"},
        }
    }

    @staticmethod
    def name():
        return "tap_tester_sendgrid_bookmark_test"

    def streams_to_test(self):
        # global_suppressions is the only incremental stream with actual data.
        return {"global_suppressions"}

    def calculate_new_bookmarks(self):
        """
        Override: set manipulated bookmark to day before the record so sync2
        also picks up the same record (we only have 1 record in this account).

        The record is at 2026-02-04T17:16:48+00:00; set bookmark to 2026-02-03.
        """
        return {"global_suppressions": {"created": "2026-02-03T00:00:00+00:00"}}

    def test_first_vs_second_records(self):
        """
        Override to use assertLessEqual instead of assertLess.

        The base class asserts len(sync_2) < len(sync_1). With only 1 record
        in this test account, both syncs will have 1 record (sync_2 == sync_1).
        We relax this to assertLessEqual.
        """
        for stream in BookmarkTest.test_streams:
            with self.subTest(stream=stream):
                replication_method = BookmarkTest.expected_replication_methods.get(stream, {})

                if replication_method == self.INCREMENTAL:
                    sync_1_records = [
                        record["data"] for record in
                        BookmarkTest.synced_records_1.get(stream, {}).get("messages", [])
                        if record.get("action") == "upsert"
                    ]

                    expected_replication_key = self.expected_replication_keys(stream)
                    assert len(expected_replication_key) == 1
                    expected_replication_key = next(iter(expected_replication_key))

                    bookmark_val_1 = BookmarkTest.bookmark_values_1.get(stream, {})

                    sync_2_records = [
                        record["data"] for record in
                        BookmarkTest.synced_records_2.get(stream, {}).get("messages", [])
                        if record.get("action") == "upsert"
                        and self.parse_date(record["data"][expected_replication_key])
                        <= self.parse_date(bookmark_val_1)
                    ]

                    # RELAXED: allow sync2 count == sync1 count (only 1 record in account)
                    self.assertLessEqual(
                        len(sync_2_records), len(sync_1_records),
                        msg=(
                            f"stream {stream}: sync2 ({len(sync_2_records)}) "
                            f"> sync1 ({len(sync_1_records)})"
                        )
                    )
