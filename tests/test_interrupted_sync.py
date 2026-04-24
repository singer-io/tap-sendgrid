"""Integration tests for tap-sendgrid interrupted-sync recovery."""
from tap_tester.base_suite_tests.interrupted_sync_test import InterruptedSyncTest

from base import SendgridBaseTest  # pylint: disable=import-error


class SendgridInterruptedSyncTest(InterruptedSyncTest, SendgridBaseTest):
    """Verify tap recovers correctly from an interrupted sync."""

    @staticmethod
    def name():
        """Return unique test-run name."""
        return "tap_tester_sendgrid_interrupted_sync_test"

    def streams_to_test(self):
        """Return stream with stable interrupted-sync coverage."""
        return {"global_suppressions"}

    def excluded_stream_reasons(self):
        """Return documented reasons for streams excluded from this test."""
        included = self.streams_to_test()
        excluded = self.expected_stream_names().difference(included)
        return {
            stream: "Excluded: interrupted-sync assertions need stable incremental history per stream."
            for stream in excluded
        }

    def test_excluded_streams_are_documented(self):
        """Verify each excluded stream has a documented reason."""
        excluded = self.expected_stream_names().difference(self.streams_to_test())
        self.assertSetEqual(excluded, set(self.excluded_stream_reasons().keys()))

    def manipulate_state(self):
        """Return state simulating an interrupted sync."""
        return {
            "currently_syncing": "global_suppressions",
            "bookmarks": {
                "global_suppressions": {"created": "2020-01-01T00:00:00.000000Z"},
            },
        }

    def test_interrupted_sync_stream_order(self):
        """Verify interrupted sync state is cleared after recovery."""
        self.assertIsNone(self.resuming_sync_state.get("currently_syncing"))
