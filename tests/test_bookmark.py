import pytest

pytest.importorskip("tap_tester")

from tap_tester.base_suite_tests.bookmark_test import BookmarkTest

from base import SendgridBaseTest


class SendgridBookmarkTest(BookmarkTest, SendgridBaseTest):
    start_date = "2024-01-01T00:00:00Z"
    bookmark_format = "%s"
    initial_bookmarks = {
        "bookmarks": {
            "blocks": {"created": 1704067200},
            "bounces": {"created": 1704067200},
            "spam_reports": {"created": 1704067200},
            "invalid_emails": {"created": 1704067200},
            "global_suppressions": {"created": 1704067200},
        }
    }

    @staticmethod
    def name():
        return "tap_tester_sendgrid_bookmark_test"

    def streams_to_test(self):
        return {"blocks", "bounces", "spam_reports", "invalid_emails", "global_suppressions"}

    def calculate_new_bookmarks(self):
        return {
            "blocks": {"created": 1704153600},
            "bounces": {"created": 1704153600},
            "spam_reports": {"created": 1704153600},
            "invalid_emails": {"created": 1704153600},
            "global_suppressions": {"created": 1704153600},
        }
