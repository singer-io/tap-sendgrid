import pytest

pytest.importorskip("tap_tester")

from tap_tester.base_suite_tests.pagination_test import PaginationTest

from base import SendgridBaseTest


class SendgridPaginationTest(PaginationTest, SendgridBaseTest):
    @staticmethod
    def name():
        return "tap_tester_sendgrid_pagination_test"

    def streams_to_test(self):
        return {
            "blocks",
            "bounces",
            "spam_reports",
            "invalid_emails",
            "global_suppressions",
            "lists",
            "single_sends",
            "templates",
        }
