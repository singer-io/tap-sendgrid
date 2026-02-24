import pytest

pytest.importorskip("tap_tester")

from tap_tester.base_suite_tests.start_date_test import StartDateTest

from base import SendgridBaseTest


class SendgridStartDateTest(StartDateTest, SendgridBaseTest):
    @staticmethod
    def name():
        return "tap_tester_sendgrid_start_date_test"

    def streams_to_test(self):
        return {"blocks", "bounces", "spam_reports", "invalid_emails", "global_suppressions"}

    @property
    def start_date_1(self):
        return "2024-01-01T00:00:00Z"

    @property
    def start_date_2(self):
        return "2024-06-01T00:00:00Z"
