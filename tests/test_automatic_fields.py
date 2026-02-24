import pytest

pytest.importorskip("tap_tester")

from tap_tester.base_suite_tests.automatic_fields_test import MinimumSelectionTest

from base import SendgridBaseTest


class SendgridAutomaticFieldsTest(MinimumSelectionTest, SendgridBaseTest):
    @staticmethod
    def name():
        return "tap_tester_sendgrid_automatic_fields_test"

    def streams_to_test(self):
        return self.expected_stream_names()
