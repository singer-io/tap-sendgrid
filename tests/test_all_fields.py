import pytest

pytest.importorskip("tap_tester")

from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest

from base import SendgridBaseTest


class SendgridAllFieldsTest(AllFieldsTest, SendgridBaseTest):
    MISSING_FIELDS = {}

    @staticmethod
    def name():
        return "tap_tester_sendgrid_all_fields_test"

    def streams_to_test(self):
        return self.expected_stream_names()
