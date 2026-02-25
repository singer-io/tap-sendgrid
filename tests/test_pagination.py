import pytest

pytest.importorskip("tap_tester")

from tap_tester.base_suite_tests.pagination_test import PaginationTest

from base import SendgridBaseTest


class SendgridPaginationTest(PaginationTest, SendgridBaseTest):
    @staticmethod
    def name():
        return "tap_tester_sendgrid_pagination_test"

    def streams_to_test(self):
        # Only streams with more than 1 record (needed for pagination test).
        # lists: 2 records, marketing_field_definitions: 29 records
        return {
            "lists",
            "marketing_field_definitions",
        }
