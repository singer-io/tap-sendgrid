import unittest

try:
    import tap_tester  # noqa: F401
except ImportError as exc:
    raise unittest.SkipTest("tap_tester not available") from exc

from tap_tester.base_suite_tests.discovery_test import DiscoveryTest

from base import SendgridBaseTest


class SendgridDiscoveryTest(DiscoveryTest, SendgridBaseTest):
    @staticmethod
    def name():
        return "tap_tester_sendgrid_discovery_test"

    def streams_to_test(self):
        return self.expected_stream_names()
