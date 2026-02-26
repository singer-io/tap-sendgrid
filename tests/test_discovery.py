"""Integration tests for tap-sendgrid stream discovery."""
import unittest

try:
    from tap_tester.base_suite_tests.discovery_test import DiscoveryTest
except ImportError as exc:
    raise unittest.SkipTest("tap_tester not available") from exc

from base import SendgridBaseTest  # pylint: disable=import-error


class SendgridDiscoveryTest(DiscoveryTest, SendgridBaseTest):
    """Verify discovery returns expected stream metadata."""

    @staticmethod
    def name():
        """Return unique test-run name."""
        return "tap_tester_sendgrid_discovery_test"

    def streams_to_test(self):
        """Return all expected streams."""
        return self.expected_stream_names()
