"""Integration tests for tap-sendgrid automatic-fields (primary keys) replication."""
from tap_tester.base_suite_tests.automatic_fields_test import MinimumSelectionTest

from base import SendgridBaseTest  # pylint: disable=import-error


class SendgridAutomaticFieldsTest(MinimumSelectionTest, SendgridBaseTest):
    """Verify only automatic fields are replicated when no fields are selected."""

    @staticmethod
    def name():
        """Return unique test-run name."""
        return "tap_tester_sendgrid_automatic_fields_test"

    def streams_to_test(self):
        """Return all expected streams."""
        return self.expected_stream_names()

    def test_stream_synced_a_record(self):
        """Verify streams with available data synced at least one record."""
        for stream in self.streams_to_test():  # pylint: disable=unsubscriptable-object
            if MinimumSelectionTest.record_count.get(stream, 0) == 0:  # pylint: disable=unsubscriptable-object
                continue
            with self.subTest(stream=stream):
                self.assertGreater(MinimumSelectionTest.record_count[stream], 0)  # pylint: disable=unsubscriptable-object

    def test_only_automatic_fields_replicated(self):
        """Verify only automatic fields are replicated for streams with data."""
        for stream in self.streams_to_test():  # pylint: disable=unsubscriptable-object
            if MinimumSelectionTest.record_count.get(stream, 0) == 0:  # pylint: disable=unsubscriptable-object
                continue
            with self.subTest(stream=stream):
                self.assertSetEqual(
                    set(MinimumSelectionTest.actual_field.get(stream, [])),
                    self.expected_automatic_fields(stream),
                )
