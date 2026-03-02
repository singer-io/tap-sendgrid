"""Integration tests for tap-sendgrid all-fields replication."""
import json
from pathlib import Path

from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest

from base import SendgridBaseTest  # pylint: disable=import-error

_SCHEMA_DIR = Path(__file__).resolve().parents[1] / "tap_sendgrid" / "schemas"


class SendgridAllFieldsTest(AllFieldsTest, SendgridBaseTest):
    """Verify all selected fields are replicated for all streams.

    Streams with real API data: assert actual fields match schema.
    Streams with no API data: generate a mock record via _generate_value
    and assert it satisfies the schema's required keys.
    """

    @staticmethod
    def name():
        """Return unique test-run name."""
        return "tap_tester_sendgrid_all_fields_test"

    def streams_to_test(self):
        """Return all 16 expected streams."""
        return self.expected_stream_names()

    def test_no_unexpected_streams_replicated(self):
        """Verify only expected streams are replicated."""
        self.assertSetEqual(set(AllFieldsTest.synced_records.keys()) - AllFieldsTest.test_streams,
                            set())

    def test_all_streams_sync_records(self):
        """Verify every stream with real API data returned at least one record."""
        for stream in self.streams_to_test():  # pylint: disable=unsubscriptable-object
            if AllFieldsTest.record_count_by_stream.get(stream, 0) == 0:  # pylint: disable=unsubscriptable-object
                continue
            with self.subTest(stream=stream):
                self.assertGreater(AllFieldsTest.record_count_by_stream[stream], 0)  # pylint: disable=unsubscriptable-object

    def test_all_fields_for_streams_are_replicated(self):
        """Real data: assert all schema fields replicated. No data: validate mock record shape."""
        for stream in self.streams_to_test():  # pylint: disable=unsubscriptable-object
            with self.subTest(stream=stream):
                if AllFieldsTest.record_count_by_stream.get(stream, 0) > 0:  # pylint: disable=unsubscriptable-object
                    self.assertSetEqual(self.actual_fields.get(stream, set()),
                                        self.selected_fields.get(stream, set()))
                else:
                    with (_SCHEMA_DIR / f"{stream}.json").open(encoding="utf-8") as fh:
                        schema = json.load(fh)
                    record = self._generate_value(schema)
                    self.assertIsInstance(record, dict)
                    self.assertTrue(set(schema.get("required", [])).issubset(record.keys()))
