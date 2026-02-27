"""Integration tests for tap-sendgrid all-fields replication."""
import json
import unittest
from pathlib import Path

from base import SendgridBaseTest
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest


class SendgridAllFieldsTest(AllFieldsTest, SendgridBaseTest):
    """Verify all selected fields are replicated."""

    @staticmethod
    def name():
        """Return unique test-run name."""
        return "tap_tester_sendgrid_all_fields_test"

    def streams_to_test(self):
        """Return all expected streams."""
        return self.expected_stream_names()

    def test_no_unexpected_streams_replicated(self):
        """Verify only expected streams are replicated."""
        synced = set(AllFieldsTest.synced_records.keys())
        unexpected = synced - AllFieldsTest.test_streams
        self.assertSetEqual(unexpected, set())

    def test_all_streams_sync_records(self):
        """Verify streams with available data synced records."""
        for stream in self.streams_to_test():
            if AllFieldsTest.record_count_by_stream.get(stream, 0) == 0:
                continue
            with self.subTest(stream=stream):
                self.assertGreater(AllFieldsTest.record_count_by_stream[stream], 0)

    def test_all_fields_for_streams_are_replicated(self):
        """Verify all fields are replicated for streams that returned records."""
        for stream in self.streams_to_test():
            if AllFieldsTest.record_count_by_stream.get(stream, 0) == 0:
                continue
            with self.subTest(stream=stream):
                expected_all_keys = self.selected_fields.get(stream, set())
                replicated = self.actual_fields.get(stream, set())
                self.assertSetEqual(replicated, expected_all_keys)


def _schema_type(schema):
    """Return a concrete JSON-schema type from a schema object."""
    schema_type = schema.get("type", "object")
    if isinstance(schema_type, list):
        non_null_types = [item for item in schema_type if item != "null"]
        return non_null_types[0] if non_null_types else "null"
    return schema_type


def _generate_value(schema):
    """Generate one mock value for a JSON-schema fragment."""
    if "enum" in schema and schema["enum"]:
        return schema["enum"][0]

    schema_type = _schema_type(schema)

    if schema_type == "object":
        properties = schema.get("properties", {})
        required = set(schema.get("required", []))
        value = {}
        for key, property_schema in properties.items():
            if key in required or _schema_type(property_schema) != "null":
                value[key] = _generate_value(property_schema)
        return value

    if schema_type == "array":
        return [_generate_value(schema.get("items", {"type": "string"}))]

    if schema_type == "integer":
        return 1

    if schema_type == "number":
        return 1.0

    if schema_type == "boolean":
        return True

    if schema_type == "string":
        schema_format = schema.get("format")
        if schema_format == "date-time":
            return "2024-01-01T00:00:00Z"
        if schema_format == "email":
            return "mock@example.com"
        return "mock"

    return None


def _value_matches_type(value, schema):
    """Return True when value is compatible with schema type."""
    schema_type = _schema_type(schema)
    if schema_type == "object":
        return isinstance(value, dict)
    if schema_type == "array":
        return isinstance(value, list)
    if schema_type == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if schema_type == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if schema_type == "boolean":
        return isinstance(value, bool)
    if schema_type == "string":
        return isinstance(value, str)
    return value is None


class SendgridDynamicMockGenerationTest(unittest.TestCase):
    """Validate runtime dynamic mock record creation from stream schemas."""

    @staticmethod
    def _schema_dir():
        return Path(__file__).resolve().parents[1] / "tap_sendgrid" / "schemas"

    @staticmethod
    def _streams_with_no_real_response():
        """Return streams that had zero records in real API integration sync."""
        if not AllFieldsTest.record_count_by_stream:
            return []

        expected_streams = set(SendgridBaseTest.expected_metadata().keys())
        return sorted(
            stream for stream in expected_streams
            if AllFieldsTest.record_count_by_stream.get(stream, 0) == 0
        )

    def test_dynamic_mock_records_only_for_no_response_streams(self):
        """Generate runtime mock records only for streams with no real response data."""
        no_response_streams = self._streams_with_no_real_response()
        if not no_response_streams:
            self.skipTest("All streams have real response data; dynamic mock generation not needed")

        schema_files = [self._schema_dir() / f"{stream}.json" for stream in no_response_streams]

        for schema_file in schema_files:
            with self.subTest(stream=schema_file.stem):
                with schema_file.open("r", encoding="utf-8") as handle:
                    schema = json.load(handle)

                record = _generate_value(schema)
                self.assertIsInstance(record, dict)

                required = set(schema.get("required", []))
                self.assertTrue(required.issubset(record.keys()))

                for key, value in record.items():
                    property_schema = schema.get("properties", {}).get(key)
                    if not property_schema:
                        continue
                    self.assertTrue(_value_matches_type(value, property_schema))

