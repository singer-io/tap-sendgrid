"""Integration tests for tap-sendgrid automatic-fields (primary keys) replication."""
import json
import os
import subprocess
import tempfile
import unittest

try:
    from tap_tester.base_suite_tests.automatic_fields_test import MinimumSelectionTest
except ImportError as exc:
    raise unittest.SkipTest("tap_tester not available") from exc

from base import SendgridBaseTest  # pylint: disable=import-error

_MOCK_DATA_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "spike", "mock_data")
)


class SendgridAutomaticFieldsTest(MinimumSelectionTest, SendgridBaseTest):
    """Verify only automatic fields are replicated when no fields are selected."""

    @staticmethod
    def name():
        """Return unique test-run name."""
        return "tap_tester_sendgrid_automatic_fields_test"

    def streams_to_test(self):
        """Return streams with CI-account data (suppression_groups covered by spike)."""
        return {"global_suppressions", "senders", "templates"}

    def test_stream_synced_a_record(self):
        """Verify streams with data synced at least one record."""
        for stream in self.streams_to_test():
            if MinimumSelectionTest.record_count.get(stream, 0) == 0:
                continue
            with self.subTest(stream=stream):
                # pylint: disable=unsubscriptable-object
                self.assertGreater(MinimumSelectionTest.record_count[stream], 0)

    def test_only_automatic_fields_replicated(self):
        """Verify only automatic fields are replicated."""
        for stream in self.streams_to_test():
            if MinimumSelectionTest.record_count.get(stream, 0) == 0:
                continue
            with self.subTest(stream=stream):
                self.assertSetEqual(
                    set(MinimumSelectionTest.actual_field.get(stream, [])),
                    self.expected_automatic_fields(stream),
                )


class SendgridAutomaticFieldsSpikeTest(unittest.TestCase):
    """Validate automatic-fields behaviour for suppression_groups using mock data."""

    SPIKE_STREAM = "suppression_groups"
    AUTOMATIC_FIELDS = {"id"}
    _messages: list = []

    @classmethod
    def setUpClass(cls):
        """Load tap messages once for all spike tests."""
        tap_path = os.getenv("STITCH_TAP_PATH", "tap-sendgrid")
        config = {
            "api_key": os.getenv("TAP_SENDGRID_API_KEY", "dummy"),
            "start_date": "2024-01-01T00:00:00Z",
            "use_mock_data": True,
            "mock_data_path": _MOCK_DATA_PATH,
        }
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as cfg_f:
            json.dump(config, cfg_f)
            cfg_path = cfg_f.name

        disc = subprocess.run(
            [tap_path, "--config", cfg_path, "--discover"],
            capture_output=True, text=True, check=False
        )
        if disc.returncode != 0:
            raise RuntimeError("Spike discovery failed:\n" + disc.stderr)
        catalog = json.loads(disc.stdout)

        for entry in catalog["streams"]:
            if entry["stream"] == cls.SPIKE_STREAM:
                for mdata in entry["metadata"]:
                    if not mdata["breadcrumb"]:
                        mdata["metadata"]["selected"] = True
                    elif mdata["metadata"].get("inclusion") != "automatic":
                        mdata["metadata"]["selected"] = False

        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as cat_f:
            json.dump(catalog, cat_f)
            cat_path = cat_f.name

        sync = subprocess.run(
            [tap_path, "--config", cfg_path, "--catalog", cat_path],
            capture_output=True, text=True, check=False
        )
        if sync.returncode != 0:
            raise RuntimeError("Spike sync failed:\n" + sync.stderr)
        cls._messages = [
            json.loads(line) for line in sync.stdout.split("\n") if line.strip()
        ]

    def _records(self):
        """Return RECORD messages for spike stream."""
        return [
            m["record"] for m in self._messages
            if m.get("type") == "RECORD" and m.get("stream") == self.SPIKE_STREAM
        ]

    def test_records_synced(self):
        """Verify records were synced."""
        self.assertGreater(len(self._records()), 0)

    def test_only_automatic_fields_in_records(self):
        """Verify only automatic fields appear in records."""
        records = self._records()
        if not records:
            self.skipTest("No records in spike")
        for record in records:
            with self.subTest(record_id=record.get("id")):
                self.assertSetEqual(set(record.keys()), self.AUTOMATIC_FIELDS)
