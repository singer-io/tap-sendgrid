"""Integration tests for tap-sendgrid all-fields replication."""
import json
import os
import subprocess
import tempfile
import unittest

try:
    from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest
except ImportError as exc:
    raise unittest.SkipTest("tap_tester not available") from exc

from base import SendgridBaseTest  # pylint: disable=import-error

_MOCK_DATA_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "spike", "mock_data")
)


class SendgridAllFieldsTest(AllFieldsTest, SendgridBaseTest):
    """Verify all selected fields are replicated."""

    MISSING_FIELDS: dict = {}

    @staticmethod
    def name():
        """Return unique test-run name."""
        return "tap_tester_sendgrid_all_fields_test"

    def streams_to_test(self):
        """Return streams with CI-account data (suppression_groups covered by spike)."""
        return {"global_suppressions", "senders", "templates"}

    def test_no_unexpected_streams_replicated(self):
        """Verify only expected streams are replicated."""
        synced = set(AllFieldsTest.synced_records.keys())
        unexpected = synced - AllFieldsTest.test_streams
        self.assertSetEqual(unexpected, set())
        streams_with_data = {
            s for s in AllFieldsTest.test_streams
            if AllFieldsTest.record_count_by_stream.get(s, 0) > 0
        }
        self.assertSetEqual(streams_with_data - synced, set())

    def test_all_streams_sync_records(self):
        """Verify streams with data synced records."""
        for stream in self.streams_to_test():
            if AllFieldsTest.record_count_by_stream.get(stream, 0) == 0:
                continue
            with self.subTest(stream=stream):
                # pylint: disable=unsubscriptable-object
                self.assertGreater(AllFieldsTest.record_count_by_stream[stream], 0)

    def test_all_fields_for_streams_are_replicated(self):
        """Verify all fields for streams with data are replicated."""
        for stream in self.streams_to_test():
            if AllFieldsTest.record_count_by_stream.get(stream, 0) == 0:
                continue
            with self.subTest(stream=stream):
                # pylint: disable=unsubscriptable-object
                self.assertGreater(AllFieldsTest.record_count_by_stream[stream], 0)


def _spike_messages(spike_streams):
    """Run tap with mock data and return parsed Singer messages."""
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
        if entry["stream"] in spike_streams:
            for mdata in entry["metadata"]:
                if not mdata["breadcrumb"]:
                    mdata["metadata"]["selected"] = True

    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as cat_f:
        json.dump(catalog, cat_f)
        cat_path = cat_f.name

    sync = subprocess.run(
        [tap_path, "--config", cfg_path, "--catalog", cat_path],
        capture_output=True, text=True, check=False
    )
    if sync.returncode != 0:
        raise RuntimeError("Spike sync failed:\n" + sync.stderr)
    return [json.loads(line) for line in sync.stdout.split("\n") if line.strip()]


class SendgridAllFieldsSpikeTest(unittest.TestCase):
    """Validate 0-record streams using mock data."""

    SPIKE_STREAMS = {"suppression_groups", "suppression_group_members"}
    _messages: list = []

    @classmethod
    def setUpClass(cls):
        """Load tap messages once for all spike tests."""
        cls._messages = _spike_messages(cls.SPIKE_STREAMS)

    def _records(self, stream):
        """Return RECORD messages for a stream."""
        return [
            m["record"] for m in self._messages
            if m.get("type") == "RECORD" and m.get("stream") == stream
        ]

    def test_schemas_emitted(self):
        """Verify SCHEMA messages are emitted for all spike streams."""
        schemas = {m["stream"] for m in self._messages if m.get("type") == "SCHEMA"}
        for stream in self.SPIKE_STREAMS:
            with self.subTest(stream=stream):
                self.assertIn(stream, schemas)

    def test_suppression_groups_fields(self):
        """Verify suppression_groups records have expected fields."""
        records = self._records("suppression_groups")
        self.assertTrue(records)
        self.assertSetEqual(
            set().union(*(r.keys() for r in records)),
            {"id", "name", "description", "is_default"}
        )

    def test_suppression_group_members_fields(self):
        """Verify suppression_group_members records have expected fields."""
        records = self._records("suppression_group_members")
        self.assertTrue(records)
        self.assertSetEqual(
            set().union(*(r.keys() for r in records)),
            {"group_id", "recipient_email"}
        )
