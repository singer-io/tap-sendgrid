import json
import os
import subprocess
import tempfile
import unittest

try:
    import tap_tester  # noqa: F401
except ImportError as exc:
    raise unittest.SkipTest("tap_tester not available") from exc

from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest

from base import SendgridBaseTest

_MOCK_DATA_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "spike", "mock_data"))


class SendgridAllFieldsTest(AllFieldsTest, SendgridBaseTest):
    MISSING_FIELDS = {}

    @staticmethod
    def name():
        return "tap_tester_sendgrid_all_fields_test"

    def streams_to_test(self):
        # Marketing endpoints return 403 on free-tier; suppression_groups has 0 records (covered by spike).
        return {"global_suppressions", "senders", "templates"}

    def test_no_unexpected_streams_replicated(self):
        synced = set(AllFieldsTest.synced_records.keys())
        unexpected = synced - AllFieldsTest.test_streams
        self.assertSetEqual(unexpected, set(), msg=f"Unexpected streams replicated: {unexpected}")
        streams_with_data = {s for s in AllFieldsTest.test_streams
                             if AllFieldsTest.record_count_by_stream.get(s, 0) > 0}
        self.assertSetEqual(streams_with_data - synced, set(),
                            msg=f"Streams with records missing from sync output: {streams_with_data - synced}")

    def test_all_streams_sync_records(self):
        for stream in self.streams_to_test():
            if AllFieldsTest.record_count_by_stream.get(stream, 0) == 0:
                continue
            with self.subTest(stream=stream):
                self.assertGreater(AllFieldsTest.record_count_by_stream[stream], 0)

    def test_all_fields_for_streams_are_replicated(self):
        for stream in self.streams_to_test():
            if AllFieldsTest.record_count_by_stream.get(stream, 0) == 0:
                continue
            with self.subTest(stream=stream):
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
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(config, f)
        cfg_path = f.name

    disc = subprocess.run([tap_path, "--config", cfg_path, "--discover"],
                          capture_output=True, text=True)
    if disc.returncode != 0:
        raise RuntimeError("Spike discovery failed:\n" + disc.stderr)
    catalog = json.loads(disc.stdout)

    for entry in catalog["streams"]:
        if entry["stream"] in spike_streams:
            for md in entry["metadata"]:
                if md["breadcrumb"] == []:
                    md["metadata"]["selected"] = True

    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(catalog, f)
        cat_path = f.name

    sync = subprocess.run([tap_path, "--config", cfg_path, "--catalog", cat_path],
                          capture_output=True, text=True)
    if sync.returncode != 0:
        raise RuntimeError("Spike sync failed:\n" + sync.stderr)
    return [json.loads(l) for l in sync.stdout.split("\n") if l.strip()]


class SendgridAllFieldsSpikeTest(unittest.TestCase):
    """Validate 0-record streams using mock data."""

    SPIKE_STREAMS = {"suppression_groups", "suppression_group_members"}
    _messages = None

    @classmethod
    def setUpClass(cls):
        cls._messages = _spike_messages(cls.SPIKE_STREAMS)

    def _records(self, stream):
        return [m["record"] for m in self._messages
                if m.get("type") == "RECORD" and m.get("stream") == stream]

    def test_schemas_emitted(self):
        schemas = {m["stream"] for m in self._messages if m.get("type") == "SCHEMA"}
        for stream in self.SPIKE_STREAMS:
            with self.subTest(stream=stream):
                self.assertIn(stream, schemas)

    def test_suppression_groups_fields(self):
        records = self._records("suppression_groups")
        self.assertTrue(records)
        self.assertSetEqual(set().union(*(r.keys() for r in records)),
                            {"id", "name", "description", "is_default"})

    def test_suppression_group_members_fields(self):
        records = self._records("suppression_group_members")
        self.assertTrue(records)
        self.assertSetEqual(set().union(*(r.keys() for r in records)),
                            {"group_id", "recipient_email"})
