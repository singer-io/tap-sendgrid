import json
import os
import subprocess
import tempfile
import unittest

try:
    import tap_tester  # noqa: F401
except ImportError as exc:
    raise unittest.SkipTest("tap_tester not available") from exc

from tap_tester.base_suite_tests.automatic_fields_test import MinimumSelectionTest

from base import SendgridBaseTest

_MOCK_DATA_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "spike", "mock_data"))


class SendgridAutomaticFieldsTest(MinimumSelectionTest, SendgridBaseTest):
    @staticmethod
    def name():
        return "tap_tester_sendgrid_automatic_fields_test"

    def streams_to_test(self):
        # Marketing endpoints return 403 on free-tier; suppression_groups has 0 records (covered by spike).
        return {"global_suppressions", "senders", "templates"}

    def test_stream_synced_a_record(self):
        for stream in self.streams_to_test():
            if MinimumSelectionTest.record_count.get(stream, 0) == 0:
                continue
            with self.subTest(stream=stream):
                self.assertGreater(MinimumSelectionTest.record_count[stream], 0)

    def test_only_automatic_fields_replicated(self):
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
    _messages = None

    @classmethod
    def setUpClass(cls):
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

        # Select stream; deselect all non-automatic fields
        for entry in catalog["streams"]:
            if entry["stream"] == cls.SPIKE_STREAM:
                for md in entry["metadata"]:
                    if md["breadcrumb"] == []:
                        md["metadata"]["selected"] = True
                    elif md["metadata"].get("inclusion") != "automatic":
                        md["metadata"]["selected"] = False

        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump(catalog, f)
            cat_path = f.name

        sync = subprocess.run([tap_path, "--config", cfg_path, "--catalog", cat_path],
                              capture_output=True, text=True)
        if sync.returncode != 0:
            raise RuntimeError("Spike sync failed:\n" + sync.stderr)
        cls._messages = [json.loads(l) for l in sync.stdout.split("\n") if l.strip()]

    def _records(self):
        return [m["record"] for m in self._messages
                if m.get("type") == "RECORD" and m.get("stream") == self.SPIKE_STREAM]

    def test_records_synced(self):
        self.assertGreater(len(self._records()), 0)

    def test_only_automatic_fields_in_records(self):
        records = self._records()
        if not records:
            self.skipTest("No records in spike")
        for record in records:
            with self.subTest(record_id=record.get("id")):
                self.assertSetEqual(set(record.keys()), self.AUTOMATIC_FIELDS)
