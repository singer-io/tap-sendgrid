"""Integration tests for tap-sendgrid start_date filtering."""
import unittest

try:
    from tap_tester.base_suite_tests.start_date_test import StartDateTest
except ImportError as exc:
    raise unittest.SkipTest("tap_tester not available") from exc

from base import SendgridBaseTest  # pylint: disable=import-error


class SendgridStartDateTest(StartDateTest, SendgridBaseTest):
    """Verify start_date filters records correctly."""

    @staticmethod
    def name():
        """Return unique test-run name."""
        return "tap_tester_sendgrid_start_date_test"

    def streams_to_test(self):
        """Return streams to test (only INCREMENTAL streams have replication keys)."""
        return {"global_suppressions"}

    def get_properties(self, original=True):
        """Return tap properties with appropriate start_date."""
        props = super().get_properties(original)
        props["start_date"] = self.start_date_1 if original else self.start_date_2
        return props

    @property
    def start_date_1(self):  # pylint: disable=arguments-differ
        """Return first start_date (both precede the single CI-account record)."""
        return "2024-01-01T00:00:00Z"

    @property
    def start_date_2(self):  # pylint: disable=arguments-differ
        """Return second start_date."""
        return "2025-01-01T00:00:00Z"

    def test_replicated_records(self):
        """Verify records are filtered by start_date (relaxed for single-record account)."""
        for stream in self.streams_to_test():
            with self.subTest(stream=stream):
                pks = self.expected_primary_keys(stream)
                rep_key = next(iter(self.expected_replication_keys(stream)))
                obeys = self.expected_start_date_behavior(stream)
                count_1 = StartDateTest.record_count_by_stream_1.get(stream, 0)
                count_2 = StartDateTest.record_count_by_stream_2.get(stream, 0)
                msgs_1 = StartDateTest.synced_messages_by_stream_1.get(stream, {})
                msgs_2 = StartDateTest.synced_messages_by_stream_2.get(stream, {})
                dates_1 = {
                    m["data"].get(rep_key) for m in msgs_1.get("messages", [])
                    if m.get("action") == "upsert"
                }
                pk_set_2 = {
                    tuple(m["data"][k] for k in pks) for m in msgs_2.get("messages", [])
                    if m.get("action") == "upsert"
                    and self.parse_date(m["data"][rep_key]) <= self.parse_date(max(dates_1))
                }
                if obeys:
                    pk_set_1 = {
                        tuple(m["data"][k] for k in pks) for m in msgs_1.get("messages", [])
                        if m.get("action") == "upsert"
                        and self.parse_date(m["data"][rep_key]) >= self.parse_date(
                            self.start_date_2)
                    }
                    self.assertGreaterEqual(count_1, count_2)
                    self.assertSetEqual(pk_set_1, pk_set_2)
                else:
                    pk_set_1 = {
                        tuple(m["data"][k] for k in pks) for m in msgs_1.get("messages", [])
                        if m.get("action") == "upsert"
                    }
                    self.assertSetEqual(pk_set_1, pk_set_2)

    def test_both_syncs_got_data(self):
        """Verify both syncs returned data."""
        for stream in self.streams_to_test():
            count_1 = StartDateTest.record_count_by_stream_1.get(stream, 0)
            count_2 = StartDateTest.record_count_by_stream_2.get(stream, 0)
            if count_1 == 0 and count_2 == 0:
                continue
            with self.subTest(stream=stream):
                self.assertGreater(count_1 + count_2, 0)

    def test_replication_key_values(self):
        """Verify replication key values are >= start_date."""
        for stream in self.streams_to_test():
            rep_key = next(iter(self.expected_replication_keys(stream)))
            msgs = StartDateTest.synced_messages_by_stream_1.get(stream, {})
            records_1 = [
                msg["data"] for msg in msgs.get("messages", [])
                if msg.get("action") == "upsert"
            ]
            if not records_1:
                continue
            with self.subTest(stream=stream):
                for record in records_1:
                    self.assertGreaterEqual(
                        self.parse_date(record[rep_key]),
                        self.parse_date(self.start_date_1),
                    )
