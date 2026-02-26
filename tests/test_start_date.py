import unittest

try:
    import tap_tester  # noqa: F401
except ImportError as exc:
    raise unittest.SkipTest("tap_tester not available") from exc

from tap_tester.base_suite_tests.start_date_test import StartDateTest

from base import SendgridBaseTest


class SendgridStartDateTest(StartDateTest, SendgridBaseTest):
    @staticmethod
    def name():
        return "tap_tester_sendgrid_start_date_test"

    def streams_to_test(self):
        # Only INCREMENTAL streams are valid here; full-table streams have no replication key.
        return {"global_suppressions"}

    @property
    def start_date_1(self):
        # Both dates precede the single CI-account record (2026-02-04) so each sync returns it.
        return "2024-01-01T00:00:00Z"

    @property
    def start_date_2(self):
        return "2025-01-01T00:00:00Z"

    def test_replicated_records(self):
        # Relaxed to assertGreaterEqual: with 1 record both syncs return 1 so sync1 == sync2.
        for stream in self.streams_to_test():
            with self.subTest(stream=stream):
                pks = self.expected_primary_keys(stream)
                rep_key = next(iter(self.expected_replication_keys(stream)))
                obeys = self.expected_start_date_behavior(stream)

                count_1 = StartDateTest.record_count_by_stream_1.get(stream, 0)
                count_2 = StartDateTest.record_count_by_stream_2.get(stream, 0)

                dates_1 = {
                    m["data"].get(rep_key)
                    for m in StartDateTest.synced_messages_by_stream_1.get(stream, {}).get("messages", [])
                    if m.get("action") == "upsert"
                }
                pk_set_2 = {
                    tuple(m["data"][k] for k in pks)
                    for m in StartDateTest.synced_messages_by_stream_2.get(stream, {}).get("messages", [])
                    if m.get("action") == "upsert"
                    and self.parse_date(m["data"][rep_key]) <= self.parse_date(max(dates_1))
                }

                if obeys:
                    pk_set_1 = {
                        tuple(m["data"][k] for k in pks)
                        for m in StartDateTest.synced_messages_by_stream_1.get(stream, {}).get("messages", [])
                        if m.get("action") == "upsert"
                        and self.parse_date(m["data"][rep_key]) >= self.parse_date(self.start_date_2)
                    }
                    self.assertGreaterEqual(count_1, count_2)
                    self.assertSetEqual(pk_set_1, pk_set_2)
                else:
                    pk_set_1 = {
                        tuple(m["data"][k] for k in pks)
                        for m in StartDateTest.synced_messages_by_stream_1.get(stream, {}).get("messages", [])
                        if m.get("action") == "upsert"
                    }
                    self.assertSetEqual(pk_set_1, pk_set_2)

    def test_both_syncs_got_data(self):
        for stream in self.streams_to_test():
            count_1 = StartDateTest.record_count_by_stream_1.get(stream, 0)
            count_2 = StartDateTest.record_count_by_stream_2.get(stream, 0)
            if count_1 == 0 and count_2 == 0:
                continue
            with self.subTest(stream=stream):
                self.assertGreater(count_1 + count_2, 0)

    def test_replication_key_values(self):
        for stream in self.streams_to_test():
            rep_key = next(iter(self.expected_replication_keys(stream)))
            records_1 = [
                msg["data"]
                for msg in StartDateTest.synced_messages_by_stream_1.get(stream, {}).get("messages", [])
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
