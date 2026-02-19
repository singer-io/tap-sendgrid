"""SpamReports stream for SendGrid tap."""
from tap_sendgrid.streams.abstracts import IncrementalStream
import pendulum


class SpamReports(IncrementalStream):
    """Stream for retrieving spam reports."""
    tap_stream_id = "spam_reports"
    key_properties = ["email"]
    replication_keys = ["created"]
    path = "suppression/spam_reports"
    data_key = "result"

    def modify_object(self, record, parent_record=None):
        """Convert Unix timestamp to ISO string for created field."""
        if 'created' in record and isinstance(record['created'], int):
            record['created'] = pendulum.from_timestamp(record['created']).to_iso8601_string()
        return record
