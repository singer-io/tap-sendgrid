"""Bounces stream for SendGrid tap."""
from tap_sendgrid.streams.abstracts import IncrementalStream
import pendulum


class Bounces(IncrementalStream):
    """Stream for retrieving bounced email addresses."""
    tap_stream_id = "bounces"
    key_properties = ["email"]
    replication_keys = ["created"]
    path = "suppression/bounces"
    data_key = "result"

    def modify_object(self, record, parent_record=None):
        """Convert Unix timestamp to ISO string for created field."""
        if 'created' in record and isinstance(record['created'], int):
            record['created'] = pendulum.from_timestamp(record['created']).to_iso8601_string()
        return record
