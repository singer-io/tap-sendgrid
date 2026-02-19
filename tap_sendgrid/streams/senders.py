"""Senders stream for SendGrid tap."""
from tap_sendgrid.streams.abstracts import FullTableStream


class Senders(FullTableStream):
    """Stream for retrieving sender identities."""
    tap_stream_id = "senders"
    key_properties = ["id"]
    replication_keys = []
    path = "senders"
    data_key = "result"
