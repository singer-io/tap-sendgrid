"""Campaigns stream for SendGrid tap."""
from tap_sendgrid.streams.abstracts import FullTableStream


class Campaigns(FullTableStream):
    """Stream for retrieving all campaigns (Single Sends)."""
    tap_stream_id = "campaigns"
    key_properties = ["id"]
    replication_keys = []
    path = "marketing/singlesends"
    data_key = "result"
