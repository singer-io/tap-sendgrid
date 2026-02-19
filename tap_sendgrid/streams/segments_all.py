"""SegmentsAll stream for SendGrid tap."""
from tap_sendgrid.streams.abstracts import FullTableStream


class SegmentsAll(FullTableStream):
    """Stream for retrieving all marketing segments."""
    tap_stream_id = "segments_all"
    key_properties = ["id"]
    replication_keys = []
    path = "marketing/segments/2.0"
    data_key = "results"  # v2.0 endpoint uses "results" instead of "result"
