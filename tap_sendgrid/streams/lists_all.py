"""ListsAll stream for SendGrid tap."""
from tap_sendgrid.streams.abstracts import FullTableStream


class ListsAll(FullTableStream):
    """Stream for retrieving all marketing lists."""
    tap_stream_id = "lists_all"
    key_properties = ["id"]
    replication_keys = []
    path = "marketing/lists"
    data_key = "result"
