"""GroupsAll stream for SendGrid tap."""
from tap_sendgrid.streams.abstracts import FullTableStream


class GroupsAll(FullTableStream):
    """Stream for retrieving all suppression groups."""
    tap_stream_id = "groups_all"
    key_properties = ["id"]
    replication_keys = []
    path = "asm/groups"
    data_key = "result"
