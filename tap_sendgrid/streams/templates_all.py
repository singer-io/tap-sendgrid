"""TemplatesAll stream for SendGrid tap."""
from tap_sendgrid.streams.abstracts import FullTableStream


class TemplatesAll(FullTableStream):
    """Stream for retrieving all transactional templates.

    The templates API requires the page_size query parameter (max 200).
    We fetch both legacy and dynamic generation templates.
    """
    tap_stream_id = "templates_all"
    key_properties = ["id"]
    replication_keys = []
    path = "templates"
    data_key = "result"
    params = {"page_size": 200, "generations": "legacy,dynamic"}
