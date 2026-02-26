"""Senders stream — verified sender identities from /v3/senders."""
from typing import Tuple

from tap_sendgrid.streams.abstracts import FullTableStream


class Senders(FullTableStream):
    """Full-table stream for SendGrid verified sender identities."""

    tap_stream_id = "senders"
    key_properties: Tuple[str, ...] = ("id",)
    path = "/v3/senders"
    data_key = None  # Response is a plain list
